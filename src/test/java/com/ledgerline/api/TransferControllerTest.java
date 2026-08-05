package com.ledgerline.api;

import static org.assertj.core.api.Assertions.assertThat;

import java.math.BigDecimal;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.admin.OffsetSpec;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.server.LocalServerPort;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.kafka.ConfluentKafkaContainer;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * Intake tests for the producer-only endpoint.
 *
 * The endpoint publishes and returns 202, so almost nothing here can assert on
 * the ledger immediately -- the entries appear only once the consumer has
 * processed the message. Tests that care about the ledger wait for it.
 */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
class TransferControllerTest {

    // Named here rather than imported: the messaging classes are package-private
    // by design, and widening them so a test in another package can read a
    // constant would be the wrong trade. These names are a fixed contract.
    private static final String TRANSACTIONS_TOPIC = "transactions";
    private static final String DLT_TOPIC = "transactions.DLT";

    private static final ConfluentKafkaContainer KAFKA =
            new ConfluentKafkaContainer("confluentinc/cp-kafka:7.8.0");
    private static final PostgreSQLContainer<?> POSTGRES =
            new PostgreSQLContainer<>("postgres:16");

    static {
        KAFKA.start();
        POSTGRES.start();
        createTopics();
    }

    private static void createTopics() {
        try (AdminClient admin = admin()) {
            admin.createTopics(List.of(
                    new NewTopic(TRANSACTIONS_TOPIC, 3, (short) 1),
                    new NewTopic(DLT_TOPIC, 1, (short) 1)))
                    .all().get(30, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException(e);
        } catch (Exception e) {
            throw new IllegalStateException("could not create topics", e);
        }
    }

    @DynamicPropertySource
    static void containerProperties(DynamicPropertyRegistry registry) {
        registry.add("spring.kafka.bootstrap-servers", KAFKA::getBootstrapServers);
        registry.add("spring.datasource.url", POSTGRES::getJdbcUrl);
        registry.add("spring.datasource.username", POSTGRES::getUsername);
        registry.add("spring.datasource.password", POSTGRES::getPassword);
    }

    @LocalServerPort
    private int port;

    @Autowired
    private JdbcTemplate jdbc;

    private static final ObjectMapper JSON = new ObjectMapper();
    private static final HttpClient CLIENT = HttpClient.newBuilder()
            .connectTimeout(Duration.ofSeconds(5))
            .build();

    private long alice;
    private long bob;

    @BeforeEach
    void setUp() {
        jdbc.update("DELETE FROM ledger_entries");
        jdbc.update("DELETE FROM transactions");

        alice = accountId("Alice Checking");
        bob = accountId("Bob Checking");
    }

    @Test
    void acceptedTransferIsEventuallyWrittenToTheLedger() throws Exception {
        String key = key();
        HttpResponse<String> response = post(key, body(alice, bob, "50.00", "USD"));

        assertThat(response.statusCode()).isEqualTo(202);

        JsonNode json = JSON.readTree(response.body());
        assertThat(json.get("transactionId").asText()).isEqualTo(key);
        assertThat(response.headers().firstValue("Location"))
                .contains("/api/v1/transfers/" + key);

        awaitEntryCount(2);
        assertThat(balanceOf(alice)).isEqualByComparingTo(new BigDecimal("-50"));
        assertThat(balanceOf(bob)).isEqualByComparingTo(new BigDecimal("50"));
    }

    /**
     * Both submissions are accepted identically.
     *
     * The producer has no state and cannot tell a retry from a first attempt, so
     * it publishes both. Deduplication happens downstream, which is why the
     * ledger still ends up with exactly one pair.
     */
    @Test
    void duplicateSubmissionIsAcceptedTwiceAndWrittenOnce() throws Exception {
        String key = key();
        String payload = body(alice, bob, "50.00", "USD");

        HttpResponse<String> first = post(key, payload);
        HttpResponse<String> second = post(key, payload);

        assertThat(first.statusCode()).isEqualTo(202);
        assertThat(second.statusCode()).isEqualTo(202);
        // Identical responses: the client never has to branch on which won.
        assertThat(second.body()).isEqualTo(first.body());

        awaitEntryCount(2);
        // Give the second message time to be consumed and recognized as a replay.
        Thread.sleep(3_000);
        assertThat(entryCount()).isEqualTo(2);
        assertThat(transactionCount()).isEqualTo(1);
    }

    @Test
    void negativeAmountIsRejectedAndNothingIsPublished() throws Exception {
        long backlogBefore = topicEndOffsets();

        HttpResponse<String> response = post(key(), body(alice, bob, "-50.00", "USD"));

        assertThat(response.statusCode()).isEqualTo(400);
        assertThat(typeOf(response)).isEqualTo(ErrorTypes.VALIDATION_FAILED.toString());
        assertThat(topicEndOffsets()).isEqualTo(backlogBefore);
    }

    @Test
    void amountBeyondLedgerScaleIsRejectedAndNothingIsPublished() throws Exception {
        long backlogBefore = topicEndOffsets();

        HttpResponse<String> response = post(key(), body(alice, bob, "50.00001", "USD"));

        assertThat(response.statusCode()).isEqualTo(400);
        assertThat(typeOf(response)).isEqualTo(ErrorTypes.VALIDATION_FAILED.toString());
        assertThat(topicEndOffsets()).isEqualTo(backlogBefore);
    }

    @Test
    void missingFieldIsRejectedAndNothingIsPublished() throws Exception {
        long backlogBefore = topicEndOffsets();

        HttpResponse<String> response = post(key(),
                "{\"fromAccountId\":%d,\"amount\":\"50.00\",\"currency\":\"USD\"}".formatted(alice));

        assertThat(response.statusCode()).isEqualTo(400);
        assertThat(topicEndOffsets()).isEqualTo(backlogBefore);
    }

    @Test
    void malformedJsonIsRejectedAndNothingIsPublished() throws Exception {
        long backlogBefore = topicEndOffsets();

        HttpResponse<String> response = post(key(), "{\"fromAccountId\": ");

        assertThat(response.statusCode()).isEqualTo(400);
        assertThat(typeOf(response)).isEqualTo(ErrorTypes.MALFORMED_REQUEST.toString());
        assertThat(topicEndOffsets()).isEqualTo(backlogBefore);
    }

    @Test
    void missingIdempotencyKeyHeaderIsRejected() throws Exception {
        long backlogBefore = topicEndOffsets();

        HttpRequest request = HttpRequest.newBuilder(uri())
                .header("Content-Type", "application/json")
                .POST(HttpRequest.BodyPublishers.ofString(body(alice, bob, "50.00", "USD")))
                .build();
        HttpResponse<String> response = CLIENT.send(request, HttpResponse.BodyHandlers.ofString());

        assertThat(response.statusCode()).isEqualTo(400);
        assertThat(topicEndOffsets()).isEqualTo(backlogBefore);
    }

    @Test
    void blankIdempotencyKeyHeaderIsRejected() throws Exception {
        HttpResponse<String> response = post("   ", body(alice, bob, "50.00", "USD"));

        assertThat(response.statusCode()).isEqualTo(400);
        assertThat(typeOf(response)).isEqualTo(ErrorTypes.VALIDATION_FAILED.toString());
    }

    /**
     * The deliberate consequence of moving semantic validation downstream.
     *
     * Intake cannot tell that two account ids are the same account without
     * reading state, so it accepts the request. The failure surfaces later, in
     * the consumer, as a dead letter. Asserted explicitly because it is a real
     * behavioural change from the synchronous contract, not an oversight.
     */
    @Test
    void sameAccountTransferIsAcceptedThenDeadLettered() throws Exception {
        long deadLettersBefore = deadLetterCount();

        HttpResponse<String> response = post(key(), body(alice, alice, "50.00", "USD"));

        assertThat(response.statusCode()).isEqualTo(202);

        awaitDeadLetterCount(deadLettersBefore + 1);
        assertThat(entryCount()).isZero();
    }

    @Test
    void unknownAccountIsAcceptedThenDeadLettered() throws Exception {
        long deadLettersBefore = deadLetterCount();

        HttpResponse<String> response = post(key(), body(999_999L, bob, "50.00", "USD"));

        assertThat(response.statusCode()).isEqualTo(202);

        awaitDeadLetterCount(deadLettersBefore + 1);
        assertThat(entryCount()).isZero();
    }

    @Test
    void currencyMismatchIsAcceptedThenDeadLettered() throws Exception {
        long deadLettersBefore = deadLetterCount();

        HttpResponse<String> response = post(key(), body(alice, bob, "50.00", "EUR"));

        assertThat(response.statusCode()).isEqualTo(202);

        awaitDeadLetterCount(deadLettersBefore + 1);
        assertThat(entryCount()).isZero();
    }

    @Test
    void amountRoundTripsExactlyAsAString() throws Exception {
        String exact = "1234567.89";
        String key = key();

        HttpResponse<String> response = post(key, body(alice, bob, exact, "USD"));
        assertThat(response.statusCode()).isEqualTo(202);

        JsonNode json = JSON.readTree(response.body());
        assertThat(json.get("amount").isTextual())
                .as("amount must be a JSON string, body was %s", response.body())
                .isTrue();
        assertThat(new BigDecimal(json.get("amount").asText())).isEqualByComparingTo(new BigDecimal(exact));

        awaitEntryCount(2);
        BigDecimal credited = jdbc.queryForObject(
                "SELECT amount FROM ledger_entries WHERE amount > 0", BigDecimal.class);
        assertThat(credited).isEqualByComparingTo(new BigDecimal(exact));
    }

    private void awaitEntryCount(long expected) throws Exception {
        long deadline = System.currentTimeMillis() + Duration.ofSeconds(30).toMillis();
        while (System.currentTimeMillis() < deadline && entryCount() != expected) {
            Thread.sleep(200);
        }
        assertThat(entryCount()).isEqualTo(expected);
    }

    private void awaitDeadLetterCount(long expected) throws Exception {
        long deadline = System.currentTimeMillis() + Duration.ofSeconds(30).toMillis();
        while (System.currentTimeMillis() < deadline && deadLetterCount() < expected) {
            Thread.sleep(200);
        }
        assertThat(deadLetterCount()).isGreaterThanOrEqualTo(expected);
    }

    /** Total records across every partition of the transactions topic. */
    private long topicEndOffsets() {
        return endOffsetsOf(TRANSACTIONS_TOPIC, 3);
    }

    private long deadLetterCount() {
        return endOffsetsOf(DLT_TOPIC, 1);
    }

    private long endOffsetsOf(String topic, int partitions) {
        try (AdminClient admin = admin()) {
            Map<TopicPartition, OffsetSpec> request = new java.util.HashMap<>();
            for (int partition = 0; partition < partitions; partition++) {
                request.put(new TopicPartition(topic, partition), OffsetSpec.latest());
            }
            return admin.listOffsets(request).all().get(30, TimeUnit.SECONDS)
                    .values().stream()
                    .mapToLong(info -> info.offset())
                    .sum();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException(e);
        } catch (Exception e) {
            throw new IllegalStateException("could not read end offsets", e);
        }
    }

    private static AdminClient admin() {
        return AdminClient.create(Map.of(
                AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA.getBootstrapServers()));
    }

    private HttpResponse<String> post(String idempotencyKey, String body) throws Exception {
        HttpRequest request = HttpRequest.newBuilder(uri())
                .header("Content-Type", "application/json")
                .header("Idempotency-Key", idempotencyKey)
                .POST(HttpRequest.BodyPublishers.ofString(body))
                .build();
        return CLIENT.send(request, HttpResponse.BodyHandlers.ofString());
    }

    private URI uri() {
        return URI.create("http://localhost:" + port + "/api/v1/transfers");
    }

    private String body(long from, long to, String amount, String currency) {
        return """
                {"fromAccountId":%d,"toAccountId":%d,"amount":"%s","currency":"%s"}"""
                .formatted(from, to, amount, currency);
    }

    private String typeOf(HttpResponse<String> response) throws Exception {
        return Optional.ofNullable(JSON.readTree(response.body()).get("type"))
                .map(JsonNode::asText)
                .orElse(null);
    }

    private long accountId(String name) {
        return jdbc.queryForObject("SELECT id FROM accounts WHERE name = ?", Long.class, name);
    }

    private long entryCount() {
        return jdbc.queryForObject("SELECT count(*) FROM ledger_entries", Long.class);
    }

    private long transactionCount() {
        return jdbc.queryForObject("SELECT count(*) FROM transactions", Long.class);
    }

    private BigDecimal balanceOf(long accountId) {
        return jdbc.queryForObject(
                "SELECT COALESCE(SUM(amount), 0) FROM ledger_entries WHERE account_id = ?",
                BigDecimal.class, accountId);
    }

    private static String key() {
        return UUID.randomUUID().toString();
    }
}
