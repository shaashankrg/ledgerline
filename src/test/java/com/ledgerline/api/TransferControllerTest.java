package com.ledgerline.api;

import static org.assertj.core.api.Assertions.assertThat;

import java.math.BigDecimal;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.server.LocalServerPort;
import org.springframework.jdbc.core.JdbcTemplate;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.ledgerline.AbstractPostgresTest;

/**
 * Full-stack tests over a real HTTP server and a real Postgres.
 *
 * The service is not mocked: the point of these is that the wiring from header
 * and body through validation, idempotency, and the ledger write behaves as one
 * piece. Requests go over the loopback interface via the JDK HTTP client rather
 * than MockMvc, so header handling and JSON shape are exercised for real.
 */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
class TransferControllerTest extends AbstractPostgresTest {

    private static final int CONCURRENT_ITERATIONS = 20;

    @LocalServerPort
    private int port;

    @Autowired
    private JdbcTemplate jdbc;

    private static final ObjectMapper JSON = new ObjectMapper();
    private static final HttpClient CLIENT = HttpClient.newBuilder()
            .connectTimeout(Duration.ofSeconds(5))
            .build();
    private static final ExecutorService EXECUTOR = Executors.newFixedThreadPool(4);

    private long alice;
    private long bob;

    @BeforeEach
    void setUp() {
        jdbc.update("DELETE FROM ledger_entries");
        jdbc.update("DELETE FROM transactions");

        alice = accountId("Alice Checking");
        bob = accountId("Bob Checking");
    }

    @AfterAll
    static void shutdownExecutor() {
        EXECUTOR.shutdownNow();
    }

    @Test
    void successfulTransferReturns201WithLocation() throws Exception {
        HttpResponse<String> response = post(key(), body(alice, bob, "50.00", "USD"));

        assertThat(response.statusCode()).isEqualTo(201);

        JsonNode json = JSON.readTree(response.body());
        long transactionId = Long.parseLong(json.get("transactionId").asText());

        assertThat(response.headers().firstValue("Location"))
                .contains("/api/v1/transfers/" + transactionId);
        assertThat(response.headers().firstValue(TransferController.REPLAY_HEADER)).isEmpty();

        assertThat(entryCount()).isEqualTo(2);
        assertThat(balanceOf(alice)).isEqualByComparingTo(new BigDecimal("-50"));
        assertThat(balanceOf(bob)).isEqualByComparingTo(new BigDecimal("50"));
    }

    @Test
    void retryWithSameKeyAndBodyReplaysIdentically() throws Exception {
        String sharedKey = key();
        String requestBody = body(alice, bob, "50.00", "USD");

        HttpResponse<String> original = post(sharedKey, requestBody);
        HttpResponse<String> retry = post(sharedKey, requestBody);

        // Same status and same body: a client must not have to branch on retry.
        assertThat(retry.statusCode()).isEqualTo(original.statusCode()).isEqualTo(201);
        assertThat(retry.body()).isEqualTo(original.body());

        assertThat(original.headers().firstValue(TransferController.REPLAY_HEADER)).isEmpty();
        assertThat(retry.headers().firstValue(TransferController.REPLAY_HEADER)).contains("true");

        assertThat(entryCount()).isEqualTo(2);
        assertThat(transactionCount()).isEqualTo(1);
    }

    /**
     * The HTTP-level version of the Day 4 concurrency test: two identical
     * submissions released together must still produce exactly one pair, and
     * neither caller may see a 5xx.
     */
    @Test
    void concurrentDuplicatesOverHttpWriteExactlyOnePair() throws Exception {
        for (int iteration = 0; iteration < CONCURRENT_ITERATIONS; iteration++) {
            jdbc.update("DELETE FROM ledger_entries");
            jdbc.update("DELETE FROM transactions");

            String sharedKey = key();
            String requestBody = body(alice, bob, "50.00", "USD");

            CountDownLatch ready = new CountDownLatch(2);
            CountDownLatch release = new CountDownLatch(1);
            List<HttpResponse<String>> responses = new CopyOnWriteArrayList<>();
            List<Exception> failures = new CopyOnWriteArrayList<>();

            List<Callable<Void>> submissions = new ArrayList<>();
            for (int i = 0; i < 2; i++) {
                submissions.add(() -> {
                    ready.countDown();
                    release.await(5, TimeUnit.SECONDS);
                    try {
                        responses.add(post(sharedKey, requestBody));
                    } catch (Exception e) {
                        failures.add(e);
                    }
                    return null;
                });
            }

            List<Future<Void>> futures = new ArrayList<>();
            for (Callable<Void> submission : submissions) {
                futures.add(EXECUTOR.submit(submission));
            }

            assertThat(ready.await(5, TimeUnit.SECONDS)).isTrue();
            release.countDown();
            for (Future<Void> future : futures) {
                future.get(20, TimeUnit.SECONDS);
            }

            assertThat(failures).as("iteration %d had transport failures", iteration).isEmpty();
            assertThat(responses).hasSize(2);

            for (HttpResponse<String> response : responses) {
                assertThat(response.statusCode())
                        .as("iteration %d got a non-2xx: %s", iteration, response.body())
                        .isBetween(200, 299);
            }

            long replayCount = responses.stream()
                    .filter(r -> r.headers().firstValue(TransferController.REPLAY_HEADER).isPresent())
                    .count();

            assertThat(replayCount)
                    .as("iteration %d did not have exactly one replay", iteration)
                    .isEqualTo(1);
            assertThat(entryCount())
                    .as("iteration %d wrote the wrong number of entries", iteration)
                    .isEqualTo(2);
            assertThat(transactionCount()).isEqualTo(1);
        }
    }

    @Test
    void missingIdempotencyKeyHeaderIsRejected() throws Exception {
        HttpRequest request = HttpRequest.newBuilder(uri())
                .header("Content-Type", "application/json")
                .POST(HttpRequest.BodyPublishers.ofString(body(alice, bob, "50.00", "USD")))
                .build();

        HttpResponse<String> response = CLIENT.send(request, HttpResponse.BodyHandlers.ofString());

        assertThat(response.statusCode()).isEqualTo(400);
        assertThat(typeOf(response)).isEqualTo(ErrorTypes.VALIDATION_FAILED.toString());
        assertThat(entryCount()).isZero();
    }

    @Test
    void blankIdempotencyKeyHeaderIsRejected() throws Exception {
        HttpResponse<String> response = post("   ", body(alice, bob, "50.00", "USD"));

        assertThat(response.statusCode()).isEqualTo(400);
        assertThat(typeOf(response)).isEqualTo(ErrorTypes.VALIDATION_FAILED.toString());
        assertThat(entryCount()).isZero();
    }

    @Test
    void sameKeyWithDifferentAmountIsRejected() throws Exception {
        String sharedKey = key();
        post(sharedKey, body(alice, bob, "50.00", "USD"));

        HttpResponse<String> response = post(sharedKey, body(alice, bob, "75.00", "USD"));

        assertThat(response.statusCode()).isEqualTo(422);
        assertThat(typeOf(response)).isEqualTo(ErrorTypes.IDEMPOTENCY_KEY_REUSE.toString());
        assertThat(entryCount()).isEqualTo(2);
        assertThat(transactionCount()).isEqualTo(1);
    }

    @Test
    void sameAccountTransferIsRejected() throws Exception {
        HttpResponse<String> response = post(key(), body(alice, alice, "50.00", "USD"));

        assertThat(response.statusCode()).isEqualTo(400);
        assertThat(typeOf(response)).isEqualTo(ErrorTypes.SAME_ACCOUNT.toString());
        assertNothingPersisted();
    }

    @Test
    void unknownAccountIsRejected() throws Exception {
        HttpResponse<String> response = post(key(), body(999_999L, bob, "50.00", "USD"));

        assertThat(response.statusCode()).isEqualTo(422);
        assertThat(typeOf(response)).isEqualTo(ErrorTypes.ACCOUNT_NOT_FOUND.toString());
        assertNothingPersisted();
    }

    @Test
    void currencyMismatchIsRejected() throws Exception {
        HttpResponse<String> response = post(key(), body(alice, bob, "50.00", "EUR"));

        assertThat(response.statusCode()).isEqualTo(400);
        assertThat(typeOf(response)).isEqualTo(ErrorTypes.CURRENCY_MISMATCH.toString());
        assertNothingPersisted();
    }

    @Test
    void amountBeyondLedgerScaleIsRejected() throws Exception {
        HttpResponse<String> response = post(key(), body(alice, bob, "50.00001", "USD"));

        assertThat(response.statusCode()).isEqualTo(400);
        assertThat(typeOf(response)).isEqualTo(ErrorTypes.AMOUNT_SCALE.toString());
        assertNothingPersisted();
    }

    @Test
    void negativeAmountFailsBeanValidation() throws Exception {
        HttpResponse<String> response = post(key(), body(alice, bob, "-50.00", "USD"));

        assertThat(response.statusCode()).isEqualTo(400);
        assertThat(typeOf(response)).isEqualTo(ErrorTypes.VALIDATION_FAILED.toString());

        JsonNode errors = JSON.readTree(response.body()).get("errors");
        assertThat(errors.has("amount")).isTrue();
        assertNothingPersisted();
    }

    @Test
    void malformedJsonIsRejected() throws Exception {
        HttpResponse<String> response = post(key(), "{\"fromAccountId\": ");

        assertThat(response.statusCode()).isEqualTo(400);
        assertThat(typeOf(response)).isEqualTo(ErrorTypes.MALFORMED_REQUEST.toString());
        assertNothingPersisted();
    }

    /**
     * A rejected request must leave its key unclaimed, or the caller could never
     * succeed with that key after correcting the request.
     */
    @Test
    void rejectedRequestLeavesKeyReusable() throws Exception {
        String sharedKey = key();

        HttpResponse<String> rejected = post(sharedKey, body(alice, alice, "50.00", "USD"));
        assertThat(rejected.statusCode()).isEqualTo(400);
        assertNothingPersisted();

        HttpResponse<String> corrected = post(sharedKey, body(alice, bob, "50.00", "USD"));
        assertThat(corrected.statusCode()).isEqualTo(201);
        assertThat(corrected.headers().firstValue(TransferController.REPLAY_HEADER)).isEmpty();
        assertThat(entryCount()).isEqualTo(2);
    }

    /**
     * Money must survive the round trip exactly, and must not be a JSON number
     * at any point -- a JS client parsing 1234567.89 as a double is the failure
     * this shape exists to prevent.
     */
    @Test
    void amountRoundTripsExactly() throws Exception {
        String exact = "1234567.89";

        HttpResponse<String> response = post(key(), body(alice, bob, exact, "USD"));
        assertThat(response.statusCode()).isEqualTo(201);

        JsonNode json = JSON.readTree(response.body());

        // Textual, not numeric: a JSON number here would already have been
        // through a double by the time any client saw it.
        assertThat(json.get("amount").isTextual())
                .as("amount must serialize as a JSON string, body was %s", response.body())
                .isTrue();
        assertThat(json.get("transactionId").isTextual()).isTrue();

        assertThat(new BigDecimal(json.get("amount").asText())).isEqualByComparingTo(new BigDecimal(exact));
        assertThat(response.body()).doesNotContain("1.23456789E");

        long transactionId = Long.parseLong(json.get("transactionId").asText());
        BigDecimal credited = jdbc.queryForObject(
                "SELECT amount FROM ledger_entries WHERE transaction_id = ? AND amount > 0",
                BigDecimal.class, transactionId);
        assertThat(credited).isEqualByComparingTo(new BigDecimal(exact));
    }

    /**
     * An unhandled failure must not describe the internals of the service.
     *
     * The trigger is a real one rather than a stub endpoint: the transactions
     * table is dropped, so the insert fails with a Postgres error naming the
     * table and the SQL. None of that may reach the client.
     */
    @Test
    void unhandledExceptionLeaksNothing() throws Exception {
        jdbc.update("ALTER TABLE ledger_entries RENAME TO ledger_entries_hidden");
        try {
            HttpResponse<String> response = post(key(), body(alice, bob, "50.00", "USD"));

            assertThat(response.statusCode()).isEqualTo(500);
            assertThat(typeOf(response)).isEqualTo(ErrorTypes.INTERNAL_ERROR.toString());

            JsonNode json = JSON.readTree(response.body());
            assertThat(json.get("correlationId").asText()).isNotBlank();

            String body = response.body().toLowerCase();
            assertThat(body)
                    .doesNotContain("ledger_entries")
                    .doesNotContain("insert")
                    .doesNotContain("sql")
                    .doesNotContain("postgres")
                    .doesNotContain("exception")
                    .doesNotContain("org.springframework")
                    .doesNotContain("java.lang")
                    .doesNotContain("\tat ");

            // Postgres reports a missing table as: relation "x" does not exist.
            // Matched as a whole word so the correlationId property, which
            // contains "relation" as a substring, does not trip it.
            assertThat(body).doesNotContainPattern("\\brelation\\b");
        } finally {
            jdbc.update("ALTER TABLE ledger_entries_hidden RENAME TO ledger_entries");
        }
    }

    private void assertNothingPersisted() {
        assertThat(entryCount()).isZero();
        assertThat(transactionCount()).isZero();
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

    /** Amount is written as a JSON string, matching the documented contract. */
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
