package com.ledgerline.api;

import static org.assertj.core.api.Assertions.assertThat;

import java.math.BigDecimal;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.server.LocalServerPort;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.kafka.ConfluentKafkaContainer;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.ledgerline.AbstractPostgresTest;

/**
 * Full-stack tests for the read endpoints, over real HTTP and real Postgres.
 *
 * Transfers are created through the write endpoint rather than by inserting
 * rows, so what is read back is exactly what the write path produces.
 */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
class AccountReadControllerTest extends AbstractPostgresTest {

    // Intake publishes now, so these tests need a broker and the consumer even
    // though what they assert on is the read path.
    private static final String TRANSACTIONS_TOPIC = "transactions";
    private static final String DLT_TOPIC = "transactions.DLT";

    private static final ConfluentKafkaContainer KAFKA =
            new ConfluentKafkaContainer("confluentinc/cp-kafka:7.8.0");

    static {
        KAFKA.start();
        try (AdminClient admin = AdminClient.create(Map.of(
                AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA.getBootstrapServers()))) {
            admin.createTopics(List.of(
                    new NewTopic(TRANSACTIONS_TOPIC, 3, (short) 1),
                    new NewTopic(DLT_TOPIC, 1, (short) 1)))
                    .all().get(30, TimeUnit.SECONDS);
        } catch (Exception e) {
            throw new IllegalStateException("could not create topics", e);
        }
    }

    @DynamicPropertySource
    static void kafkaProperties(DynamicPropertyRegistry registry) {
        registry.add("spring.kafka.bootstrap-servers", KAFKA::getBootstrapServers);
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
    private long carol;

    @BeforeEach
    void setUp() throws Exception {
        // Entries first, then transactions: the foreign key forbids the other
        // order. Retried because the consumer runs on its own thread and may be
        // mid-write from a previous test when this fires.
        long deadline = System.currentTimeMillis() + Duration.ofSeconds(10).toMillis();
        while (true) {
            try {
                jdbc.update("DELETE FROM ledger_entries");
                jdbc.update("DELETE FROM transactions");
                break;
            } catch (DataIntegrityViolationException e) {
                if (System.currentTimeMillis() > deadline) {
                    throw e;
                }
                Thread.sleep(100);
            }
        }

        alice = accountId("Alice Checking");
        bob = accountId("Bob Checking");
        carol = accountId("Carol Checking");
    }

    @Test
    void balanceReflectsRecordedTransfers() throws Exception {
        transfer(alice, bob, "50.00");
        transfer(bob, carol, "20.00");
        transfer(carol, alice, "5.50");

        // Alice: -50 out, +5.50 in.
        JsonNode alicebalance = getJson("/api/v1/accounts/" + alice + "/balance");
        assertThat(new BigDecimal(alicebalance.get("balance").asText()))
                .isEqualByComparingTo(new BigDecimal("-44.50"));
        assertThat(alicebalance.get("entryCount").asLong()).isEqualTo(2);
        assertThat(alicebalance.get("currency").asText()).isEqualTo("USD");

        // Bob: +50 in, -20 out.
        JsonNode bobBalance = getJson("/api/v1/accounts/" + bob + "/balance");
        assertThat(new BigDecimal(bobBalance.get("balance").asText()))
                .isEqualByComparingTo(new BigDecimal("30.00"));
        assertThat(bobBalance.get("entryCount").asLong()).isEqualTo(2);
    }

    @Test
    void accountWithNoEntriesHasZeroBalance() throws Exception {
        JsonNode balance = getJson("/api/v1/accounts/" + alice + "/balance");

        assertThat(balance.get("balance").asText()).isEqualTo("0.0000");
        assertThat(balance.get("entryCount").asLong()).isZero();
    }

    @Test
    void unknownAccountBalanceIs404() throws Exception {
        HttpResponse<String> response = get("/api/v1/accounts/999999/balance");

        assertThat(response.statusCode()).isEqualTo(404);
        assertThat(JSON.readTree(response.body()).get("type").asText())
                .isEqualTo(ErrorTypes.RESOURCE_NOT_FOUND.toString());
    }

    @Test
    void balanceIsSerializedAsStringNotNumber() throws Exception {
        transfer(alice, bob, "50.00");

        HttpResponse<String> response = get("/api/v1/accounts/" + bob + "/balance");
        JsonNode balance = JSON.readTree(response.body());

        assertThat(balance.get("balance").isTextual())
                .as("balance must be a JSON string, body was %s", response.body())
                .isTrue();
        assertThat(balance.get("balance").asText()).isEqualTo("50.0000");
    }

    /** The double-entry invariant, observed through the read path. */
    @Test
    void bothSidesOfATransferSumToZero() throws Exception {
        transfer(alice, bob, "37.7500");

        BigDecimal aliceBalance = balanceOf(alice);
        BigDecimal bobBalance = balanceOf(bob);

        assertThat(aliceBalance.add(bobBalance)).isEqualByComparingTo(BigDecimal.ZERO);
        assertThat(aliceBalance).isEqualByComparingTo(new BigDecimal("-37.75"));
        assertThat(bobBalance).isEqualByComparingTo(new BigDecimal("37.75"));
    }

    @Test
    void paginationReturnsEveryEntryExactlyOnce() throws Exception {
        int transferCount = 25;
        for (int i = 0; i < transferCount; i++) {
            transfer(alice, bob, "1.00");
        }

        List<Long> seen = drainAllPages("/api/v1/accounts/" + alice + "/entries?limit=10");

        assertThat(seen).hasSize(transferCount);
        assertThat(seen).doesNotHaveDuplicates();
        assertThat(Set.copyOf(seen)).isEqualTo(Set.copyOf(entryIdsOf(alice)));
    }

    /**
     * The test that distinguishes a cursor from an offset.
     *
     * Page 1 is fetched, then new entries are inserted at the head of the
     * ordering, then page 2 is fetched with the cursor from page 1. Under
     * LIMIT/OFFSET the inserts shift every row one position later, so page 2
     * repeats rows already returned. A keyset cursor names a fixed position, so
     * the second page continues exactly where the first stopped.
     */
    @Test
    void cursorIsStableAcrossInsertsBetweenPages() throws Exception {
        for (int i = 0; i < 10; i++) {
            transfer(alice, bob, "1.00");
        }
        List<Long> originalOrder = entryIdsOf(alice);

        JsonNode firstPage = getJson("/api/v1/accounts/" + alice + "/entries?limit=5");
        List<Long> firstPageIds = idsOf(firstPage);
        String cursor = firstPage.get("nextCursor").asText();

        assertThat(firstPageIds).hasSize(5);
        assertThat(firstPageIds).isEqualTo(originalOrder.subList(0, 5));

        // Five newer entries arrive between the two page requests.
        for (int i = 0; i < 5; i++) {
            transfer(alice, carol, "2.00");
        }

        JsonNode secondPage = getJson("/api/v1/accounts/" + alice + "/entries?limit=5&before=" + cursor);
        List<Long> secondPageIds = idsOf(secondPage);

        List<Long> repeated = secondPageIds.stream().filter(firstPageIds::contains).toList();
        assertThat(secondPageIds)
                .as("page 2 repeated %d row(s) already returned on page 1: %s (page1=%s, page2=%s)",
                        repeated.size(), repeated, firstPageIds, secondPageIds)
                .doesNotContainAnyElementsOf(firstPageIds);

        // Continues exactly where page 1 stopped, despite the newer inserts.
        assertThat(secondPageIds)
                .as("page 2 skipped or reordered rows")
                .isEqualTo(originalOrder.subList(5, 10));
    }

    @Test
    void limitAboveMaximumIsClamped() throws Exception {
        for (int i = 0; i < 3; i++) {
            transfer(alice, bob, "1.00");
        }

        HttpResponse<String> response = get(
                "/api/v1/accounts/" + alice + "/entries?limit=" + (AccountReadController.MAX_LIMIT + 500));

        // Clamped, not rejected.
        assertThat(response.statusCode()).isEqualTo(200);
        assertThat(idsOf(JSON.readTree(response.body()))).hasSize(3);
    }

    @Test
    void malformedCursorIsRejected() throws Exception {
        HttpResponse<String> response = get("/api/v1/accounts/" + alice + "/entries?before=not-a-cursor");

        assertThat(response.statusCode()).isEqualTo(400);
        assertThat(JSON.readTree(response.body()).get("type").asText())
                .isEqualTo(ErrorTypes.MALFORMED_CURSOR.toString());
    }

    @Test
    void entriesCarryCounterpartyAndSignedAmount() throws Exception {
        transfer(alice, bob, "50.00");

        JsonNode page = getJson("/api/v1/accounts/" + alice + "/entries");
        JsonNode entry = page.get("items").get(0);

        assertThat(entry.get("amount").isTextual()).isTrue();
        // Alice is the debit side.
        assertThat(new BigDecimal(entry.get("amount").asText()))
                .isEqualByComparingTo(new BigDecimal("-50"));
        assertThat(entry.get("counterpartyAccountId").asLong()).isEqualTo(bob);
        assertThat(entry.get("currency").asText()).isEqualTo("USD");
        assertThat(entry.get("createdAt").asText()).isNotBlank();
    }

    @Test
    void lastPageHasNullCursor() throws Exception {
        transfer(alice, bob, "1.00");

        JsonNode page = getJson("/api/v1/accounts/" + alice + "/entries?limit=10");

        assertThat(page.get("items")).hasSize(1);
        assertThat(page.get("nextCursor").isNull()).isTrue();
    }

    /** Walks every page, collecting entry ids in order. */
    private List<Long> drainAllPages(String firstPageUrl) throws Exception {
        List<Long> collected = new ArrayList<>();
        Set<String> cursorsSeen = new LinkedHashSet<>();

        String url = firstPageUrl;
        while (url != null) {
            JsonNode page = getJson(url);
            collected.addAll(idsOf(page));

            JsonNode next = page.get("nextCursor");
            if (next == null || next.isNull()) {
                break;
            }

            String cursor = next.asText();
            // Guards against a cursor that never advances looping forever.
            assertThat(cursorsSeen.add(cursor)).as("cursor %s repeated", cursor).isTrue();
            url = firstPageUrl + "&before=" + cursor;
        }
        return collected;
    }

    private List<Long> idsOf(JsonNode page) {
        List<Long> ids = new ArrayList<>();
        page.get("items").forEach(item -> ids.add(Long.parseLong(item.get("entryId").asText())));
        return ids;
    }

    /** Entry ids for an account in the same order the endpoint returns them. */
    private List<Long> entryIdsOf(long accountId) {
        return jdbc.queryForList(
                "SELECT id FROM ledger_entries WHERE account_id = ? ORDER BY created_at DESC, id DESC",
                Long.class, accountId);
    }

    private BigDecimal balanceOf(long accountId) throws Exception {
        return new BigDecimal(getJson("/api/v1/accounts/" + accountId + "/balance").get("balance").asText());
    }

    /**
     * Submits a transfer and waits for the consumer to write it.
     *
     * Intake is asynchronous now: the endpoint answers 202 once the message is
     * on the topic, and the entries appear only after the consumer processes
     * it. These are read-path tests, so they need the write to have landed
     * before they assert on it -- hence the wait, which is not part of what is
     * under test here.
     */
    private void transfer(long from, long to, String amount) throws Exception {
        String body = """
                {"fromAccountId":%d,"toAccountId":%d,"amount":"%s","currency":"USD"}"""
                .formatted(from, to, amount);

        long entriesBefore = entryCount();

        HttpRequest request = HttpRequest.newBuilder(URI.create("http://localhost:" + port + "/api/v1/transfers"))
                .header("Content-Type", "application/json")
                .header("Idempotency-Key", UUID.randomUUID().toString())
                .POST(HttpRequest.BodyPublishers.ofString(body))
                .build();

        HttpResponse<String> response = CLIENT.send(request, HttpResponse.BodyHandlers.ofString());
        assertThat(response.statusCode()).as("transfer failed: %s", response.body()).isEqualTo(202);

        long deadline = System.currentTimeMillis() + Duration.ofSeconds(30).toMillis();
        while (System.currentTimeMillis() < deadline && entryCount() < entriesBefore + 2) {
            Thread.sleep(100);
        }
        assertThat(entryCount())
                .as("the consumer did not write the transfer in time")
                .isEqualTo(entriesBefore + 2);
    }

    private long entryCount() {
        return jdbc.queryForObject("SELECT count(*) FROM ledger_entries", Long.class);
    }

    private JsonNode getJson(String path) throws Exception {
        HttpResponse<String> response = get(path);
        assertThat(response.statusCode()).as("GET %s failed: %s", path, response.body()).isEqualTo(200);
        return JSON.readTree(response.body());
    }

    private HttpResponse<String> get(String path) throws Exception {
        HttpRequest request = HttpRequest.newBuilder(URI.create("http://localhost:" + port + path))
                .GET()
                .build();
        return CLIENT.send(request, HttpResponse.BodyHandlers.ofString());
    }

    private long accountId(String name) {
        return jdbc.queryForObject("SELECT id FROM accounts WHERE name = ?", Long.class, name);
    }
}
