package com.ledgerline.settlement;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import java.io.ByteArrayInputStream;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.kafka.ConfluentKafkaContainer;

import com.ledgerline.generator.GeneratorConfig;
import com.ledgerline.generator.TransactionGenerator;
import com.ledgerline.generator.TransactionGenerator.GeneratorResult;

/**
 * Proves the settlement file's merchant field is a genuine join key against
 * the ledger, not a value that only ever agrees with itself.
 *
 * The consumer runs here -- deliberately unlike {@link SettlementSimulatorTest},
 * which disables it because several of its tests need the ledger empty or
 * irrelevant. This class needs the opposite: a real ledger, actually written
 * by the real consumer, so the join in {@link #fuzzyMatchFieldsExistOnBothSides}
 * exercises the same access a reconciliation engine will actually have --
 * database only, no knowledge of the generator's seed or of
 * {@code SyntheticMerchants}.
 */
@SpringBootTest
class FuzzyMatchFieldsTest {

    private static final String TRANSACTIONS_TOPIC = "transactions";
    private static final String DLT_TOPIC = "transactions.DLT";

    private static final ConfluentKafkaContainer KAFKA =
            new ConfluentKafkaContainer("confluentinc/cp-kafka:7.8.0");
    private static final PostgreSQLContainer<?> POSTGRES =
            new PostgreSQLContainer<>("postgres:16");

    static {
        KAFKA.start();
        POSTGRES.start();
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
    static void containerProperties(DynamicPropertyRegistry registry) {
        registry.add("spring.kafka.bootstrap-servers", KAFKA::getBootstrapServers);
        registry.add("spring.datasource.url", POSTGRES::getJdbcUrl);
        registry.add("spring.datasource.username", POSTGRES::getUsername);
        registry.add("spring.datasource.password", POSTGRES::getPassword);
    }

    @Autowired
    private TransactionGenerator generator;

    @Autowired
    private SettlementSimulator simulator;

    @Autowired
    private SettlementLoader loader;

    @Autowired
    private JdbcTemplate jdbc;

    private List<Long> accountIds;

    @BeforeEach
    void setUp() {
        jdbc.update("DELETE FROM settlement_records");
        jdbc.update("DELETE FROM recon_batches");
        jdbc.update("DELETE FROM faultlab.injected_faults");
        jdbc.update("DELETE FROM faultlab.generator_runs");
        jdbc.update("DELETE FROM ledger_entries");
        jdbc.update("DELETE FROM transactions");
        jdbc.update("DELETE FROM transaction_states");
        jdbc.update("DELETE FROM parked_events");

        accountIds = jdbc.queryForList("SELECT id FROM accounts ORDER BY id", Long.class);
    }

    private static final int TRANSACTION_COUNT = 15;

    /**
     * A settlement row's merchant_id must match the merchant_id the real
     * consumer wrote to transactions for the same payment -- joined through
     * the database only, exactly the access a reconciliation engine has.
     *
     * No reference to the generator's seed or to SyntheticMerchants appears
     * below the point where the batch is generated: the assertion reads both
     * sides purely from what's stored, which is the whole point. If this
     * test read the seed to predict the merchant, it would prove nothing --
     * the reconciliation engine it's modeling can never do that.
     *
     * Two things this test must not let slip past silently, because
     * merchantId is nullable everywhere it travels (TransactionMessage,
     * TransactionEvent, transactions.merchant_id): if assignment in
     * planTransaction ever stopped firing -- a skipped config path, a
     * refactor that reaches for the old 7-arg TransactionMessage
     * constructor -- both sides would carry NULL, and NULL = NULL is not
     * true in SQL, but an equality assertion over Java's boxed nulls would
     * still read as "equal" and pass vacuously. Guarded against here by:
     * (1) asserting both sides are non-null before comparing them, not just
     * the ledger side, and (2) asserting the joined row count equals the
     * known transaction count, so a join that silently dropped rows -- which
     * a mismatched ON condition or a dropped merchant would produce just as
     * easily as an outright bug -- can't pass by shrinking down to an
     * "everything remaining agrees" result.
     */
    @Test
    void fuzzyMatchFieldsExistOnBothSides() throws Exception {
        String runId = "run-" + UUID.randomUUID();
        GeneratorResult published = generator.generate(
                GeneratorConfig.clean(runId, 3011L, TRANSACTION_COUNT, accountIds));

        // Wait for the real consumer to write every captured payment's
        // transaction row -- one CAPTURE per clean-run transaction.
        await().atMost(Duration.ofSeconds(30)).until(() ->
                countCaptureTransactions(runId) >= TRANSACTION_COUNT);

        String batchId = "batch-" + UUID.randomUUID();
        var result = simulator.generate(
                SettlementConfig.clean(runId, batchId, 3011L, Instant.parse("2026-01-01T00:00:00Z"),
                        java.time.Clock.systemUTC()),
                published);
        loader.load(batchId, new ByteArrayInputStream(result.csvBytes()));

        List<Map<String, Object>> joined = jdbc.queryForList(
                "SELECT sr.external_txn_id, sr.merchant_id AS settlement_merchant, "
                        + "       t.merchant_id AS ledger_merchant "
                        + "FROM settlement_records sr "
                        + "JOIN transactions t ON t.idempotency_key = sr.external_txn_id || ':CAPTURE' "
                        + "WHERE sr.batch_id = ?",
                batchId);

        // A join that silently dropped rows -- wrong ON condition, a
        // mismatched id format -- would still pass an isNotEmpty check.
        // Only a known expected count catches that.
        assertThat(joined)
                .as("every one of the %d captured payments must join to a settlement row", TRANSACTION_COUNT)
                .hasSize(TRANSACTION_COUNT);
        assertThat(joined).allSatisfy(row -> {
            assertThat(row.get("ledger_merchant"))
                    .as("the ledger side must actually carry a non-null merchant_id, row %s", row)
                    .isNotNull();
            assertThat(row.get("settlement_merchant"))
                    .as("the settlement side must actually carry a non-null merchant_id, row %s", row)
                    .isNotNull();
            assertThat(row.get("settlement_merchant"))
                    .as("settlement and ledger merchant_id must match for %s", row)
                    .isEqualTo(row.get("ledger_merchant"));
        });
    }

    private long countCaptureTransactions(String runId) {
        Long count = jdbc.queryForObject(
                "SELECT count(*) FROM transactions WHERE idempotency_key LIKE ?",
                Long.class, runId + "-txn-%:CAPTURE");
        return count == null ? 0 : count;
    }
}
