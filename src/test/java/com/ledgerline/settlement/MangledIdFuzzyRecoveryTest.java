package com.ledgerline.settlement;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import java.io.ByteArrayInputStream;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.EnumMap;
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
import com.ledgerline.reconciliation.ReconciliationService;

/**
 * End to end, on a generated batch rather than hand-built fixtures: the
 * NETWORK_MANGLED_TXN_ID fault is genuinely recoverable by pass 2, and
 * measurably shrinks the unmatched set on real generated data.
 *
 * Every other Day 3 fuzzy-matching test constructs its own settlement rows
 * directly. That was necessary to prove the matcher's decision logic in
 * isolation, but it left an open question the fix-up spec calls out: does
 * fuzzy matching recover anything on data the simulator actually produces?
 * Before this fault existed, the answer was no -- NETWORK_UNKNOWN_TXN
 * fabricates a well-formed id for a payment that doesn't exist, which fails
 * on amount and merchant too and was never recoverable by design. This class
 * is the proof that the gap is closed: real consumer, real ledger, real
 * settlement file, real fault.
 */
@SpringBootTest
class MangledIdFuzzyRecoveryTest {

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
    private ReconciliationService reconciliationService;

    @Autowired
    private JdbcTemplate jdbc;

    private List<Long> accountIds;

    @BeforeEach
    void setUp() {
        jdbc.update("DELETE FROM recon_line_outcomes");
        jdbc.update("DELETE FROM recon_exceptions");
        jdbc.update("DELETE FROM recon_runs");
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

    private static final int TRANSACTION_COUNT = 40;

    @Test
    void mangledIdRowIsRecoveredByFuzzyMatching() throws Exception {
        String runId = "run-" + UUID.randomUUID();
        GeneratorResult published = generator.generate(
                GeneratorConfig.clean(runId, 7001L, TRANSACTION_COUNT, accountIds));

        await().atMost(Duration.ofSeconds(30)).until(() ->
                countCaptureTransactions(runId) >= TRANSACTION_COUNT);

        String batchId = "batch-" + UUID.randomUUID();
        Map<NetworkFaultType, Double> onlyMangled = new EnumMap<>(NetworkFaultType.class);
        onlyMangled.put(NetworkFaultType.NETWORK_MANGLED_TXN_ID, 1.0);

        // baseInstant must sit near the real capture time, not an arbitrary
        // fixed date: the consumer writes transactions.created_at as the
        // actual wall-clock instant it processes each event
        // (TransactionEventService, off limits, writes no other value), so
        // settled_at = baseInstant + 18h has to be close to *now* for any
        // candidate to fall inside a realistic window. A fixed historical
        // baseInstant -- fine for the byte-identical-file tests elsewhere,
        // which never touch the ledger -- silently produces zero candidates
        // here, which is a fixture bug, not a matcher one.
        SettlementConfig config = new SettlementConfig(
                runId, batchId, 7001L, Instant.now(),
                onlyMangled, Clock.systemUTC());
        SettlementSimulator.SettlementResult result = simulator.generate(config, published);
        loader.load(batchId, new ByteArrayInputStream(result.csvBytes()));

        // Ground truth: every row this run actually mangled, and the real id
        // behind each one.
        List<Map<String, Object>> groundTruth = jdbc.queryForList(
                "SELECT external_txn_id, detail FROM faultlab.injected_faults "
                        + "WHERE run_id = ? AND fault_type = 'NETWORK_MANGLED_TXN_ID'",
                batchId);
        assertThat(groundTruth)
                .as("the simulator must have injected at least one mangled-id fault "
                        + "at rate 1.0 over %d transactions", TRANSACTION_COUNT)
                .isNotEmpty();

        reconciliationService.run(batchId);

        long reconRunId = jdbc.queryForObject(
                "SELECT recon_run_id FROM recon_runs WHERE batch_id = ?", Long.class, batchId);

        // Every mangled payment must appear as exactly one FUZZY_MATCHED
        // claim in this run -- not merely that some row somewhere matched,
        // but that the specific set of payments the simulator actually
        // corrupted is the specific set pass 2 recovered.
        List<String> matchedTxnIds = jdbc.queryForList(
                "SELECT matched_txn_id FROM recon_line_outcomes "
                        + "WHERE recon_run_id = ? AND outcome = 'FUZZY_MATCHED'",
                String.class, reconRunId);
        List<String> originalIds = groundTruth.stream()
                .map(f -> (String) f.get("external_txn_id"))
                .toList();

        assertThat(matchedTxnIds)
                .as("every mangled payment must be recovered by fuzzy matching")
                .containsExactlyInAnyOrderElementsOf(originalIds);

        System.out.println("[mangledIdRowIsRecoveredByFuzzyMatching] injected="
                + groundTruth.size() + " recovered=" + matchedTxnIds.size());
    }

    @Test
    void unmatchedSetShrinksVersusPassOneOnAGeneratedBatch() throws Exception {
        String runId = "run-" + UUID.randomUUID();
        GeneratorResult published = generator.generate(
                GeneratorConfig.clean(runId, 7002L, TRANSACTION_COUNT, accountIds));

        await().atMost(Duration.ofSeconds(30)).until(() ->
                countCaptureTransactions(runId) >= TRANSACTION_COUNT);

        String batchId = "batch-" + UUID.randomUUID();
        Map<NetworkFaultType, Double> onlyMangled = new EnumMap<>(NetworkFaultType.class);
        onlyMangled.put(NetworkFaultType.NETWORK_MANGLED_TXN_ID, 0.5);

        // See the comment in mangledIdRowIsRecoveredByFuzzyMatching: baseInstant
        // has to track real time, since capture time is the consumer's actual
        // wall-clock write.
        SettlementConfig config = new SettlementConfig(
                runId, batchId, 7002L, Instant.now(),
                onlyMangled, Clock.systemUTC());
        SettlementSimulator.SettlementResult result = simulator.generate(config, published);
        loader.load(batchId, new ByteArrayInputStream(result.csvBytes()));

        // Pass 1 alone: a zero-width window admits no fuzzy candidate.
        reconciliationService.run(batchId, 0);
        long passOneRunId = jdbc.queryForObject(
                "SELECT recon_run_id FROM recon_runs WHERE batch_id = ? AND window_seconds = 0",
                Long.class, batchId);
        int unmatchedAfterPassOne = jdbc.queryForObject(
                "SELECT count(*) FROM recon_line_outcomes "
                        + "WHERE recon_run_id = ? AND outcome = 'MISSING_IN_LEDGER'",
                Integer.class, passOneRunId);

        reconciliationService.run(batchId, ReconciliationService.DEFAULT_WINDOW_SECONDS);
        long passTwoRunId = jdbc.queryForObject(
                "SELECT recon_run_id FROM recon_runs WHERE batch_id = ? AND window_seconds = ?",
                Long.class, batchId, ReconciliationService.DEFAULT_WINDOW_SECONDS);
        int unmatchedAfterPassTwo = jdbc.queryForObject(
                "SELECT count(*) FROM recon_line_outcomes "
                        + "WHERE recon_run_id = ? AND outcome = 'MISSING_IN_LEDGER'",
                Integer.class, passTwoRunId);
        int recovered = jdbc.queryForObject(
                "SELECT count(*) FROM recon_line_outcomes "
                        + "WHERE recon_run_id = ? AND outcome = 'FUZZY_MATCHED'",
                Integer.class, passTwoRunId);

        System.out.println("[unmatchedSetShrinksVersusPassOneOnAGeneratedBatch] "
                + "MISSING_IN_LEDGER after pass 1 = " + unmatchedAfterPassOne
                + ", after pass 2 = " + unmatchedAfterPassTwo
                + ", FUZZY_MATCHED = " + recovered);

        assertThat(unmatchedAfterPassOne).isGreaterThan(0);
        assertThat(unmatchedAfterPassTwo).isLessThan(unmatchedAfterPassOne);
        assertThat(recovered).isGreaterThan(0);
    }

    private long countCaptureTransactions(String runId) {
        Long count = jdbc.queryForObject(
                "SELECT count(*) FROM transactions WHERE idempotency_key LIKE ?",
                Long.class, runId + "-txn-%:CAPTURE");
        return count == null ? 0 : count;
    }
}
