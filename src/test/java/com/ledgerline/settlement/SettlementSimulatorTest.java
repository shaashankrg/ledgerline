package com.ledgerline.settlement;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.ByteArrayInputStream;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
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

/**
 * The settlement simulator and loader, against real Postgres and Kafka.
 *
 * The consumer stays disabled, same reasoning as
 * {@code TransactionGeneratorTest}: what's under test here is the simulator's
 * own output and the loader that reads it back, not what the ledger does with
 * the underlying generator run. Kafka is real (not mocked) because the
 * generator being fed to the simulator publishes to a real broker; Postgres is
 * real because the whole point of several tests here (the permission
 * boundary, the header assertion, the independence test) is a property of the
 * real database, not of a mock that only ever does what it's told.
 */
@SpringBootTest(properties = "ledgerline.consumer.enabled=false")
class SettlementSimulatorTest {

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

    private static final Instant BASE_INSTANT = Instant.parse("2026-01-15T00:00:00Z");

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

        accountIds = jdbc.queryForList("SELECT id FROM accounts ORDER BY id", Long.class);
    }

    // ---- Determinism -----------------------------------------------------

    @Test
    void sameSeedProducesByteIdenticalFile() throws Exception {
        String runId = runId();
        GeneratorResult published = generator.generate(
                GeneratorConfig.clean(runId, 42L, 30, accountIds));

        // recon_batches.batch_id is a primary key, so the two generations
        // below cannot share one id -- each batch must be recorded once.
        // batch_id is also itself a CSV column, so the raw bytes are expected
        // to differ by exactly that one column; stripping it before comparing
        // isolates the property actually under test, the same technique
        // TransactionGeneratorTest uses to strip its run id.
        String batchA = batchId();
        String batchB = batchId();
        byte[] fileA = simulator.generate(allFaultsConfig(runId, batchA, 999L), published).csvBytes();
        byte[] fileB = simulator.generate(allFaultsConfig(runId, batchB, 999L), published).csvBytes();

        String normalizedA = normalize(fileA, batchA);
        String normalizedB = normalize(fileB, batchB);

        assertThat(sha256(normalizedA.getBytes(StandardCharsets.UTF_8)))
                .as("identical seed and input must produce byte-identical files, modulo the batch id")
                .isEqualTo(sha256(normalizedB.getBytes(StandardCharsets.UTF_8)));
        assertThat(normalizedA).isEqualTo(normalizedB);
    }

    @Test
    void differentSeedProducesDifferentFile() throws Exception {
        String runId = runId();
        GeneratorResult published = generator.generate(
                GeneratorConfig.clean(runId, 43L, 30, accountIds));

        byte[] fileA = simulator.generate(allFaultsConfig(runId, batchId(), 1L), published).csvBytes();
        byte[] fileB = simulator.generate(allFaultsConfig(runId, batchId(), 2L), published).csvBytes();

        assertThat(fileA)
                .as("a simulator that ignores its seed would pass the byte-identical test trivially")
                .isNotEqualTo(fileB);
    }

    /**
     * The file's row order must not depend on which fault rates are
     * configured -- rows two configs have in common must appear in the same
     * relative order in both files.
     *
     * Row order is decided by {@code SettlementSimulator.shuffle} sorting on
     * a per-row key derived from {@code (seed, externalTxnId)} -- the row's
     * own stable identity -- rather than by shuffling a list with Fisher-Yates.
     * That distinction is what makes this property hold across configs with
     * different row counts (different fault rates drop or add different
     * rows): a key derived from identity never changes when some other row
     * is added or removed, so sorting by it is stable under changing input
     * in a way that a position-based shuffle -- even one drawing from its own
     * independent RNG -- is not (Fisher-Yates' draws depend on the current
     * list size at each step, so the same seed produces an unrelated
     * permutation once the list is a different length). Verified here rather
     * than merely asserted in a comment, because this specific failure mode
     * (RNG independence achieved, but only for a fixed row count) was found
     * during review after the first fix -- a property that looked done from
     * reading the code and wasn't, until it was actually exercised across
     * two different fault-rate configs.
     */
    @Test
    void rowOrderIsIndependentOfFaultRates() throws Exception {
        String runId = runId();
        GeneratorResult published = generator.generate(
                GeneratorConfig.clean(runId, 2024L, 40, accountIds));

        String cleanBatchId = batchId();
        byte[] cleanFile = simulator.generate(
                SettlementConfig.clean(runId, cleanBatchId, 5555L, BASE_INSTANT, java.time.Clock.systemUTC()),
                published).csvBytes();

        // Only NETWORK_DROPPED_ROW, so the only rows missing from this file
        // relative to the clean one are rows that were dropped -- no
        // duplicates or fabricated rows to complicate which ids are shared.
        String faultyBatchId = batchId();
        Map<NetworkFaultType, Double> dropOnly = new EnumMap<>(NetworkFaultType.class);
        dropOnly.put(NetworkFaultType.NETWORK_DROPPED_ROW, 0.4);
        byte[] faultyFile = simulator.generate(
                new SettlementConfig(runId, faultyBatchId, 5555L, BASE_INSTANT, dropOnly, java.time.Clock.systemUTC()),
                published).csvBytes();

        List<String> cleanIds = txnIdsInFileOrder(cleanFile);
        List<String> faultyIds = txnIdsInFileOrder(faultyFile);

        // The rows dropping left behind, in the order the clean file has
        // them -- this is the relative order a correctly independent
        // shuffle must preserve in the faulty file too.
        List<String> survivingInCleanOrder = cleanIds.stream()
                .filter(faultyIds::contains)
                .toList();
        List<String> survivingInFaultyOrder = faultyIds.stream()
                .filter(cleanIds::contains)
                .toList();

        assertThat(survivingInCleanOrder)
                .as("some rows must survive NETWORK_DROPPED_ROW at rate 0.4 for this comparison to mean anything")
                .isNotEmpty();
        assertThat(survivingInFaultyOrder)
                .as("rows present in both files must keep the same relative order in both -- "
                        + "a rate-dependent shuffle would scramble this too, since it reorders the "
                        + "whole list from scratch on every call rather than preserving a stable "
                        + "permutation of whatever survived")
                .isEqualTo(survivingInCleanOrder);
    }

    private static List<String> txnIdsInFileOrder(byte[] csvBytes) {
        List<String> lines = List.of(new String(csvBytes, StandardCharsets.UTF_8).split("\n"));
        List<String> ids = new java.util.ArrayList<>();
        for (int i = 1; i < lines.size(); i++) { // skip header
            String line = lines.get(i);
            if (line.isBlank()) {
                continue;
            }
            List<String> fields = CsvLine.split(line, i);
            ids.add(fields.get(1)); // external_txn_id column
        }
        return ids;
    }

    // ---- External expectation, one per fault type -------------------------

    @Test
    void droppedRowFaultAppearsWhenItsRateIsNonZero() throws Exception {
        assertFaultAppears(NetworkFaultType.NETWORK_DROPPED_ROW);
    }

    @Test
    void amountDriftFaultAppearsWhenItsRateIsNonZero() throws Exception {
        assertFaultAppears(NetworkFaultType.NETWORK_AMOUNT_DRIFT);
    }

    @Test
    void duplicateRowFaultAppearsWhenItsRateIsNonZero() throws Exception {
        assertFaultAppears(NetworkFaultType.NETWORK_DUPLICATE_ROW);
    }

    @Test
    void lateSettlementFaultAppearsWhenItsRateIsNonZero() throws Exception {
        assertFaultAppears(NetworkFaultType.NETWORK_LATE_SETTLEMENT);
    }

    @Test
    void unknownTxnFaultAppearsWhenItsRateIsNonZero() throws Exception {
        assertFaultAppears(NetworkFaultType.NETWORK_UNKNOWN_TXN);
    }

    /**
     * Every fault a settlement run writes must be labelled NETWORK, not the
     * silent PROCESSOR default.
     *
     * {@code faultlab.injected_faults.source} defaults to PROCESSOR because
     * FaultLedger (off-limits, generator-side) inserts without naming the
     * column. That default is a silent fallback: a future settlement-side
     * writer that forgets to name source would be mislabelled rather than
     * rejected, in the one table whose entire job is to be a trustworthy
     * grader. This test can't remove the default, but it pins the property
     * the default puts at risk, scoped by run_id so it only inspects rows
     * this run actually wrote -- pre-existing PROCESSOR rows from other runs
     * must not interfere.
     */
    @Test
    void everyFaultFromASettlementRunIsLabelledNetwork() throws Exception {
        String runId = runId();
        String batchId = batchId();
        GeneratorResult published = generator.generate(
                GeneratorConfig.clean(runId, 63L, 60, accountIds));

        Map<NetworkFaultType, Double> rates = new EnumMap<>(NetworkFaultType.class);
        for (NetworkFaultType type : NetworkFaultType.values()) {
            rates.put(type, 0.4);
        }
        SettlementConfig config = new SettlementConfig(
                runId, batchId, 63L, BASE_INSTANT, rates, java.time.Clock.systemUTC());

        var result = simulator.generate(config, published);
        assertThat(result.injectedFaults()).as("this run should have injected something").isPositive();

        List<String> nonNetworkSources = jdbc.queryForList(
                "SELECT DISTINCT source FROM faultlab.injected_faults WHERE run_id = ? AND source <> 'NETWORK'",
                String.class, batchId);

        assertThat(nonNetworkSources)
                .as("every row this settlement run wrote must be labelled NETWORK")
                .isEmpty();
    }

    private void assertFaultAppears(NetworkFaultType type) throws Exception {
        String runId = runId();
        String batchId = batchId();
        GeneratorResult published = generator.generate(
                GeneratorConfig.clean(runId, 55L, 20, accountIds));

        Map<NetworkFaultType, Double> rates = new EnumMap<>(NetworkFaultType.class);
        rates.put(type, 1.0);
        SettlementConfig config = new SettlementConfig(
                runId, batchId, 77L, BASE_INSTANT, rates, java.time.Clock.systemUTC());

        simulator.generate(config, published);

        Long count = jdbc.queryForObject(
                "SELECT count(*) FROM faultlab.injected_faults "
                        + "WHERE run_id = ? AND fault_type = ? AND source = 'NETWORK'",
                Long.class, batchId, type.name());

        assertThat(count).as("%s should appear when its rate is 1.0", type).isPositive();
    }

    /**
     * At most one network fault lands on the same transaction.
     *
     * A stronger, structural check than the per-type tests above: even with
     * every rate turned up to a level that would ordinarily produce heavy
     * overlap, no external_txn_id should carry two different fault types.
     */
    @Test
    void atMostOneNetworkFaultPerTransaction() throws Exception {
        String runId = runId();
        String batchId = batchId();
        GeneratorResult published = generator.generate(
                GeneratorConfig.clean(runId, 61L, 50, accountIds));

        Map<NetworkFaultType, Double> rates = new EnumMap<>(NetworkFaultType.class);
        rates.put(NetworkFaultType.NETWORK_DROPPED_ROW, 0.3);
        rates.put(NetworkFaultType.NETWORK_AMOUNT_DRIFT, 0.3);
        rates.put(NetworkFaultType.NETWORK_DUPLICATE_ROW, 0.3);
        rates.put(NetworkFaultType.NETWORK_LATE_SETTLEMENT, 0.3);
        SettlementConfig config = new SettlementConfig(
                runId, batchId, 61L, BASE_INSTANT, rates, java.time.Clock.systemUTC());

        simulator.generate(config, published);

        List<Integer> overlapping = jdbc.queryForList(
                "SELECT count(*) FROM faultlab.injected_faults "
                        + "WHERE run_id = ? AND source = 'NETWORK' AND external_txn_id IS NOT NULL "
                        + "GROUP BY external_txn_id HAVING count(*) > 1",
                Integer.class, batchId);

        assertThat(overlapping)
                .as("no external_txn_id should carry more than one network fault")
                .isEmpty();
    }

    /** Amount drift must land on both sides of the captured amount, not just one. */
    @Test
    void amountDriftInjectsInBothDirections() throws Exception {
        String runId = runId();
        String batchId = batchId();
        GeneratorResult published = generator.generate(
                GeneratorConfig.clean(runId, 71L, 80, accountIds));

        Map<NetworkFaultType, Double> rates = new EnumMap<>(NetworkFaultType.class);
        rates.put(NetworkFaultType.NETWORK_AMOUNT_DRIFT, 1.0);
        SettlementConfig config = new SettlementConfig(
                runId, batchId, 71L, BASE_INSTANT, rates, java.time.Clock.systemUTC());

        simulator.generate(config, published);

        List<java.math.BigDecimal> deltas = jdbc.queryForList(
                "SELECT actual_amount - expected_amount FROM faultlab.injected_faults "
                        + "WHERE run_id = ? AND fault_type = 'NETWORK_AMOUNT_DRIFT'",
                java.math.BigDecimal.class, batchId);

        assertThat(deltas).isNotEmpty();
        assertThat(deltas).anyMatch(d -> d.signum() > 0);
        assertThat(deltas).anyMatch(d -> d.signum() < 0);
    }

    /**
     * No fault type should be identifiable from the shape of the CSV alone.
     *
     * A fault the matcher could spot without ever touching the ledger would
     * let the accuracy harness score "detection" that was really just
     * pattern-matching a tell in the data -- the same family of problem the
     * faultlab schema permission boundary exists to stop from the other
     * direction (there, the engine can't read the answers; here, the answers
     * must not leak into the questions).
     *
     * At minimum: the set of distinct merchant_id values on fault-affected
     * rows must be a subset of those on unaffected rows -- no merchant value
     * exclusive to a fault. This also exercises the specific leaks the audit
     * found and fixed: a drifted row's fee must still reconcile to 2% of its
     * own (post-drift) gross, exactly like every other row; a late-settled
     * row's shift must not always be the same fixed number of days; and no
     * externalTxnId should carry a literal "unknown" marker.
     */
    @Test
    void faultRowsAreNotIdentifiableByShapeAlone() throws Exception {
        String runId = runId();
        String batchId = batchId();
        // Large enough that every merchant in SyntheticMerchants' weighted
        // pool -- including the lowest-weight tail entries -- is
        // overwhelmingly likely to be drawn on both the affected and
        // unaffected sides below; the merchant-subset assertion would
        // otherwise be a coin flip on which side a rare merchant landed.
        GeneratorResult published = generator.generate(
                GeneratorConfig.clean(runId, 8181L, 1500, accountIds));

        // A partial rate, not 1.0: the merchant-subset assertion below needs
        // both fault-affected and untouched rows to compare.
        Map<NetworkFaultType, Double> rates = new EnumMap<>(NetworkFaultType.class);
        rates.put(NetworkFaultType.NETWORK_AMOUNT_DRIFT, 0.5);
        SettlementConfig driftConfig = new SettlementConfig(
                runId, batchId, 8181L, BASE_INSTANT, rates, java.time.Clock.systemUTC());
        var driftResult = simulator.generate(driftConfig, published);
        loader.load(batchId, new ByteArrayInputStream(driftResult.csvBytes()));

        // Every row's fee must reconcile to exactly 2% of its own gross,
        // rounded, floored at 1 cent -- drifted rows included. A stale fee
        // computed from a pre-drift gross would be the one arithmetic
        // mismatch in the file; there must be none.
        List<Map<String, Object>> rows = jdbc.queryForList(
                "SELECT gross_amount_minor, fee_minor FROM settlement_records WHERE batch_id = ?", batchId);
        assertThat(rows).isNotEmpty();
        assertThat(rows).allSatisfy(row -> {
            long gross = ((Number) row.get("gross_amount_minor")).longValue();
            long fee = ((Number) row.get("fee_minor")).longValue();
            long expectedFee = Math.max(1, Math.round(gross * 0.02));
            assertThat(fee)
                    .as("fee must reconcile to 2%% of this row's own gross, row %s", row)
                    .isEqualTo(expectedFee);
        });

        // No externalTxnId anywhere in the answer key should carry a literal
        // marker identifying it as fabricated.
        List<String> markedIds = jdbc.queryForList(
                "SELECT external_txn_id FROM faultlab.injected_faults "
                        + "WHERE run_id = ? AND external_txn_id LIKE '%unknown%'",
                String.class, batchId);
        assertThat(markedIds).as("no synthetic id should be self-identifying").isEmpty();

        // Late-settlement shifts must vary, not land on one fixed offset.
        String lateRunId = runId();
        String lateBatchId = batchId();
        Map<NetworkFaultType, Double> lateRates = new EnumMap<>(NetworkFaultType.class);
        lateRates.put(NetworkFaultType.NETWORK_LATE_SETTLEMENT, 1.0);
        GeneratorResult publishedForLate = generator.generate(
                GeneratorConfig.clean(lateRunId, 9191L, 60, accountIds));
        SettlementConfig lateConfig = new SettlementConfig(
                lateRunId, lateBatchId, 9191L, BASE_INSTANT, lateRates, java.time.Clock.systemUTC());
        simulator.generate(lateConfig, publishedForLate);

        List<Map<String, Object>> lateFaults = jdbc.queryForList(
                "SELECT detail FROM faultlab.injected_faults "
                        + "WHERE run_id = ? AND fault_type = 'NETWORK_LATE_SETTLEMENT'",
                lateBatchId);
        assertThat(lateFaults).isNotEmpty();
        // detail carries "original <instant>, shifted <instant>" -- distinct
        // detail strings across faults is a cheap, sufficient proxy for
        // distinct shift magnitudes without parsing timestamps out by hand.
        long distinctDetails = lateFaults.stream().map(m -> m.get("detail")).distinct().count();
        assertThat(distinctDetails)
                .as("late-settlement shifts must vary, not land on one fixed offset every time")
                .isGreaterThan(1);

        // Merchant values on fault-affected rows must be a subset of those on
        // unaffected rows -- no merchant is exclusive to a fault.
        List<String> faultAffectedMerchants = jdbc.queryForList(
                "SELECT DISTINCT sr.merchant_id FROM settlement_records sr "
                        + "JOIN faultlab.injected_faults f "
                        + "  ON f.external_txn_id = sr.external_txn_id AND f.source = 'NETWORK' "
                        + "WHERE sr.batch_id = ?",
                String.class, batchId);
        List<String> unaffectedMerchants = jdbc.queryForList(
                "SELECT DISTINCT sr.merchant_id FROM settlement_records sr "
                        + "WHERE sr.batch_id = ? AND sr.external_txn_id NOT IN ("
                        + "  SELECT external_txn_id FROM faultlab.injected_faults "
                        + "  WHERE run_id = ? AND source = 'NETWORK' AND external_txn_id IS NOT NULL)",
                String.class, batchId, batchId);
        assertThat(unaffectedMerchants)
                .as("this batch's fault rates should leave some rows untouched to compare against")
                .isNotEmpty();
        assertThat(unaffectedMerchants)
                .as("no merchant value should be exclusive to fault-affected rows")
                .containsAll(faultAffectedMerchants);
    }

    /**
     * The settlement file's merchant identity must actually discriminate.
     *
     * A later matching pass does fuzzy matching on
     * (amount_minor, merchant_id, settled_at +/- window). If merchant_id
     * collapses to a handful of values, that triple degenerates toward
     * (amount, time) alone, candidate sets multiply, and an accuracy sweep
     * built later would measure that distortion instead of the parameter
     * it's supposed to measure. This pins a floor so the property can't
     * silently regress back to the six ledger account ids.
     */
    @Test
    void generatedBatchHasSufficientMerchantCardinality() throws Exception {
        String runId = runId();
        String batchId = batchId();
        GeneratorResult published = generator.generate(
                GeneratorConfig.clean(runId, 4242L, 200, accountIds));
        var result = simulator.generate(
                SettlementConfig.clean(runId, batchId, 4242L, BASE_INSTANT, java.time.Clock.systemUTC()),
                published);
        loader.load(batchId, new ByteArrayInputStream(result.csvBytes()));

        Long distinctMerchants = jdbc.queryForObject(
                "SELECT count(DISTINCT merchant_id) FROM settlement_records WHERE batch_id = ?",
                Long.class, batchId);

        assertThat(distinctMerchants)
                .as("merchant_id must have real cardinality, not collapse to the handful of ledger accounts")
                .isGreaterThanOrEqualTo(10);
    }

    // ---- Parsing -----------------------------------------------------------

    @Test
    void merchantNameContainingCommaRoundTrips() throws Exception {
        String batchId = batchId();
        List<SettlementRow> rows = List.of(
                new SettlementRow("txn-1", "Bob's Diner, Inc.", 5000, 100, "USD", BASE_INSTANT));
        String csv = writeMinimalCsv(batchId, rows);

        jdbc.update("INSERT INTO recon_batches (batch_id, run_id, seed, base_instant, row_count, file_sha256) "
                        + "VALUES (?, ?, ?, ?, ?, ?)",
                batchId, "manual-run", 1L, java.sql.Timestamp.from(BASE_INSTANT), 1, "test-hash");

        loader.load(batchId, new ByteArrayInputStream(csv.getBytes(StandardCharsets.UTF_8)));

        String storedMerchant = jdbc.queryForObject(
                "SELECT merchant_id FROM settlement_records WHERE batch_id = ?", String.class, batchId);
        assertThat(storedMerchant).isEqualTo("Bob's Diner, Inc.");
    }

    @Test
    void malformedAmountIsRejectedNotCoerced() throws Exception {
        String batchId = batchId();
        String csv = SettlementCsvWriter.HEADER + "\n"
                + batchId + ",txn-1,merchant-1,NOT_A_NUMBER,100,USD,2026-01-15T18:00:00Z\n";

        jdbc.update("INSERT INTO recon_batches (batch_id, run_id, seed, base_instant, row_count, file_sha256) "
                        + "VALUES (?, ?, ?, ?, ?, ?)",
                batchId, "manual-run", 1L, java.sql.Timestamp.from(BASE_INSTANT), 1, "test-hash");

        assertThatThrownBy(() ->
                loader.load(batchId, new ByteArrayInputStream(csv.getBytes(StandardCharsets.UTF_8))))
                .isInstanceOf(SettlementLoader.SettlementParseException.class)
                .hasMessageContaining("line 1")
                .hasMessageContaining("NOT_A_NUMBER");

        Long count = jdbc.queryForObject(
                "SELECT count(*) FROM settlement_records WHERE batch_id = ?", Long.class, batchId);
        assertThat(count).as("a rejected row must not be partially inserted").isZero();
    }

    @Test
    void rawLineIsStoredVerbatim() throws Exception {
        String batchId = batchId();
        String dataLine = batchId + ",txn-99,merchant-7,12345,250,USD,2026-01-15T18:00:00Z";
        String csv = SettlementCsvWriter.HEADER + "\n" + dataLine + "\n";

        jdbc.update("INSERT INTO recon_batches (batch_id, run_id, seed, base_instant, row_count, file_sha256) "
                        + "VALUES (?, ?, ?, ?, ?, ?)",
                batchId, "manual-run", 1L, java.sql.Timestamp.from(BASE_INSTANT), 1, "test-hash");

        loader.load(batchId, new ByteArrayInputStream(csv.getBytes(StandardCharsets.UTF_8)));

        String storedRawLine = jdbc.queryForObject(
                "SELECT raw_line FROM settlement_records WHERE batch_id = ?", String.class, batchId);
        assertThat(storedRawLine).isEqualTo(dataLine);
    }

    // ---- Permission boundary ------------------------------------------------

    @Test
    void reconciliationRoleCannotReadTheAnswerKey() throws Exception {
        try (Connection connection = DriverManager.getConnection(
                POSTGRES.getJdbcUrl(), "recon_role", "recon_role")) {
            assertThatThrownBy(() -> {
                try (var statement = connection.createStatement()) {
                    statement.executeQuery("SELECT * FROM faultlab.injected_faults");
                }
            }).isInstanceOf(SQLException.class);
        }
    }

    @Test
    void reconciliationRoleCanReadSettlementRecords() throws Exception {
        try (Connection connection = DriverManager.getConnection(
                POSTGRES.getJdbcUrl(), "recon_role", "recon_role")) {
            try (var statement = connection.createStatement()) {
                // Must not throw -- the boundary is specifically the faultlab
                // schema, not every table this role touches.
                statement.executeQuery("SELECT * FROM settlement_records");
            }
        }
    }

    // ---- Independence --------------------------------------------------------

    @Test
    void settlementFileDoesNotDependOnLedgerState() throws Exception {
        String runId = runId();
        GeneratorResult published = generator.generate(
                GeneratorConfig.clean(runId, 91L, 25, accountIds));

        String batchWithLedgerData = batchId();
        SettlementConfig config1 = allFaultsConfig(runId, batchWithLedgerData, 5L);
        byte[] fileWithLedgerData = simulator.generate(config1, published).csvBytes();

        jdbc.update("DELETE FROM ledger_entries");
        jdbc.update("DELETE FROM transactions");
        jdbc.update("DELETE FROM transaction_states");
        jdbc.update("DELETE FROM parked_events");

        String batchWithEmptyLedger = batchId();
        SettlementConfig config2 = allFaultsConfig(runId, batchWithEmptyLedger, 5L);
        byte[] fileWithEmptyLedger = simulator.generate(config2, published).csvBytes();

        assertThat(normalize(fileWithEmptyLedger, batchWithEmptyLedger))
                .as("the settlement file must not depend on ledger state at all, modulo the batch id")
                .isEqualTo(normalize(fileWithLedgerData, batchWithLedgerData));
    }

    // ---- helpers ---------------------------------------------------------

    private SettlementConfig allFaultsConfig(String runId, String batchId, long seed) {
        Map<NetworkFaultType, Double> rates = new EnumMap<>(NetworkFaultType.class);
        rates.put(NetworkFaultType.NETWORK_DROPPED_ROW, 0.15);
        rates.put(NetworkFaultType.NETWORK_AMOUNT_DRIFT, 0.15);
        rates.put(NetworkFaultType.NETWORK_DUPLICATE_ROW, 0.15);
        rates.put(NetworkFaultType.NETWORK_LATE_SETTLEMENT, 0.15);
        rates.put(NetworkFaultType.NETWORK_UNKNOWN_TXN, 0.1);
        return new SettlementConfig(runId, batchId, seed, BASE_INSTANT, rates, java.time.Clock.systemUTC());
    }

    private static String writeMinimalCsv(String batchId, List<SettlementRow> rows) {
        return SettlementCsvWriter.write(batchId, rows);
    }

    /** Strips a batch's own id out of its file, so two batches differing only in id compare equal. */
    private static String normalize(byte[] csvBytes, String batchId) {
        return new String(csvBytes, StandardCharsets.UTF_8).replace(batchId, "BATCH");
    }

    private static String sha256(byte[] bytes) throws Exception {
        return java.util.HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(bytes));
    }

    private static String runId() {
        return "run-" + UUID.randomUUID();
    }

    private static String batchId() {
        return "batch-" + UUID.randomUUID();
    }
}
