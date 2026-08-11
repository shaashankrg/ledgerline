package com.ledgerline.harness;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import java.io.ByteArrayInputStream;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.EnumMap;
import java.util.EnumSet;
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
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.kafka.ConfluentKafkaContainer;

import com.ledgerline.generator.GeneratorConfig;
import com.ledgerline.generator.TransactionGenerator;
import com.ledgerline.generator.TransactionGenerator.GeneratorResult;
import com.ledgerline.reconciliation.ReconciliationService;
import com.ledgerline.settlement.NetworkFaultType;
import com.ledgerline.settlement.SettlementConfig;
import com.ledgerline.settlement.SettlementLoader;
import com.ledgerline.settlement.SettlementSimulator;

/**
 * Asserts, in both directions, that fault injection and fault detection stay
 * lined up -- a check the project did not have until now, and whose absence
 * is why three separate reachability gaps (VOIDED payments, late settlement,
 * and the missing mangled-id fault) were each found by hand, late, after
 * degrading Day 4's numbers in ways that looked like matcher defects rather
 * than what they actually were: a fault with no detector, or a detector with
 * no fault.
 *
 * <h2>Why this reads {@code faultlab}, and why that is not the isolation
 * violation it would be anywhere else</h2>
 *
 * The reconciliation engine -- {@code ReconciliationService},
 * {@code ReconciliationAudit}, everything in {@code com.ledgerline.reconciliation}
 * that runs as {@code recon_role} -- must never read {@code faultlab}. That
 * rule is what makes Day 4's accuracy numbers meaningful: an engine graded
 * against an answer key it can freely read is not being tested at all.
 *
 * This class is not that engine. It is a build-time check on the test
 * infrastructure itself: it asks "does the fault the simulator can inject
 * have somewhere to land in the classifier," which is a question about the
 * *shape* of the system, decided once per build, never at runtime, and never
 * consulted by any classification decision {@link ReconciliationService}
 * makes. It connects with the application's own privileged datasource (the
 * default {@code JdbcTemplate}, the same one every other settlement test in
 * this project uses), not {@code recon_role} -- because {@code recon_role}
 * is the credential that must never see {@code faultlab}, and this class is
 * not that credential's caller.
 *
 * Day 4's accuracy harness will read {@code faultlab} too, for the same
 * reason and under the same justification: it computes precision and recall
 * *about* the engine's output, from outside it, after the fact. Neither this
 * class nor that harness informs, gates, or has any causal path into a
 * classification decision. That is the boundary that matters, and it is
 * narrower than "never touches the schema" -- it is "never touches it from
 * inside the thing being graded."
 *
 * {@code SettlementSimulatorTest.reconciliationRoleCannotReadTheAnswerKey}
 * and {@code ReconciliationServiceTest.reconciliationRoleCannotReadTheAnswerKey}
 * are what actually enforce the rule this class's existence depends on
 * remaining true. Both are unmodified.
 */
@SpringBootTest
class FaultReachabilityTest {

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

    private static final int TRANSACTION_COUNT = 60;

    /**
     * Fault types with a known, documented, open reachability gap.
     *
     * Short and uncomfortable to look at on purpose -- each entry here is a
     * standing admission that an injected fault currently produces no
     * detectable classification, and each must name the item tracking its
     * fix rather than being silently skipped.
     *
     * NETWORK_LATE_SETTLEMENT: a late-but-otherwise-correct row exact-matches
     * on id, and exact matching does not look at time, so it lands MATCHED
     * with no exception raised at all. See
     * {@code LateSettlementReachabilityTest}, which pins this exact
     * behaviour, and the Day 3 fix-up report's item 3, which populates
     * {@code time_delta_seconds} on exact matches as the evidence-preserving
     * first step without yet deciding whether lateness becomes a
     * classification. Until that decision is made, this fault is
     * *deliberately* unreachable, not accidentally so -- but it still
     * belongs on this list, because "deliberate" is not the same as
     * "resolved."
     */
    private static final Set<NetworkFaultType> KNOWN_UNREACHABLE_FAULTS =
            EnumSet.of(NetworkFaultType.NETWORK_LATE_SETTLEMENT);

    /**
     * For each fault type the simulator can emit, generate a batch
     * containing only that fault, reconcile it, and assert every row the
     * fault touched produced *something* other than a plain MATCHED --
     * either a non-MATCHED outcome or an exception. Not whether the label is
     * the "right" one; only whether the engine noticed at all.
     *
     * Driven from {@link NetworkFaultType#values()}, not a hand-maintained
     * list, so a seventh fault type added later is covered automatically and
     * fails this test the moment it has no classification path -- which is
     * the entire value of the check. A test that only knew about today's six
     * types would not have caught the next gap any better than the humans
     * who found the first three by hand.
     */
    @Test
    void everyInjectedFaultTypeHasAReachableClassification() throws Exception {
        Map<NetworkFaultType, Boolean> reachability = new EnumMap<>(NetworkFaultType.class);

        for (NetworkFaultType type : NetworkFaultType.values()) {
            boolean reachable = isReachable(type);
            reachability.put(type, reachable);
            System.out.println("[FaultReachabilityTest] " + type + " reachable=" + reachable
                    + (KNOWN_UNREACHABLE_FAULTS.contains(type) ? " (documented gap)" : ""));
        }

        List<NetworkFaultType> unexpectedlyUnreachable = reachability.entrySet().stream()
                .filter(e -> !e.getValue())
                .map(Map.Entry::getKey)
                .filter(type -> !KNOWN_UNREACHABLE_FAULTS.contains(type))
                .toList();

        assertThat(unexpectedlyUnreachable)
                .as("fault types with no reachable classification and no entry in "
                        + "KNOWN_UNREACHABLE_FAULTS -- either the classifier has a gap, "
                        + "or this fault belongs on that list with a reason: %s",
                        unexpectedlyUnreachable)
                .isEmpty();

        // The documented gap must still actually be a gap. If a change
        // elsewhere made NETWORK_LATE_SETTLEMENT reachable without anyone
        // updating this list, that is exactly the kind of drift this class
        // exists to catch -- in the opposite direction from the usual one.
        for (NetworkFaultType known : KNOWN_UNREACHABLE_FAULTS) {
            assertThat(reachability.get(known))
                    .as("%s is listed as a known gap but is now reachable -- "
                            + "update KNOWN_UNREACHABLE_FAULTS and the item tracking it", known)
                    .isFalse();
        }
    }

    /**
     * Exception types with a known, documented gap on this side of the round
     * trip: reachable in the classifier and covered by direct tests, but not
     * producible by any {@code NetworkFaultType} the settlement simulator can
     * inject.
     *
     * STATE_CONFLICT: found by running this very test. It requires a payment
     * whose lifecycle state is something other than CAPTURED or SETTLED --
     * VOIDED, REFUNDED, AUTHORIZED-and-never-captured -- and
     * {@code TransactionGenerator.planTransaction} (Week 1-2 code, off
     * limits) only ever emits AUTHORIZE, CAPTURE, and SETTLE under
     * {@code GeneratorConfig.clean}. No NetworkFaultType controls the
     * generator's event stream -- that is a different layer entirely, one
     * this simulator has no lever into by design (see
     * SettlementSimulator's own class comment: it derives the honest view
     * from published messages, it does not decide what gets published). So
     * STATE_CONFLICT is real, tested directly
     * ({@code ReconciliationServiceTest.voidedPaymentSettlementLineIsStateConflict},
     * {@code refundedPaymentSettlementLineIsStateConflict}), and currently
     * unreachable from generated data by any means -- a gap in the fault
     * side, not the classifier side, and outside this Day 3 fix-up's scope
     * to close (it would mean either extending TransactionGenerator, which
     * is off limits, or adding a settlement-side fault that can only ever
     * describe a state the settlement file itself has no way to cause).
     * Reported rather than routed around.
     */
    private static final Set<String> KNOWN_UNREACHABLE_EXCEPTION_TYPES = Set.of("STATE_CONFLICT");

    /**
     * The other direction: every exception type the classifier can produce
     * must be reachable from at least one injected fault. An exception type
     * nothing can trigger is either dead code in the classifier or a
     * ground-truth fault this simulator is missing.
     *
     * MISSING_IN_SETTLEMENT is the one type not driven by a settlement fault
     * at all -- it fires when a captured payment has no settlement line
     * naming it, which NETWORK_DROPPED_ROW produces directly (the row's
     * absence from the file *is* the fault). Covered here the same way as
     * every other type, not exempted, because the round trip has to hold for
     * all five as far as this test's scope reaches -- see
     * KNOWN_UNREACHABLE_EXCEPTION_TYPES for the one type that scope does not
     * cover.
     */
    @Test
    void everyExceptionTypeIsProducibleBySomeInjectedFault() throws Exception {
        Set<String> producedExceptionTypes = new java.util.HashSet<>();

        for (NetworkFaultType type : NetworkFaultType.values()) {
            producedExceptionTypes.addAll(exceptionTypesProducedBy(type));
        }

        System.out.println("[FaultReachabilityTest] exception types produced across all faults: "
                + producedExceptionTypes);

        List<String> allExceptionTypes = List.of(
                "AMOUNT_MISMATCH", "MISSING_IN_LEDGER", "MISSING_IN_SETTLEMENT",
                "DUPLICATE_SETTLEMENT", "STATE_CONFLICT");

        List<String> untriggerable = allExceptionTypes.stream()
                .filter(t -> !producedExceptionTypes.contains(t))
                .filter(t -> !KNOWN_UNREACHABLE_EXCEPTION_TYPES.contains(t))
                .toList();

        assertThat(untriggerable)
                .as("exception type(s) no injected fault can currently trigger and no entry in "
                        + "KNOWN_UNREACHABLE_EXCEPTION_TYPES -- either the classifier has a gap, "
                        + "or this type belongs on that list with a reason: %s", untriggerable)
                .isEmpty();

        // The documented gap must still actually be a gap, same reasoning as
        // the fault-direction test's check.
        for (String known : KNOWN_UNREACHABLE_EXCEPTION_TYPES) {
            assertThat(producedExceptionTypes)
                    .as("%s is listed as a known gap but some fault now produces it -- "
                            + "update KNOWN_UNREACHABLE_EXCEPTION_TYPES", known)
                    .doesNotContain(known);
        }
    }

    /**
     * Generates a batch containing only {@code type} at rate 1.0, reconciles
     * it, and reports whether any row landed somewhere other than a plain
     * MATCHED-with-no-exception.
     */
    private boolean isReachable(NetworkFaultType type) throws Exception {
        return !exceptionTypesProducedBy(type).isEmpty() || anyNonMatchedOutcome(type);
    }

    private boolean anyNonMatchedOutcome(NetworkFaultType type) throws Exception {
        long reconRunId = runBatchWithOnlyFault(type);
        Integer nonMatched = jdbc.queryForObject(
                "SELECT count(*) FROM recon_line_outcomes "
                        + "WHERE recon_run_id = ? AND outcome <> 'MATCHED'",
                Integer.class, reconRunId);
        return nonMatched != null && nonMatched > 0;
    }

    private Set<String> exceptionTypesProducedBy(NetworkFaultType type) throws Exception {
        long reconRunId = runBatchWithOnlyFault(type);
        List<String> types = jdbc.queryForList(
                "SELECT DISTINCT type FROM recon_exceptions WHERE recon_run_id = ?",
                String.class, reconRunId);
        return new java.util.HashSet<>(types);
    }

    private long runBatchWithOnlyFault(NetworkFaultType type) throws Exception {
        String runId = "run-" + UUID.randomUUID();
        GeneratorResult published = generator.generate(
                GeneratorConfig.clean(runId, faultSeed(type), TRANSACTION_COUNT, accountIds));

        await().atMost(Duration.ofSeconds(30)).until(() -> countCaptureTransactions(runId) >= TRANSACTION_COUNT);

        String batchId = "batch-" + UUID.randomUUID();
        Map<NetworkFaultType, Double> onlyThisFault = new EnumMap<>(NetworkFaultType.class);
        onlyThisFault.put(type, 1.0);

        SettlementConfig config = new SettlementConfig(
                runId, batchId, faultSeed(type), Instant.now(), onlyThisFault, Clock.systemUTC());
        SettlementSimulator.SettlementResult result = simulator.generate(config, published);
        loader.load(batchId, new ByteArrayInputStream(result.csvBytes()));

        reconciliationService.run(batchId);

        return jdbc.queryForObject(
                "SELECT recon_run_id FROM recon_runs WHERE batch_id = ?", Long.class, batchId);
    }

    /** A distinct seed per fault type so runs across the sweep never collide. */
    private static long faultSeed(NetworkFaultType type) {
        return 9_000_000L + type.ordinal();
    }

    private long countCaptureTransactions(String runId) {
        Long count = jdbc.queryForObject(
                "SELECT count(*) FROM transactions WHERE idempotency_key LIKE ?",
                Long.class, runId + "-txn-%:CAPTURE");
        return count == null ? 0 : count;
    }
}
