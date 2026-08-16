package com.ledgerline.messaging;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Random;
import java.util.concurrent.atomic.AtomicBoolean;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;

import com.ledgerline.generator.GeneratorConfig;
import com.ledgerline.generator.TransactionGenerator;
import com.ledgerline.generator.TransactionGenerator.GeneratorResult;

/**
 * Day 9's second deferred gap: how many payments does auto-commit actually
 * lose under a real pod kill, against the real Deployment's own consumer --
 * not a simulated approximation.
 *
 * <h2>Why this needs a real Helm upgrade, not a test-local consumer</h2>
 *
 * {@code enable.auto.commit} is hardcoded to {@code false} in the shipped
 * {@link KafkaConsumerConfig} for exactly the reason its own comment gives:
 * the client commits offsets on a timer, independently of whether the
 * record was actually handled, so a record polled and still mid-write when
 * the process dies is gone with nothing to signal it. That argument has
 * always been correct by construction, but Day 9 is where it stops being an
 * argument and becomes a measurement -- {@code
 * ledgerline.consumer.auto-commit-enabled} exists (see {@code
 * KafkaConsumerConfig}'s comment on it) purely so this test can flip the
 * REAL Deployment's consumer into the unsafe configuration via a real
 * {@code helm upgrade}, kill a REAL pod mid-batch under load, and count the
 * REAL number of payments that never arrive -- then revert immediately.
 *
 * This class does not perform the {@code helm upgrade}/revert itself (shell
 * commands against a live release are exactly the kind of action worth a
 * human running deliberately, see {@code docs/day9-auto-commit-loss.md} for
 * the exact commands) -- it assumes the caller has already flipped
 * {@code processor.consumer.autoCommitEnabled=true} and applied it before
 * running {@link #measureLossUnderAutoCommit()}, and that they will revert
 * it afterward. What this class does is generate real load, kill a real pod
 * at a moment chosen to land mid-batch, and report the precise, directly
 * counted loss -- the actual deliverable.
 */
@SpringBootTest
@EnabledIfSystemProperty(named = "ledgerline.chaostest", matches = "true")
class AutoCommitLossMeasurementTest {

    private static final Logger log = LoggerFactory.getLogger(AutoCommitLossMeasurementTest.class);

    private static final String NAMESPACE = System.getProperty("ledgerline.chaostest.namespace", "ledgerline");
    private static final String RELEASE = System.getProperty("ledgerline.chaostest.release", "ledgerline");
    private static final String PROCESSOR_LABEL = "app=" + RELEASE + "-processor";

    private static final int KAFKA_EXTERNAL_PORT = 9094;
    private static final int POSTGRES_LOCAL_PORT = 15433; // distinct from ChaosInvariantTest's forward

    private static Process postgresPortForward;

    @BeforeAll
    static void portForwardServices() throws Exception {
        postgresPortForward = KubectlTestSupport.startPortForward(
                NAMESPACE, "svc/" + RELEASE + "-postgres", 5432, POSTGRES_LOCAL_PORT);
        KubectlTestSupport.awaitPortOpen("localhost", KAFKA_EXTERNAL_PORT, Duration.ofSeconds(30));
        KubectlTestSupport.awaitPortOpen("localhost", POSTGRES_LOCAL_PORT, Duration.ofSeconds(30));
    }

    @AfterAll
    static void stopPortForwards() {
        if (postgresPortForward != null) {
            postgresPortForward.destroyForcibly();
        }
    }

    @DynamicPropertySource
    static void containerProperties(DynamicPropertyRegistry registry) {
        registry.add("spring.kafka.bootstrap-servers", () -> "localhost:" + KAFKA_EXTERNAL_PORT);
        registry.add("spring.datasource.url", () -> "jdbc:postgresql://localhost:" + POSTGRES_LOCAL_PORT + "/ledgerline");
        registry.add("spring.datasource.username", () -> "ledgerline");
        registry.add("spring.datasource.password", () -> "ledgerline");
    }

    @Autowired
    private TransactionGenerator generator;

    @Autowired
    private JdbcTemplate jdbc;

    /**
     * Generates a burst of load, kills a random processor pod partway
     * through, waits for the survivors to finish what they can, and counts
     * -- precisely -- how many of this run's own transactions never
     * produced a balanced pair.
     *
     * This is NOT asserted pass/fail against zero: the entire point is to
     * observe and record a nonzero number when auto-commit is on. What it
     * DOES assert is that the measurement itself is trustworthy -- every
     * message was actually published (no confusing a publish failure with a
     * consumer-side loss).
     */
    @Test
    void measureLossUnderAutoCommit() throws Exception {
        int transactionCount = 3000;
        int ratePerSecond = 100;
        String runId = "autocommitloss-" + Instant.now().toEpochMilli();

        List<Long> accountIds = jdbc.queryForList("SELECT id FROM accounts ORDER BY id", Long.class);
        GeneratorConfig config = new GeneratorConfig(runId, 7001L, transactionCount, ratePerSecond,
                new java.util.EnumMap<>(com.ledgerline.generator.FaultType.class), accountIds);

        AtomicBoolean killed = new AtomicBoolean(false);
        Thread killerThread = new Thread(() -> killOnePodPartwayThrough(killed), "auto-commit-loss-killer");
        killerThread.start();

        log.info("AUTO-COMMIT LOSS TEST: generating {} transactions at {} tx/s (runId={})",
                transactionCount, ratePerSecond, runId);
        GeneratorResult result = generator.generate(config);
        killerThread.join(Duration.ofSeconds(30).toMillis());

        assertThat(result.publishFailures())
                .as("every message must have actually reached Kafka -- a publish failure would be "
                        + "a producer-side loss, not the consumer-side auto-commit loss this measures")
                .isZero();

        // Auto-commit's default interval is 5s (Kafka's own
        // auto.commit.interval.ms default) -- give the surviving consumers
        // comfortably longer than that to finish committing everything they
        // actually processed before counting what's missing.
        Thread.sleep(Duration.ofSeconds(20).toMillis());

        long published = result.messages().size();
        long expectedPairs = result.messages().stream()
                .filter(m -> m.eventType() == com.ledgerline.domain.EventType.CAPTURE)
                .count();
        long actualPairs = jdbc.queryForObject(
                "SELECT count(*) FROM ("
                        + "  SELECT e.transaction_id FROM ledger_entries e "
                        + "  JOIN transactions t ON t.id = e.transaction_id "
                        + "  WHERE t.idempotency_key LIKE ? "
                        + "  GROUP BY e.transaction_id "
                        + "  HAVING count(*) = 2 AND SUM(e.amount) = 0"
                        + ") balanced", Long.class, runId + "-%");

        long lostPayments = expectedPairs - actualPairs;

        log.info("AUTO-COMMIT LOSS TEST RESULT: published={} expectedPairs={} actualPairs={} "
                        + "LOST PAYMENTS={} (runId={})",
                published, expectedPairs, actualPairs, lostPayments, runId);

        // The measurement's own integrity check, not a pass/fail on loss
        // itself: actualPairs must never exceed expected (that would mean
        // double-writes, a different bug entirely) and the run must have
        // actually killed a pod, or this "measurement" measured nothing.
        assertThat(actualPairs)
                .as("balanced pairs written must never exceed what was published -- "
                        + "more would mean duplicate writes, a separate defect from loss")
                .isLessThanOrEqualTo(expectedPairs);
        assertThat(killed.get())
                .as("the pod-kill must actually have happened for this to be a real measurement")
                .isTrue();
    }

    /** Waits briefly, then kills one processor pod -- meant to land mid-batch under sustained load. */
    private void killOnePodPartwayThrough(AtomicBoolean killed) {
        try {
            Thread.sleep(Duration.ofSeconds(8).toMillis());
            List<String> pods = KubectlTestSupport.listPods(NAMESPACE, PROCESSOR_LABEL);
            if (pods.isEmpty()) {
                log.warn("AUTO-COMMIT LOSS TEST: no processor pods found to kill");
                return;
            }
            String victim = pods.get(new Random().nextInt(pods.size()));
            KubectlTestSupport.deletePod(NAMESPACE, victim);
            log.info("AUTO-COMMIT LOSS TEST: killed pod {} mid-batch", victim);
            killed.set(true);
        } catch (Exception e) {
            log.error("AUTO-COMMIT LOSS TEST: pod killer failed", e);
        }
    }
}
