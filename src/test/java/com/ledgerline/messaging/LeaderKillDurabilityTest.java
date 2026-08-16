package com.ledgerline.messaging;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

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
 * Day 10: kills the partition leader broker under sustained load and
 * measures produced-vs-persisted counts, run once with {@code acks=all}
 * and once with {@code acks=1} against the real 3-broker cluster.
 *
 * <h2>Why this needs a real Helm-configured producer, not a test-local one</h2>
 *
 * {@code ledgerline.producer.acks} (see {@code KafkaProducerConfig}'s
 * comment on it) exists so this test can measure the real Deployment's real
 * producer against a real killed leader, under each acks setting in turn --
 * not a simulated approximation. This class does not perform the {@code
 * helm upgrade} between runs itself (see {@code docs/day10-multi-broker.md}
 * for the exact commands, the same pattern as Day 9's auto-commit-loss
 * measurement) -- it assumes the caller has already applied the desired
 * {@code processor.producer.acks} value and confirmed it live before
 * running {@link #killLeaderUnderLoadAndMeasureLoss()}, and reads which
 * setting was actually live from the running pods' own producer-config log
 * line, recording it alongside the result rather than trusting the Helm
 * value.
 *
 * <h2>What "persisted" means here</h2>
 *
 * Publish success (what the producer's {@code Future} resolves to) is not
 * the question -- acks=1 can report success and still lose the record if
 * the leader dies before replication. What this test actually counts is
 * payments that make it all the way through: a balanced ledger pair
 * actually written by a consumer, which can only happen for a message the
 * broker durably kept. That is the "persisted" count compared against how
 * many payments were attempted.
 */
@SpringBootTest
@EnabledIfSystemProperty(named = "ledgerline.chaostest", matches = "true")
class LeaderKillDurabilityTest {

    private static final Logger log = LoggerFactory.getLogger(LeaderKillDurabilityTest.class);

    private static final String NAMESPACE = System.getProperty("ledgerline.chaostest.namespace", "ledgerline");
    private static final String RELEASE = System.getProperty("ledgerline.chaostest.release", "ledgerline");

    private static final int KAFKA_EXTERNAL_PORT = 9094; // broker 0's EXTERNAL listener is enough to bootstrap
    private static final int POSTGRES_LOCAL_PORT = 15434; // distinct from the other chaos tests' forwards

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
     * Generates sustained load, kills whichever broker pod is the current
     * partition leader for partition 0 partway through, and reports
     * produced-vs-persisted counts. Not asserted pass/fail against zero
     * loss -- the point of the acks=1 run is to observe and record real
     * loss, and the point of the acks=all run is to observe and confirm
     * there is none. What IS asserted is the measurement's own integrity
     * (every message actually reached a broker, and the leader kill
     * actually happened).
     */
    @Test
    void killLeaderUnderLoadAndMeasureLoss() throws Exception {
        // Higher rate and a near-immediate kill than the original values
        // (100 tx/s, kill at 8s): on a fast local network, followers catch
        // up on replication quickly enough that a kill 8s into a 100 tx/s
        // run reliably finds nothing still unreplicated -- confirmed by a
        // real acks=1 run at those settings showing 0 lost payments, same
        // as acks=all. That is a legitimate result (acks=1 offers no
        // guarantee, but "no guarantee" is not the same as "always loses
        // data"), but it does not exercise the actual race the comparison
        // is meant to demonstrate. Killing sooner, before replication has
        // had time to catch up on the initial burst, is what actually
        // creates the in-flight window acks=1 can lose and acks=all cannot.
        int transactionCount = 3000;
        int ratePerSecond = 300;
        String runId = "leaderkill-" + Instant.now().toEpochMilli();

        List<Long> accountIds = jdbc.queryForList("SELECT id FROM accounts ORDER BY id", Long.class);
        GeneratorConfig config = new GeneratorConfig(runId, 9001L, transactionCount, ratePerSecond,
                new java.util.EnumMap<>(com.ledgerline.generator.FaultType.class), accountIds);

        String leaderPod = currentPartitionZeroLeaderPodName();
        log.info("LEADER KILL TEST: partition 0's current leader is pod {}", leaderPod);

        AtomicBoolean killed = new AtomicBoolean(false);
        AtomicReference<Instant> killedAt = new AtomicReference<>();
        Thread killerThread = new Thread(
                () -> killLeaderPartwayThrough(leaderPod, killed, killedAt), "leader-kill-killer");
        killerThread.start();

        log.info("LEADER KILL TEST: generating {} transactions at {} tx/s (runId={})",
                transactionCount, ratePerSecond, runId);
        GeneratorResult result = generator.generate(config);
        killerThread.join(Duration.ofSeconds(30).toMillis());

        // Give the new leader election, ISR catch-up, and consumer rebalance
        // time to fully settle before counting. A first run at 20s produced
        // an apparent (but false) loss under acks=all -- the topic's own
        // consumer group still had lag at that point, not because anything
        // was lost but because leader failover plus a 3-way consumer
        // rebalance takes measurably longer to fully settle than Day 9's
        // simple pod-kill-and-recreate did. Waiting for the group's own lag
        // to hit zero is the actual settle signal; the fixed sleep is just
        // an upper bound in case something is genuinely stuck.
        awaitConsumerGroupCaughtUp(Duration.ofSeconds(60));

        long expectedPairs = result.messages().stream()
                .filter(m -> m.eventType() == com.ledgerline.domain.EventType.CAPTURE)
                .count();

        // Zero consumer lag means the group has applied everything it has
        // received so far -- it does not mean every producer send has
        // finished landing yet. Leader failover under this test's load was
        // observed taking noticeably longer than Day 9's simple
        // pod-kill-and-recreate to fully settle (a real acks=all run's
        // count was still 268 short at the 20s mark, but reached 2999/3000
        // when rechecked minutes later) -- so this waits for the count to
        // stop *changing* across consecutive polls, not just for it to hit
        // the expected total or for a fixed budget to expire. A loss that
        // never converges (the acks=1 case) still exits once the number
        // stops moving; a loss that's still catching up keeps polling.
        long actualPairs = awaitStableBalancedPairCount(runId, Duration.ofMinutes(3));

        long lostPayments = expectedPairs - actualPairs;
        java.math.BigDecimal invariantDelta = jdbc.queryForObject(
                "SELECT COALESCE(SUM(amount), 0) FROM ledger_entries", java.math.BigDecimal.class);

        log.info("LEADER KILL TEST RESULT: leaderPodKilled={} killedAt={} publishFailures={} "
                        + "expectedPairs={} actualPairs={} LOST PAYMENTS={} invariantDelta={} (runId={})",
                leaderPod, killedAt.get(), result.publishFailures(), expectedPairs, actualPairs,
                lostPayments, invariantDelta, runId);

        assertThat(killed.get())
                .as("the leader pod-kill must actually have happened for this to be a real measurement")
                .isTrue();
        assertThat(actualPairs)
                .as("balanced pairs written must never exceed what was published -- "
                        + "more would mean duplicate writes, a separate defect from loss")
                .isLessThanOrEqualTo(expectedPairs);
        // The core Day 10 claim, true under EITHER acks setting: idempotency
        // guards against a message applied twice, not against one that never
        // arrived. A lost payment never gets a balanced pair written for it
        // at all -- there is no partial/torn entry sitting in the table --
        // so the ledger stays exactly balanced even while silently missing
        // up to `lostPayments` transactions.
        assertThat(invariantDelta)
                .as("the ledger invariant must hold regardless of acks setting or loss count -- "
                        + "durability (whether a message survives) is independent of idempotency "
                        + "(whether a message that DID arrive is applied exactly once)")
                .isEqualByComparingTo(java.math.BigDecimal.ZERO);
    }

    /**
     * Polls {@code kafka-consumer-groups --describe} until every partition's
     * LAG column reads 0, or the timeout elapses. Parses the CLI output
     * rather than trusting a fixed sleep -- exactly the lesson Day 9's
     * settle-recheck fix already taught (see docs/day9-chaos-test.md): a
     * fixed wait that happens to be long enough once is not the same as
     * actually observing the condition you care about.
     */
    private void awaitConsumerGroupCaughtUp(Duration timeout) throws Exception {
        long deadline = System.currentTimeMillis() + timeout.toMillis();
        while (System.currentTimeMillis() < deadline) {
            String describe = KubectlTestSupport.runKubectl("exec", "-n", NAMESPACE, RELEASE + "-kafka-0", "--",
                    "kafka-consumer-groups", "--bootstrap-server", "localhost:29092",
                    "--describe", "--group", KafkaConsumerConfig.GROUP_ID);
            boolean allCaughtUp = true;
            boolean sawAnyPartitionRow = false;
            for (String line : describe.split("\n")) {
                String trimmed = line.trim();
                if (trimmed.isEmpty() || trimmed.startsWith("GROUP") || trimmed.startsWith("Warning")) {
                    continue;
                }
                String[] fields = trimmed.split("\\s+");
                // GROUP TOPIC PARTITION CURRENT-OFFSET LOG-END-OFFSET LAG ...
                if (fields.length < 6) {
                    continue;
                }
                sawAnyPartitionRow = true;
                if (!"0".equals(fields[5])) {
                    allCaughtUp = false;
                    break;
                }
            }
            if (sawAnyPartitionRow && allCaughtUp) {
                return;
            }
            Thread.sleep(2000);
        }
        log.warn("LEADER KILL TEST: consumer group did not report zero lag within {}, "
                + "proceeding to count anyway", timeout);
    }

    /**
     * Polls {@link #countBalancedPairs} until it reports the same value on
     * 3 consecutive polls 3s apart (9s of genuine quiet, not just one lucky
     * reading), or the timeout elapses -- whichever comes first. Returns
     * whatever the last reading was either way, so a genuine, real loss
     * (acks=1) still returns a real, stable, non-zero-loss number once
     * writes have actually stopped arriving, rather than looping until a
     * timeout that was tuned for the wrong case.
     */
    private long awaitStableBalancedPairCount(String runId, Duration timeout) throws InterruptedException {
        long deadline = System.currentTimeMillis() + timeout.toMillis();
        long last = countBalancedPairs(runId);
        int stableStreak = 1;
        while (stableStreak < 3 && System.currentTimeMillis() < deadline) {
            Thread.sleep(3000);
            long current = countBalancedPairs(runId);
            if (current == last) {
                stableStreak++;
            } else {
                stableStreak = 1;
                last = current;
            }
        }
        return last;
    }

    private long countBalancedPairs(String runId) {
        return jdbc.queryForObject(
                "SELECT count(*) FROM ("
                        + "  SELECT e.transaction_id FROM ledger_entries e "
                        + "  JOIN transactions t ON t.id = e.transaction_id "
                        + "  WHERE t.idempotency_key LIKE ? "
                        + "  GROUP BY e.transaction_id "
                        + "  HAVING count(*) = 2 AND SUM(e.amount) = 0"
                        + ") balanced", Long.class, runId + "-%");
    }

    private String currentPartitionZeroLeaderPodName() throws Exception {
        String describe = KubectlTestSupport.runKubectl("exec", "-n", NAMESPACE, RELEASE + "-kafka-0", "--",
                "kafka-topics", "--bootstrap-server", "localhost:29092", "--describe", "--topic", "transactions");
        for (String line : describe.split("\n")) {
            if (line.contains("Partition: 0")) {
                String[] fields = line.trim().split("\\s+");
                for (int i = 0; i < fields.length; i++) {
                    if (fields[i].equals("Leader:")) {
                        int nodeId = Integer.parseInt(fields[i + 1]);
                        // StatefulSet ordinals are 0-indexed, node IDs are
                        // ordinal+1 (see the wrapper script in
                        // helm/ledgerline/templates/kafka/statefulset.yaml).
                        return RELEASE + "-kafka-" + (nodeId - 1);
                    }
                }
            }
        }
        throw new IllegalStateException("could not determine partition 0's leader from: " + describe);
    }

    private void killLeaderPartwayThrough(String leaderPod, AtomicBoolean killed, AtomicReference<Instant> killedAt) {
        try {
            // Short delay, not 8s: the whole point is to catch records that
            // are still in the leader's local log but not yet replicated to
            // either follower. On a fast local network followers usually
            // catch up within a couple hundred milliseconds of a record
            // landing, so waiting even a few seconds lets replication fully
            // drain the backlog before the kill, which is why an earlier
            // acks=1 run at an 8s delay showed zero loss -- a legitimate
            // result, but not one that exercises the race this test exists
            // to demonstrate.
            Thread.sleep(Duration.ofMillis(500));
            KubectlTestSupport.deletePod(NAMESPACE, leaderPod);
            killedAt.set(Instant.now());
            log.info("LEADER KILL TEST: killed leader pod {} mid-batch", leaderPod);
            killed.set(true);
        } catch (Exception e) {
            log.error("LEADER KILL TEST: leader killer failed", e);
        }
    }

}
