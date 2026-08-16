package com.ledgerline.messaging;

import java.math.BigDecimal;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
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

/**
 * Investigation-only test (see docs/known-limitations.md, "torn ledger
 * write" entry): a dedicated, repeatable attempt to reproduce the exact
 * single-entry torn write found during Day 10's leader-kill testing
 * (transaction 28038, one entry instead of two, discovered via
 * LeaderKillDurabilityTest but not caused by anything in that test's own
 * assertions).
 *
 * Deliberately mirrors the ORIGINAL timing that produced the incident
 * (100 tx/s, kill 8s in) rather than the retuned 500ms/300tx/s version
 * LeaderKillDurabilityTest now uses -- the incident write landed during the
 * slow multi-minute catch-up tail that timing produced, not near the kill
 * itself, so reproducing that same slow-tail shape matters more than
 * reproducing the kill's exact millisecond offset.
 *
 * Runs a continuous scanner throughout generation and the settle window,
 * not just an end-of-run check -- if a torn write appears, this records
 * exactly when, closing the "was this near the kill, or during ordinary
 * catch-up traffic" question with real timestamps instead of inference.
 */
@SpringBootTest
@EnabledIfSystemProperty(named = "ledgerline.chaostest", matches = "true")
class TornWriteReproductionTest {

    private static final Logger log = LoggerFactory.getLogger(TornWriteReproductionTest.class);

    private static final String NAMESPACE = System.getProperty("ledgerline.chaostest.namespace", "ledgerline");
    private static final String RELEASE = System.getProperty("ledgerline.chaostest.release", "ledgerline");

    private static final int KAFKA_EXTERNAL_PORT = 9094;
    private static final int POSTGRES_LOCAL_PORT = 15436;

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

    @Test
    void attemptToReproduceTheTornWrite() throws Exception {
        int transactionCount = 3000;
        int ratePerSecond = 100; // the ORIGINAL rate, matching the incident run
        String runId = "tornrepro-" + Instant.now().toEpochMilli();

        List<Long> accountIds = jdbc.queryForList("SELECT id FROM accounts ORDER BY id", Long.class);
        GeneratorConfig config = new GeneratorConfig(runId, 9001L, transactionCount, ratePerSecond,
                new java.util.EnumMap<>(com.ledgerline.generator.FaultType.class), accountIds);

        String leaderPod = currentPartitionZeroLeaderPodName();
        log.info("TORN WRITE REPRO: partition 0's current leader is pod {}", leaderPod);

        AtomicBoolean stopScanner = new AtomicBoolean(false);
        Thread scanner = new Thread(() -> scanForTornWrites(runId, stopScanner), "torn-write-scanner");
        scanner.start();

        Thread killerThread = new Thread(() -> killLeaderAfterDelay(leaderPod), "torn-repro-killer");
        killerThread.start();

        log.info("TORN WRITE REPRO: generating {} transactions at {} tx/s (runId={}, mirrors the ORIGINAL "
                        + "leader-kill timing that produced the incident, not the retuned fast-kill version)",
                transactionCount, ratePerSecond, runId);
        generator.generate(config);
        killerThread.join(Duration.ofSeconds(30).toMillis());

        // The incident write landed nearly 3 minutes into a run whose
        // generation phase alone took ~30s -- so the scanner needs to keep
        // watching well past generation, through the same kind of long
        // catch-up tail, not just through the kill and a short settle.
        log.info("TORN WRITE REPRO: generation complete, scanning through the catch-up tail for 4 more minutes");
        Thread.sleep(Duration.ofMinutes(4).toMillis());

        stopScanner.set(true);
        scanner.join(Duration.ofSeconds(10).toMillis());

        List<String> finalCheck = jdbc.query(
                "SELECT e.transaction_id || ':' || count(*) || ':' || SUM(e.amount) FROM ledger_entries e "
                        + "JOIN transactions t ON t.id = e.transaction_id "
                        + "WHERE t.idempotency_key LIKE ? "
                        + "GROUP BY e.transaction_id "
                        + "HAVING count(*) <> 2 OR SUM(e.amount) <> 0",
                (rs, rowNum) -> rs.getString(1), runId + "-%");

        if (finalCheck.isEmpty()) {
            log.info("TORN WRITE REPRO RESULT: NOT REPRODUCED -- every transaction in this run "
                    + "({} runId={}) has exactly 2 balanced entries", transactionCount, runId);
        } else {
            log.warn("TORN WRITE REPRO RESULT: REPRODUCED -- {} torn/unbalanced transaction(s) found: {}",
                    finalCheck.size(), finalCheck);
        }
    }

    private void scanForTornWrites(String runId, AtomicBoolean stop) {
        while (!stop.get()) {
            try {
                List<String> found = jdbc.query(
                        "SELECT e.transaction_id || ':' || count(*) || ':' || SUM(e.amount) || ':' || now() "
                                + "FROM ledger_entries e "
                                + "JOIN transactions t ON t.id = e.transaction_id "
                                + "WHERE t.idempotency_key LIKE ? "
                                + "GROUP BY e.transaction_id "
                                + "HAVING count(*) <> 2 OR SUM(e.amount) <> 0",
                        (rs, rowNum) -> rs.getString(1), runId + "-%");
                for (String hit : found) {
                    // A hit here can still be transient (a debit committed,
                    // credit not yet committed, half a millisecond apart --
                    // the same benign window Day 9's settle-recheck exists
                    // for). Reconfirm after a short pause before treating it
                    // as a genuine torn write worth logging loudly.
                    Thread.sleep(300);
                    long txnId = Long.parseLong(hit.split(":")[0]);
                    Integer stillTorn = jdbc.queryForObject(
                            "SELECT count(*) FROM ledger_entries WHERE transaction_id = ?",
                            Integer.class, txnId);
                    if (stillTorn != null && stillTorn != 2) {
                        log.warn("TORN WRITE REPRO SCANNER: confirmed torn write at {} -- transaction_id={} "
                                + "raw_hit={}", Instant.now(), txnId, hit);
                    }
                }
            } catch (Exception e) {
                log.debug("TORN WRITE REPRO SCANNER: scan failed transiently: {}", e.toString());
            }
            try {
                Thread.sleep(500);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return;
            }
        }
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
                        return RELEASE + "-kafka-" + (nodeId - 1);
                    }
                }
            }
        }
        throw new IllegalStateException("could not determine partition 0's leader from: " + describe);
    }

    private void killLeaderAfterDelay(String leaderPod) {
        try {
            Thread.sleep(Duration.ofSeconds(8).toMillis()); // the ORIGINAL delay, matching the incident run
            KubectlTestSupport.deletePod(NAMESPACE, leaderPod);
            log.info("TORN WRITE REPRO: killed leader pod {} at +8s, mirroring the incident run's timing", leaderPod);
        } catch (Exception e) {
            log.error("TORN WRITE REPRO: leader killer failed", e);
        }
    }
}
