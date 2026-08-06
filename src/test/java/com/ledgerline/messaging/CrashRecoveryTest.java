package com.ledgerline.messaging;

import java.io.File;
import java.math.BigDecimal;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.assertj.core.api.SoftAssertions;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.kafka.ConfluentKafkaContainer;

import com.ledgerline.generator.GeneratorConfig;
import com.ledgerline.generator.TransactionGenerator;

/**
 * What survives the consumer being killed outright.
 *
 * The consumer runs in a separate JVM so it can be SIGKILLed. That is the whole
 * point: an in-process container.stop() shuts down gracefully and commits its
 * offsets on the way out, which proves only that orderly shutdown works. A
 * process that dies without warning is what production failures look like, and
 * it is the only way to exercise the window between "record processed" and
 * "offset committed".
 *
 * Disabled unless -Dledgerline.crashtest=true, because it spawns a real JVM and
 * takes far longer than the rest of the suite. It is a deliberate exercise, not
 * something to pay for on every run.
 */
@SpringBootTest(properties = "ledgerline.consumer.enabled=false")
@EnabledIfSystemProperty(named = "ledgerline.crashtest", matches = "true")
class CrashRecoveryTest {

    private static final Logger log = LoggerFactory.getLogger(CrashRecoveryTest.class);

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
    private JdbcTemplate jdbc;

    private final List<Process> spawned = new ArrayList<>();

    @BeforeEach
    void setUp() {
        jdbc.update("DELETE FROM ledger_entries");
        jdbc.update("DELETE FROM transactions");
        jdbc.update("DELETE FROM transaction_states");
        jdbc.update("DELETE FROM parked_events");
        jdbc.update("DELETE FROM faultlab.injected_faults");
        jdbc.update("DELETE FROM faultlab.generator_runs");
    }

    @AfterEach
    void stopSpawnedConsumers() {
        spawned.forEach(Process::destroyForcibly);
        spawned.clear();
    }

    /**
     * Kill the consumer mid-batch, restart it, and check the ledger.
     *
     * The two failure modes this looks for are opposites, and a test that
     * checked only one would miss the other entirely:
     *
     *   LOST -- a record whose offset was committed before its write landed.
     *   The offset moves past it, nothing redelivers it, and the transfer
     *   simply never happened.
     *
     *   DUPLICATED -- a record written twice because it was redelivered after
     *   its write but before its commit. Correct behaviour is redelivery;
     *   correct behaviour is also that the second attempt writes nothing.
     */
    @Test
    void killedConsumerLosesNothingAndDuplicatesNothing() throws Exception {
        int transactionCount = 120;
        String runId = "crash-" + UUID.randomUUID();
        List<Long> accountIds = jdbc.queryForList("SELECT id FROM accounts ORDER BY id", Long.class);

        // Everything on the topic before any consumer starts, so the kill lands
        // in the middle of a real backlog rather than racing production.
        generator.generate(GeneratorConfig.clean(runId, 4242L, transactionCount, accountIds));

        long expectedPairs = transactionCount; // one capture per transaction
        log.info("CRASH TEST: published {} transactions ({} messages)",
                transactionCount, transactionCount * 3);

        Process first = startConsumerProcess();
        awaitSomeProgress();

        long entriesBeforeKill = entryCount();
        long offsetBeforeKill = committedOffsetTotal();
        log.info("CRASH TEST: killing consumer with {} entries written, offset total {}",
                entriesBeforeKill, offsetBeforeKill);

        // SIGKILL equivalent: no shutdown hook, no offset commit, no chance to
        // finish the record in flight.
        first.destroyForcibly();
        first.waitFor(30, TimeUnit.SECONDS);

        Process second = startConsumerProcess();
        awaitDrained(expectedPairs);

        long entriesAfter = entryCount();
        long offsetAfter = committedOffsetTotal();
        long distinctKeys = distinctIdempotencyKeys();
        long balancedPairs = balancedPairCount();

        log.info("CRASH TEST RESULT: entries before kill={}, after={}, "
                        + "offset before={}, after={}, distinct keys={}, balanced pairs={}",
                entriesBeforeKill, entriesAfter, offsetBeforeKill, offsetAfter,
                distinctKeys, balancedPairs);

        second.destroyForcibly();

        SoftAssertions softly = new SoftAssertions();

        // Nothing lost: every transaction produced its pair.
        softly.assertThat(balancedPairs)
                .as("balanced pairs written (lost work if below %d)", expectedPairs)
                .isEqualTo(expectedPairs);

        // Every event processed, not merely every capture. A lost authorize or
        // settle leaves the pair count intact while still being data loss, so
        // counting pairs alone cannot see it -- one claimed key per published
        // message is the assertion that can.
        softly.assertThat(distinctKeys)
                .as("claimed keys, one per published event (lost events if below %d)",
                        transactionCount * 3)
                .isEqualTo(transactionCount * 3L);

        // Nothing duplicated: pairs and claimed keys agree.
        softly.assertThat(balancedPairs)
                .as("pairs must equal the captures that claimed a key")
                .isEqualTo(captureKeyCount());

        // The invariant still holds across everything written.
        softly.assertThat(systemBalance())
                .as("system-wide sum")
                .isEqualByComparingTo(BigDecimal.ZERO);

        softly.assertThat(unbalancedTransactionCount())
                .as("transactions whose entries do not sum to zero")
                .isZero();

        softly.assertAll();
    }

    /**
     * Starts the application as its own OS process, consumer enabled.
     *
     * A separate JVM is what makes destroyForcibly a real crash. Killing a
     * thread inside this JVM would still run shutdown hooks and could still
     * flush a commit, which is precisely the behaviour that must not happen
     * here.
     */
    private Process startConsumerProcess() throws Exception {
        Path jar = Path.of("target", "ledgerline-0.0.1-SNAPSHOT.jar");
        if (!Files.exists(jar)) {
            throw new IllegalStateException(
                    "run `mvnw package -DskipTests` first: " + jar.toAbsolutePath());
        }

        List<String> command = List.of(
                Path.of(System.getProperty("java.home"), "bin", "java").toString(),
                "-jar", jar.toString(),
                "--spring.main.web-application-type=none",
                "--spring.kafka.bootstrap-servers=" + KAFKA.getBootstrapServers(),
                "--spring.datasource.url=" + POSTGRES.getJdbcUrl(),
                "--spring.datasource.username=" + POSTGRES.getUsername(),
                "--spring.datasource.password=" + POSTGRES.getPassword(),
                "--ledgerline.consumer.enabled=true");

        Process process = new ProcessBuilder(command)
                .redirectErrorStream(true)
                .redirectOutput(new File(System.getProperty("java.io.tmpdir"),
                        "ledgerline-consumer-" + UUID.randomUUID() + ".log"))
                .start();

        spawned.add(process);
        return process;
    }

    /** Waits until the consumer has written something, so the kill is mid-batch. */
    private void awaitSomeProgress() throws Exception {
        long deadline = System.currentTimeMillis() + Duration.ofSeconds(90).toMillis();
        while (System.currentTimeMillis() < deadline) {
            if (entryCount() > 0) {
                // A little further in, so the kill is genuinely mid-stream
                // rather than on the very first record.
                Thread.sleep(400);
                return;
            }
            Thread.sleep(100);
        }
        throw new IllegalStateException("consumer never started writing");
    }

    private void awaitDrained(long expectedPairs) throws Exception {
        long deadline = System.currentTimeMillis() + Duration.ofSeconds(180).toMillis();
        while (System.currentTimeMillis() < deadline) {
            if (balancedPairCount() >= expectedPairs) {
                // Settle briefly, so a late duplicate would still be counted.
                Thread.sleep(2_000);
                return;
            }
            Thread.sleep(500);
        }
        log.warn("CRASH TEST: drained only {} of {} pairs before timeout",
                balancedPairCount(), expectedPairs);
    }

    private long committedOffsetTotal() throws Exception {
        try (AdminClient admin = AdminClient.create(Map.of(
                AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA.getBootstrapServers()))) {
            Map<TopicPartition, OffsetAndMetadata> committed = admin
                    .listConsumerGroupOffsets(KafkaConsumerConfig.GROUP_ID)
                    .partitionsToOffsetAndMetadata()
                    .get(30, TimeUnit.SECONDS);

            return committed.entrySet().stream()
                    .filter(entry -> entry.getKey().topic().equals(TRANSACTIONS_TOPIC))
                    .mapToLong(entry -> entry.getValue().offset())
                    .sum();
        }
    }

    private long entryCount() {
        return jdbc.queryForObject("SELECT count(*) FROM ledger_entries", Long.class);
    }

    /** Transactions holding a balanced pair of entries. */
    private long balancedPairCount() {
        return jdbc.queryForObject(
                "SELECT count(*) FROM ("
                        + "  SELECT transaction_id FROM ledger_entries "
                        + "  GROUP BY transaction_id "
                        + "  HAVING count(*) = 2 AND SUM(amount) = 0"
                        + ") balanced", Long.class);
    }

    private long distinctIdempotencyKeys() {
        return jdbc.queryForObject(
                "SELECT count(DISTINCT idempotency_key) FROM transactions", Long.class);
    }

    /** Claimed keys belonging to captures, which are what write entries. */
    private long captureKeyCount() {
        return jdbc.queryForObject(
                "SELECT count(*) FROM transactions WHERE idempotency_key LIKE '%:CAPTURE'",
                Long.class);
    }

    private BigDecimal systemBalance() {
        return jdbc.queryForObject(
                "SELECT COALESCE(SUM(amount), 0) FROM ledger_entries", BigDecimal.class);
    }

    private long unbalancedTransactionCount() {
        return jdbc.queryForObject(
                "SELECT count(*) FROM ("
                        + "  SELECT transaction_id FROM ledger_entries "
                        + "  GROUP BY transaction_id HAVING SUM(amount) <> 0"
                        + ") unbalanced", Long.class);
    }
}
