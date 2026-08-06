package com.ledgerline.generator;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Duration;
import java.util.ArrayList;
import java.util.EnumMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.assertj.core.api.SoftAssertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.kafka.ConfluentKafkaContainer;

/**
 * The generator's output and its ground truth.
 *
 * The consumer is disabled here: what is under test is what the generator
 * publishes and records, not what the ledger does with it. Letting the consumer
 * run would mix the two and make a generator failure look like a ledger
 * failure.
 */
@SpringBootTest(properties = "ledgerline.consumer.enabled=false")
class TransactionGeneratorTest {

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

    private List<Long> accountIds;

    @BeforeEach
    void setUp() {
        jdbc.update("DELETE FROM faultlab.injected_faults");
        jdbc.update("DELETE FROM faultlab.generator_runs");

        accountIds = jdbc.queryForList("SELECT id FROM accounts ORDER BY id", Long.class);
    }

    @Test
    void producesTheRequestedNumberOfTransactions() throws Exception {
        GeneratorConfig config = GeneratorConfig.clean(runId(), 42L, 25, accountIds);

        var result = generator.generate(config);

        assertThat(result.transactionIds()).hasSize(25);
        assertThat(result.transactionIds()).doesNotHaveDuplicates();
        // A clean run is authorize, capture, settle for every transaction.
        assertThat(result.publishedMessages()).isEqualTo(75);
        assertThat(result.injectedFaults()).isZero();
    }

    @Test
    void cleanRunRecordsNoFaults() throws Exception {
        generator.generate(GeneratorConfig.clean(runId(), 7L, 10, accountIds));

        assertThat(faultCount()).isZero();
    }

    /**
     * Every fault type appears when its rate is turned on.
     *
     * Rates are 1.0 so each type is certain to fire, which is what makes this
     * an assertion about the generator rather than about the RNG.
     */
    @Test
    void eachFaultTypeAppearsWhenItsRateIsNonZero() throws Exception {
        for (FaultType type : FaultType.values()) {
            String runId = runId();
            Map<FaultType, Double> rates = new EnumMap<>(FaultType.class);
            rates.put(type, 1.0);

            generator.generate(new GeneratorConfig(runId, 99L, 5, 0, rates, accountIds));

            assertThat(faultCountOf(runId, type))
                    .as("%s should appear when its rate is 1.0", type)
                    .isPositive();
        }
    }

    /**
     * Every injected fault has exactly one ground-truth row.
     *
     * The count the generator reports and the count in the table must agree.
     * A fault injected without a row becomes, at grading time, a defect the
     * harness scores as a false positive when a detector correctly finds it --
     * which would make the accuracy number wrong in the most misleading
     * direction.
     */
    @Test
    void everyInjectedFaultHasExactlyOneGroundTruthRow() throws Exception {
        String runId = runId();
        Map<FaultType, Double> rates = new EnumMap<>(FaultType.class);
        rates.put(FaultType.DUPLICATE_PUBLISH, 0.4);
        rates.put(FaultType.OUT_OF_ORDER, 0.3);
        rates.put(FaultType.ORPHAN_CAPTURE, 0.2);
        rates.put(FaultType.AMOUNT_DRIFT, 0.3);
        rates.put(FaultType.MISSING_SETTLEMENT, 0.2);

        var result = generator.generate(new GeneratorConfig(runId, 12345L, 60, 0, rates, accountIds));

        assertThat(result.injectedFaults())
                .as("a run with these rates should inject something")
                .isPositive();
        assertThat(faultCountOf(runId))
                .as("ground truth rows must match the faults the generator reports")
                .isEqualTo(result.injectedFaults());
    }

    /** The run's seed and parameters are recorded, so it can be replayed. */
    @Test
    void runIsRecordedWithItsSeed() throws Exception {
        String runId = runId();
        generator.generate(GeneratorConfig.clean(runId, 5150L, 5, accountIds));

        Long seed = jdbc.queryForObject(
                "SELECT seed FROM faultlab.generator_runs WHERE run_id = ?", Long.class, runId);
        assertThat(seed).isEqualTo(5150L);

        assertThat(jdbc.queryForObject(
                "SELECT finished_at IS NOT NULL FROM faultlab.generator_runs WHERE run_id = ?",
                Boolean.class, runId)).isTrue();
    }

    /**
     * The same seed produces an identical stream.
     *
     * Compared on the actual published payloads, not on the generator's summary
     * counts: two runs could agree on how many messages they sent while
     * disagreeing on every amount in them. Without this property, an accuracy
     * figure measured in Week 3 cannot be reproduced or compared against a
     * later one, which would make the whole measurement exercise pointless.
     */
    @Test
    void sameSeedProducesAnIdenticalStream() throws Exception {
        Map<FaultType, Double> rates = new EnumMap<>(FaultType.class);
        rates.put(FaultType.DUPLICATE_PUBLISH, 0.3);
        rates.put(FaultType.AMOUNT_DRIFT, 0.3);
        rates.put(FaultType.OUT_OF_ORDER, 0.2);

        generator.generate(new GeneratorConfig("run-a", 777L, 20, 0, rates, accountIds));
        generator.generate(new GeneratorConfig("run-b", 777L, 20, 0, rates, accountIds));

        // The run id is part of every transaction id, so strip it before
        // comparing -- what must match is the shape and the numbers, not the
        // label identifying which run produced them.
        List<String> first = payloadsOf("run-a");
        List<String> second = payloadsOf("run-b");

        SoftAssertions softly = new SoftAssertions();
        softly.assertThat(first).as("run-a produced nothing").isNotEmpty();
        softly.assertThat(second)
                .as("same seed must produce the same messages")
                .isEqualTo(first);
        softly.assertAll();
    }

    /** A different seed produces a different stream, or the seed does nothing. */
    @Test
    void differentSeedProducesADifferentStream() throws Exception {
        Map<FaultType, Double> rates = new EnumMap<>(FaultType.class);
        rates.put(FaultType.AMOUNT_DRIFT, 0.5);

        generator.generate(new GeneratorConfig("seed-x", 1L, 20, 0, rates, accountIds));
        generator.generate(new GeneratorConfig("seed-y", 2L, 20, 0, rates, accountIds));

        assertThat(payloadsOf("seed-y"))
                .as("a different seed that produced an identical stream means the seed is ignored")
                .isNotEqualTo(payloadsOf("seed-x"));
    }

    /** Money is a JSON string on the wire, never a number. */
    @Test
    void amountsArePublishedAsStrings() throws Exception {
        String runId = runId();
        generator.generate(GeneratorConfig.clean(runId, 31L, 5, accountIds));

        List<String> payloads = payloadsOf(runId);

        assertThat(payloads).isNotEmpty();
        assertThat(payloads).allSatisfy(payload ->
                assertThat(payload)
                        .as("amount must be quoted, payload was %s", payload)
                        .containsPattern("\"amount\":\"[0-9.]+\""));
    }

    private long faultCount() {
        return jdbc.queryForObject("SELECT count(*) FROM faultlab.injected_faults", Long.class);
    }

    private long faultCountOf(String runId) {
        return jdbc.queryForObject(
                "SELECT count(*) FROM faultlab.injected_faults WHERE run_id = ?", Long.class, runId);
    }

    private long faultCountOf(String runId, FaultType type) {
        return jdbc.queryForObject(
                "SELECT count(*) FROM faultlab.injected_faults WHERE run_id = ? AND fault_type = ?",
                Long.class, runId, type.name());
    }

    /**
     * Every payload this run published, identified by its run id rather than
     * by offset arithmetic.
     *
     * Filtering on the run id is what makes this reliable: the topic has three
     * partitions, so arrival order across it is not publication order, and
     * "skip the first N records" picks a different subset every time. Every
     * transaction id carries its run id, so matching on that isolates one run
     * exactly however the partitions interleave.
     *
     * The run id is then stripped, so two runs with the same seed compare
     * equal on content -- what must match is the accounts, amounts, and event
     * sequence, not the label saying which run produced them. Sorted for the
     * same reason: what is under test is the set of messages, not the order
     * three partitions happened to deliver them in.
     */
    private List<String> payloadsOf(String runId) {
        Map<String, Object> config = new java.util.HashMap<>();
        config.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA.getBootstrapServers());
        config.put(ConsumerConfig.GROUP_ID_CONFIG, "gen-probe-" + UUID.randomUUID());
        config.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        config.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        config.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        config.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);

        List<String> matching = new ArrayList<>();
        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(config)) {
            consumer.subscribe(List.of(TRANSACTIONS_TOPIC));

            long deadline = System.currentTimeMillis() + Duration.ofSeconds(20).toMillis();
            int emptyPolls = 0;
            while (System.currentTimeMillis() < deadline && emptyPolls < 3) {
                ConsumerRecords<String, String> polled = consumer.poll(Duration.ofMillis(500));
                if (polled.isEmpty()) {
                    emptyPolls++;
                    continue;
                }
                emptyPolls = 0;
                for (ConsumerRecord<String, String> record : polled) {
                    if (record.value().contains("\"" + runId + "-txn-")) {
                        matching.add(record.value().replace(runId, "RUN"));
                    }
                }
            }
        }

        return matching.stream().sorted().toList();
    }

    private static String runId() {
        return "run-" + UUID.randomUUID();
    }
}
