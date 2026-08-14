package com.ledgerline.messaging;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import java.math.BigDecimal;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.ListOffsetsResult;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.admin.OffsetSpec;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.MessageListenerContainer;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.kafka.ConfluentKafkaContainer;

import com.ledgerline.transfer.TransactionEventService;

/**
 * Consumer tests against a real broker and a real Postgres.
 *
 * Both containers are real because what is under test is the interaction
 * between them: whether an offset is committed only after a row is written, and
 * what happens to the partition when it is not.
 */
@SpringBootTest
class TransactionConsumerTest {

    /** Matches docker-compose. */
    static final int PARTITIONS = 3;

    private static final ConfluentKafkaContainer KAFKA =
            new ConfluentKafkaContainer("confluentinc/cp-kafka:7.8.0");
    private static final PostgreSQLContainer<?> POSTGRES =
            new PostgreSQLContainer<>("postgres:16");

    static {
        KAFKA.start();
        POSTGRES.start();
        // Topics must exist before the Spring context starts, or the listener
        // subscribes to a topic that is not there and sits idle.
        createTopics();
    }

    private static void createTopics() {
        Map<String, Object> config = Map.of(
                AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA.getBootstrapServers());

        try (AdminClient admin = AdminClient.create(config)) {
            admin.createTopics(List.of(
                    new NewTopic(TransactionProducer.TOPIC, PARTITIONS, (short) 1),
                    new NewTopic(DeadLetterPublisher.DLT_TOPIC, 1, (short) 1)))
                    .all().get(30, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException(e);
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
    private KafkaTemplate<String, String> deadLetterKafkaTemplate;

    @Autowired
    private JdbcTemplate jdbc;

    @Autowired
    private KafkaListenerEndpointRegistry listenerRegistry;

    @Autowired
    @SuppressWarnings("unused") // asserts the consumer did not take over its job
    private TransactionEventService eventService;

    @Autowired
    private io.micrometer.core.instrument.MeterRegistry meterRegistry;

    private long alice;
    private long bob;

    /** DLT offset at the start of the current test. */
    private long deadLetterReadFrom;

    @BeforeEach
    void setUp() {
        jdbc.update("DELETE FROM ledger_entries");
        jdbc.update("DELETE FROM transactions");
        // Lifecycle state too, or a transaction id reused across tests would
        // start from whatever state an earlier test left it in.
        jdbc.update("DELETE FROM transaction_states");

        // The DLT is read from the beginning in each test, so dead letters from
        // earlier tests would otherwise be counted again here.
        discardExistingDeadLetters();

        alice = accountId("Alice Checking");
        bob = accountId("Bob Checking");
    }

    /** Records the current end of the DLT, so only new dead letters are read. */
    private void discardExistingDeadLetters() {
        Map<String, Object> config = Map.of(
                AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA.getBootstrapServers());

        try (AdminClient admin = AdminClient.create(config)) {
            TopicPartition partition = new TopicPartition(DeadLetterPublisher.DLT_TOPIC, 0);
            ListOffsetsResult.ListOffsetsResultInfo end = admin
                    .listOffsets(Map.of(partition, OffsetSpec.latest()))
                    .partitionResult(partition)
                    .get(30, TimeUnit.SECONDS);
            deadLetterReadFrom = end.offset();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException(e);
        } catch (Exception e) {
            throw new IllegalStateException("could not read DLT end offset", e);
        }
    }

    @Test
    void validMessageIsWrittenToTheLedger() {
        String transactionId = UUID.randomUUID().toString();

        publish(transactionId, message(transactionId, alice, bob, "50.00", "USD"));

        await().atMost(Duration.ofSeconds(30)).until(() -> entryCount() == 2);

        assertThat(balanceOf(alice)).isEqualByComparingTo(new BigDecimal("-50"));
        assertThat(balanceOf(bob)).isEqualByComparingTo(new BigDecimal("50"));

        // The offset advanced past the record, so it will not be redelivered.
        await().atMost(Duration.ofSeconds(30))
                .until(() -> committedOffset(TransactionProducer.TOPIC) >= 1);
    }

    @Test
    void duplicateMessageIsWrittenOnce() {
        String transactionId = UUID.randomUUID().toString();
        String payload = message(transactionId, alice, bob, "50.00", "USD");

        publish(transactionId, payload);
        await().atMost(Duration.ofSeconds(30)).until(() -> entryCount() == 2);

        // Byte-identical redelivery of the same transaction.
        publish(transactionId, payload);
        await().atMost(Duration.ofSeconds(30))
                .until(() -> committedOffset(TransactionProducer.TOPIC) >= 2);

        // The replay wrote nothing, so the original pair is still all there is.
        assertThat(entryCount()).isEqualTo(2);
        // Two transaction rows, not one: a legacy message with no eventType is
        // read as AUTHORIZE followed by CAPTURE, each claiming its own row.
        // The replay is of the whole record, so it adds neither.
        assertThat(transactionCount()).isEqualTo(2);
        assertThat(balanceOf(alice)).isEqualByComparingTo(new BigDecimal("-50"));
    }

    /**
     * The redelivery that at-least-once delivery actually produces.
     *
     * Rewinding the committed offset makes the broker hand the same record back,
     * exactly as it would after a crash between the ledger write and the commit.
     * The ledger must absorb it without writing a second pair.
     */
    @Test
    void recordRedeliveredAfterOffsetRewindIsNotWrittenTwice() throws Exception {
        String transactionId = UUID.randomUUID().toString();
        // The partition this record landed on, rather than an assumed one --
        // the key decides it, and earlier tests have left committed offsets on
        // the others.
        TopicPartition written = publishAndLocate(
                transactionId, message(transactionId, alice, bob, "50.00", "USD"));

        await().atMost(Duration.ofSeconds(30)).until(() -> entryCount() == 2);
        await().atMost(Duration.ofSeconds(30))
                .until(() -> committedOffset(TransactionProducer.TOPIC) >= 1);

        long offsetBeforeRewind = committedOffset(TransactionProducer.TOPIC);

        // Stop the listener so the group is free, rewind, then restart it.
        MessageListenerContainer container = listenerContainer();
        container.stop();
        rewindOneRecordOn(written);
        container.start();

        // Wait for the rewound record to be consumed again and re-committed.
        await().atMost(Duration.ofSeconds(30))
                .until(() -> committedOffset(TransactionProducer.TOPIC) >= offsetBeforeRewind);

        // Re-consumed and recognized as a replay: still one pair, still two
        // transaction rows (AUTHORIZE, CAPTURE) from the original delivery.
        assertThat(entryCount()).isEqualTo(2);
        assertThat(transactionCount()).isEqualTo(2);
    }

    /**
     * The point of the dead letter path.
     *
     * A message that can never succeed must not stall everything behind it, so
     * the assertion that matters is the second one: a valid message produced
     * after the poison pill is still processed.
     */
    @Test
    void poisonPillGoesToDeadLetterAndPartitionKeepsMoving() {
        String poisonId = UUID.randomUUID().toString();
        // Five decimal places against a NUMERIC(19,4) ledger.
        publish(poisonId, message(poisonId, alice, bob, "50.00001", "USD"));

        String validId = UUID.randomUUID().toString();
        publish(validId, message(validId, alice, bob, "25.00", "USD"));

        // The valid message, produced after the poison pill, still lands.
        await().atMost(Duration.ofSeconds(30)).until(() -> entryCount() == 2);
        assertThat(balanceOf(bob)).isEqualByComparingTo(new BigDecimal("25"));

        List<ConsumerRecord<String, String>> deadLettered = drainDeadLetters(1);
        assertThat(deadLettered).hasSize(1);
        assertThat(deadLettered.get(0).key()).isEqualTo(poisonId);
        assertThat(headerOf(deadLettered.get(0), DeadLetterPublisher.EXCEPTION_HEADER))
                .contains("AmountScaleException");

        // Day 5's dlq_messages_total{reason} wiring test: this is the real
        // production call site (DeadLetterPublisher.publish, over a real
        // broker), not a mock standing in for it.
        io.micrometer.core.instrument.Counter counter = meterRegistry
                .find("dlq_messages_total").tag("reason", "AmountScaleException").counter();
        assertThat(counter).as("dlq_messages_total{reason=AmountScaleException} must be registered").isNotNull();
        assertThat(counter.count()).isGreaterThanOrEqualTo(1.0);
    }

    @Test
    void keyReusedWithDifferentPayloadGoesToDeadLetter() {
        String transactionId = UUID.randomUUID().toString();

        publish(transactionId, message(transactionId, alice, bob, "50.00", "USD"));
        await().atMost(Duration.ofSeconds(30)).until(() -> entryCount() == 2);

        // Same key, different amount: never valid, however often it is retried.
        publish(transactionId, message(transactionId, alice, bob, "75.00", "USD"));

        List<ConsumerRecord<String, String>> deadLettered = drainDeadLetters(1);
        assertThat(deadLettered).hasSize(1);
        assertThat(headerOf(deadLettered.get(0), DeadLetterPublisher.EXCEPTION_HEADER))
                .contains("IdempotencyKeyReuseException");

        // The rejected message added nothing beyond the original delivery's
        // two rows (AUTHORIZE, CAPTURE). Its own AUTHORIZE step is what
        // collides first -- same eventId, different amount -- so its CAPTURE
        // is never reached.
        assertThat(entryCount()).isEqualTo(2);
        assertThat(transactionCount()).isEqualTo(2);
    }

    @Test
    void malformedJsonGoesToDeadLetterAndPartitionKeepsMoving() {
        publish("malformed", "{\"transactionId\": \"broken\", ");

        String validId = UUID.randomUUID().toString();
        publish(validId, message(validId, alice, bob, "10.00", "USD"));

        await().atMost(Duration.ofSeconds(30)).until(() -> entryCount() == 2);

        List<ConsumerRecord<String, String>> deadLettered = drainDeadLetters(1);
        assertThat(deadLettered).hasSize(1);
        assertThat(deadLettered.get(0).value()).isEqualTo("{\"transactionId\": \"broken\", ");
    }

    @Test
    void unknownAccountGoesToDeadLetter() {
        String transactionId = UUID.randomUUID().toString();
        publish(transactionId, message(transactionId, 999_999L, bob, "50.00", "USD"));

        List<ConsumerRecord<String, String>> deadLettered = drainDeadLetters(1);
        assertThat(deadLettered).hasSize(1);
        assertThat(headerOf(deadLettered.get(0), DeadLetterPublisher.EXCEPTION_HEADER))
                .contains("AccountNotFoundException");
        assertThat(entryCount()).isZero();
    }

    private MessageListenerContainer listenerContainer() {
        return listenerRegistry.getListenerContainers().iterator().next();
    }

    /**
     * Moves the group's committed offset backward, forcing redelivery.
     *
     * Safe to join the group here only because the listener container is
     * stopped before this is called; with it running, this consumer would take
     * the partition away from it.
     */
    private void rewindOneRecordOn(TopicPartition target) {
        Map<String, Object> config = Map.of(
                AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA.getBootstrapServers());

        try (AdminClient admin = AdminClient.create(config)) {
            OffsetAndMetadata committed = admin
                    .listConsumerGroupOffsets(KafkaConsumerConfig.GROUP_ID)
                    .partitionsToOffsetAndMetadata()
                    .get(30, TimeUnit.SECONDS)
                    .get(target);

            if (committed == null || committed.offset() == 0) {
                throw new IllegalStateException("nothing committed on " + target + " to rewind");
            }

            Map<String, Object> consumerConfig = consumerConfig(KafkaConsumerConfig.GROUP_ID);
            try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(consumerConfig)) {
                consumer.assign(List.of(target));
                consumer.commitSync(Map.of(target, new OffsetAndMetadata(committed.offset() - 1)));
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException(e);
        } catch (Exception e) {
            throw new IllegalStateException("could not rewind offset", e);
        }
    }

    /** Publishes and reports which partition the record landed on. */
    private TopicPartition publishAndLocate(String key, String payload) {
        try {
            var metadata = deadLetterKafkaTemplate
                    .send(TransactionProducer.TOPIC, key, payload)
                    .get(30, TimeUnit.SECONDS)
                    .getRecordMetadata();
            return new TopicPartition(metadata.topic(), metadata.partition());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException(e);
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }
    }

    /**
     * Reads the live listener group's committed offset.
     *
     * Via AdminClient rather than a KafkaConsumer: a consumer created with the
     * same group.id would join that group as a second member, take the
     * partition away from the listener under test, and report offsets for a
     * group it had just disrupted. AdminClient only queries.
     */
    /**
     * Total committed offsets across every partition of the topic.
     *
     * Summed rather than read from partition 0, because the topic has three
     * partitions and a keyed record lands on whichever one its key hashes to.
     * The total still answers the only question these tests ask: how many
     * records the group has committed.
     */
    private long committedOffset(String topic) {
        Map<String, Object> config = Map.of(
                AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA.getBootstrapServers());

        try (AdminClient admin = AdminClient.create(config)) {
            return admin
                    .listConsumerGroupOffsets(KafkaConsumerConfig.GROUP_ID)
                    .partitionsToOffsetAndMetadata()
                    .get(30, TimeUnit.SECONDS)
                    .entrySet().stream()
                    .filter(entry -> entry.getKey().topic().equals(topic))
                    .mapToLong(entry -> entry.getValue().offset())
                    .sum();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException(e);
        } catch (Exception e) {
            throw new IllegalStateException("could not read committed offset", e);
        }
    }

    /** Reads dead letters produced since this test started, not before it. */
    private List<ConsumerRecord<String, String>> drainDeadLetters(int expected) {
        Map<String, Object> config = consumerConfig("dlt-probe-" + UUID.randomUUID());
        List<ConsumerRecord<String, String>> collected = new ArrayList<>();

        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(config)) {
            TopicPartition partition = new TopicPartition(DeadLetterPublisher.DLT_TOPIC, 0);
            consumer.assign(List.of(partition));
            consumer.seek(partition, deadLetterReadFrom);

            long deadline = System.currentTimeMillis() + Duration.ofSeconds(30).toMillis();
            while (System.currentTimeMillis() < deadline && collected.size() < expected) {
                ConsumerRecords<String, String> polled = consumer.poll(Duration.ofMillis(500));
                polled.forEach(collected::add);
            }
        }
        return collected;
    }

    private Map<String, Object> consumerConfig(String groupId) {
        Map<String, Object> config = new HashMap<>();
        config.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA.getBootstrapServers());
        config.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        config.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        config.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        config.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        config.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        return config;
    }

    private static String headerOf(ConsumerRecord<String, String> record, String header) {
        return new String(record.headers().lastHeader(header).value(), java.nio.charset.StandardCharsets.UTF_8);
    }

    private void publish(String key, String payload) {
        try {
            deadLetterKafkaTemplate.send(TransactionProducer.TOPIC, key, payload).get(30, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException(e);
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }
    }

    /** Amount as a JSON string, matching what the producer writes. */
    private String message(String transactionId, long from, long to, String amount, String currency) {
        return """
                {"transactionId":"%s","fromAccountId":"%d","toAccountId":"%d","amount":"%s","currency":"%s"}"""
                .formatted(transactionId, from, to, amount, currency);
    }

    private long accountId(String name) {
        return jdbc.queryForObject("SELECT id FROM accounts WHERE name = ?", Long.class, name);
    }

    private long entryCount() {
        return jdbc.queryForObject("SELECT count(*) FROM ledger_entries", Long.class);
    }

    private long transactionCount() {
        return jdbc.queryForObject("SELECT count(*) FROM transactions", Long.class);
    }

    private BigDecimal balanceOf(long accountId) {
        return jdbc.queryForObject(
                "SELECT COALESCE(SUM(amount), 0) FROM ledger_entries WHERE account_id = ?",
                BigDecimal.class, accountId);
    }
}
