package com.ledgerline.messaging;

import static org.assertj.core.api.Assertions.assertThat;

import java.math.BigDecimal;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.kafka.ConfluentKafkaContainer;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * Producer tests against a real broker.
 *
 * Not an embedded broker and not a mock: what is under test is the shape of the
 * bytes on the wire and the partition a key lands on, both of which are
 * properties of the real client and broker rather than of our code alone.
 *
 * This class does not extend AbstractPostgresTest -- the producer touches no
 * database, so starting Postgres for it would only slow the run.
 */
@SpringBootTest
class TransactionProducerTest {

    private static final String TOPIC = TransactionProducer.TOPIC;

    /*
     * cp-kafka rather than apache/kafka, matching docker-compose.
     *
     * Testcontainers 1.21.3 cannot start the apache/kafka image: that image
     * formats storage with StorageTool before launching the broker, and the
     * format step validates advertised.listeners, but Testcontainers only
     * exports its listener config at broker launch. The format step therefore
     * sees the 0.0.0.0 default and aborts with "advertised.listeners cannot use
     * the nonroutable meta-address". Setting the variable ourselves does not
     * help, because the container overwrites it. Both apache/kafka and
     * apache/kafka-native fail this way; cp-kafka with ConfluentKafkaContainer
     * starts cleanly. Still KRaft -- cp-kafka 7.8 runs without ZooKeeper.
     */
    private static final ConfluentKafkaContainer KAFKA =
            new ConfluentKafkaContainer("confluentinc/cp-kafka:7.8.0");

    static {
        KAFKA.start();
    }

    @DynamicPropertySource
    static void kafkaProperties(DynamicPropertyRegistry registry) {
        registry.add("spring.kafka.bootstrap-servers", KAFKA::getBootstrapServers);
    }

    private static final ObjectMapper JSON = new ObjectMapper();

    @Autowired
    private TransactionProducer producer;

    /**
     * The compose file creates the topic explicitly, so the test broker gets it
     * the same way rather than relying on auto-creation.
     */
    @BeforeAll
    static void createTopic() throws Exception {
        Map<String, Object> config = Map.of(
                AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA.getBootstrapServers());

        try (AdminClient admin = AdminClient.create(config)) {
            // Three partitions, matching docker-compose. The same-key test below
            // is only meaningful with more than one partition to choose from.
            admin.createTopics(List.of(new NewTopic(TOPIC, 3, (short) 1)))
                    .all().get(30, TimeUnit.SECONDS);
        }
    }

    @Test
    void messageRoundTripsWithEveryFieldIntact() throws Exception {
        TransactionMessage sent = new TransactionMessage(
                UUID.randomUUID().toString(), 1L, 2L, new BigDecimal("50.00"), "USD");

        producer.publish(sent).get(30, TimeUnit.SECONDS);

        ConsumerRecord<String, String> received = consumeOne(sent.transactionId());
        JsonNode payload = JSON.readTree(received.value());

        assertThat(received.key()).isEqualTo(sent.transactionId());
        assertThat(payload.get("transactionId").asText()).isEqualTo(sent.transactionId());
        assertThat(payload.get("fromAccountId").asText()).isEqualTo("1");
        assertThat(payload.get("toAccountId").asText()).isEqualTo("2");
        assertThat(payload.get("currency").asText()).isEqualTo("USD");
        assertThat(new BigDecimal(payload.get("amount").asText()))
                .isEqualByComparingTo(sent.amount());
    }

    @Test
    void amountRoundTripsExactlyAsAString() throws Exception {
        BigDecimal exact = new BigDecimal("1234567.89");
        TransactionMessage sent = new TransactionMessage(
                UUID.randomUUID().toString(), 1L, 2L, exact, "USD");

        producer.publish(sent).get(30, TimeUnit.SECONDS);

        ConsumerRecord<String, String> received = consumeOne(sent.transactionId());
        JsonNode payload = JSON.readTree(received.value());

        // Textual, not numeric: a JSON number would already have passed through
        // a double in most consumers before anyone could inspect it.
        assertThat(payload.get("amount").isTextual())
                .as("amount must be a JSON string, payload was %s", received.value())
                .isTrue();
        assertThat(payload.get("amount").isNumber()).isFalse();
        assertThat(received.value()).contains("\"amount\":\"1234567.8900\"");

        assertThat(new BigDecimal(payload.get("amount").asText())).isEqualByComparingTo(exact);
    }

    /**
     * A value with more significant digits than a double can hold.
     *
     * 9007199254740993 is 2^53 + 1, the first integer a double cannot represent
     * -- it rounds to 2^53. Carrying decimal places on top of that puts the
     * value far outside what a double round trip could preserve, so if this
     * survives, the payload genuinely never passed through one.
     */
    @Test
    void valueBeyondDoublePrecisionSurvives() throws Exception {
        BigDecimal beyondDouble = new BigDecimal("9007199254740993.1234");
        TransactionMessage sent = new TransactionMessage(
                UUID.randomUUID().toString(), 1L, 2L, beyondDouble, "USD");

        producer.publish(sent).get(30, TimeUnit.SECONDS);

        ConsumerRecord<String, String> received = consumeOne(sent.transactionId());
        BigDecimal roundTripped = new BigDecimal(JSON.readTree(received.value()).get("amount").asText());

        assertThat(roundTripped).isEqualByComparingTo(beyondDouble);
        // The exact digits, not merely a close value.
        assertThat(roundTripped.toPlainString()).isEqualTo("9007199254740993.1234");

        // What the value would have become had it gone through a double.
        assertThat(roundTripped).isNotEqualByComparingTo(
                BigDecimal.valueOf(beyondDouble.doubleValue()));
    }

    @Test
    void messagesSharingAKeyLandOnOnePartition() throws Exception {
        String sharedKey = UUID.randomUUID().toString();
        int messageCount = 10;

        for (int i = 0; i < messageCount; i++) {
            producer.publish(new TransactionMessage(
                    sharedKey, 1L, 2L, new BigDecimal("1.00"), "USD"))
                    .get(30, TimeUnit.SECONDS);
        }

        List<ConsumerRecord<String, String>> records = consumeMatching(sharedKey, messageCount);

        assertThat(records).hasSize(messageCount);
        Set<Integer> partitions = records.stream()
                .map(ConsumerRecord::partition)
                .collect(Collectors.toSet());
        assertThat(partitions)
                .as("messages with one key must not be split across partitions")
                .hasSize(1);

        // Offsets ascend, so the partition preserved the order they were sent in.
        List<Long> offsets = records.stream().map(ConsumerRecord::offset).toList();
        assertThat(offsets).isSorted();
    }

    /** Reads the topic from the start and returns the record with this key. */
    private ConsumerRecord<String, String> consumeOne(String key) {
        List<ConsumerRecord<String, String>> matches = consumeMatching(key, 1);
        assertThat(matches).as("no record found for key %s", key).hasSize(1);
        return matches.get(0);
    }

    /**
     * Polls until the expected number of records with this key have been seen.
     *
     * Values are deserialized as raw strings so assertions can inspect the JSON
     * as it actually sits on the topic, rather than a Jackson-reconstructed
     * object that would hide how the amount was encoded.
     */
    private List<ConsumerRecord<String, String>> consumeMatching(String key, int expected) {
        Map<String, Object> config = new HashMap<>();
        config.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA.getBootstrapServers());
        config.put(ConsumerConfig.GROUP_ID_CONFIG, "test-" + UUID.randomUUID());
        config.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        config.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        config.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);

        List<ConsumerRecord<String, String>> matches = new ArrayList<>();

        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(config)) {
            consumer.subscribe(List.of(TOPIC));

            long deadline = System.currentTimeMillis() + Duration.ofSeconds(30).toMillis();
            while (System.currentTimeMillis() < deadline && matches.size() < expected) {
                ConsumerRecords<String, String> polled = consumer.poll(Duration.ofMillis(500));
                for (ConsumerRecord<String, String> record : polled) {
                    if (key.equals(record.key())) {
                        matches.add(record);
                    }
                }
            }
        }
        return matches;
    }
}
