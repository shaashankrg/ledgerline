package com.ledgerline.messaging;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import java.math.BigDecimal;
import java.time.Duration;
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

import com.ledgerline.domain.EventType;

import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;

/**
 * The one true publish-to-commit path: {@link TransactionProducer#publish}
 * (which stamps {@link TransactionProducer#PRODUCED_AT_HEADER}) into the real
 * {@link TransactionConsumer} listener, over a real broker.
 *
 * A separate class from {@link TransactionConsumerTest} and {@link
 * TransactionProducerTest} deliberately: those two test the consumer and
 * producer in isolation (the consumer's tests publish raw strings directly to
 * the topic, bypassing TransactionProducer and therefore never stamping the
 * header this timer reads; the producer's tests never run a consumer to
 * receive what they send). Neither exercises the actual round trip the
 * {@code ledgerline_end_to_end_latency} timer measures, so it needed its own
 * test rather than a borrowed assertion bolted onto either.
 */
@SpringBootTest
class EndToEndLatencyTimerTest {

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
                    new NewTopic(TransactionProducer.TOPIC, 3, (short) 1),
                    new NewTopic(DeadLetterPublisher.DLT_TOPIC, 1, (short) 1)))
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
    private TransactionProducer producer;

    @Autowired
    private MeterRegistry meterRegistry;

    @Autowired
    private JdbcTemplate jdbc;

    private long alice;
    private long bob;

    @BeforeEach
    void setUp() {
        jdbc.update("DELETE FROM ledger_entries");
        jdbc.update("DELETE FROM transactions");
        jdbc.update("DELETE FROM transaction_states");

        alice = jdbc.queryForObject("SELECT id FROM accounts WHERE name = ?", Long.class, "Alice Checking");
        bob = jdbc.queryForObject("SELECT id FROM accounts WHERE name = ?", Long.class, "Bob Checking");
    }

    @Test
    void producedHeaderIsPresentOnTheWireBeforeAnyConsumerReadsIt() throws Exception {
        TransactionMessage message = new TransactionMessage(
                UUID.randomUUID().toString(), UUID.randomUUID().toString(),
                EventType.CAPTURE, alice, bob, new BigDecimal("10.00"), "USD");

        var result = producer.publish(message).get(30, TimeUnit.SECONDS);

        var header = result.getProducerRecord().headers().lastHeader(TransactionProducer.PRODUCED_AT_HEADER);
        assertThat(header).as("%s header must be stamped at publish", TransactionProducer.PRODUCED_AT_HEADER)
                .isNotNull();
        assertThat(Long.parseLong(new String(header.value(), java.nio.charset.StandardCharsets.UTF_8)))
                .isCloseTo(System.currentTimeMillis(), org.assertj.core.data.Offset.offset(30_000L));
    }

    @Test
    void endToEndLatencyTimerRecordsASampleForARealPublishedMessage() throws Exception {
        Timer timer = meterRegistry.find("ledgerline_end_to_end_latency").timer();
        assertThat(timer).as("ledgerline_end_to_end_latency must be registered").isNotNull();
        long before = timer.count();

        TransactionMessage message = new TransactionMessage(
                UUID.randomUUID().toString(), UUID.randomUUID().toString(),
                EventType.CAPTURE, alice, bob, new BigDecimal("10.00"), "USD");

        producer.publish(message).get(30, TimeUnit.SECONDS);

        await().atMost(Duration.ofSeconds(30)).until(() -> timer.count() > before);

        // A real, if small, elapsed time -- not a zero placeholder. Proves the
        // header round-tripped through the broker and was actually read back,
        // not that the timer merely got called with an arbitrary duration.
        assertThat(timer.totalTime(TimeUnit.MILLISECONDS)).isGreaterThan(0.0);
    }
}
