package com.ledgerline.messaging;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.kafka.ConfluentKafkaContainer;

/**
 * Day 8's sabotage: forces a real Kafka consumer group rebalance and proves
 * readiness actually flips false during it, then true again once it settles.
 *
 * "Force a real rebalance" per the Day 8 prompt, not a simulated one: this
 * test starts a second, independent consumer in the exact same consumer
 * group ({@link KafkaConsumerConfig#GROUP_ID}) against the same topic. A
 * second member joining the group is exactly what triggers a rebalance in
 * production -- a replica scaling event does precisely this, one more
 * consumer joins the group and the broker reassigns partitions among all of
 * them. Nothing here is a mock of a rebalance; it is the broker's own
 * rebalance protocol, observed through the application's real {@link
 * KafkaConsumerGroupHealth} bean.
 *
 * A readiness probe that unconditionally returns UP regardless of actual
 * state is the same class of bug as a metrics counter wired to nothing
 * (Day 5's per-counter wiring tests) -- it looks like a working signal but
 * proves nothing. This test does not accept "returns 200 and nothing
 * crashed" as passing: it specifically observes the DOWN state during the
 * rebalance window, not just the UP state before and after it.
 */
@SpringBootTest
class KafkaConsumerGroupHealthRebalanceTest {

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
                    // Two partitions: with the app's own consumer (concurrency=1,
                    // one thread) already holding both, a second consumer joining
                    // the same group has something real to be reassigned --
                    // one partition each once the rebalance settles, rather than
                    // the second consumer simply sitting idle with zero partitions
                    // forever (which a single-partition topic would produce, and
                    // which would prove nothing about reassignment specifically).
                    new NewTopic(TransactionProducer.TOPIC, 2, (short) 1),
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
    private KafkaConsumerGroupHealth consumerGroupHealth;

    @Test
    void readinessFlipsFalseDuringARealRebalanceAndTrueAgainAfter() {
        // Sanity first: before any interference, the app's own consumer
        // holds both partitions and readiness is UP. Asserted explicitly so
        // a failure later in this test can't be misread as "it was never
        // ready to begin with".
        await().atMost(Duration.ofSeconds(30)).until(consumerGroupHealth::isReady);
        assertThat(consumerGroupHealth.isReady())
                .as("readiness must be UP before any rebalance is forced")
                .isTrue();

        // A second, real consumer joining KafkaConsumerConfig.GROUP_ID
        // against the same topic. This is what an actual replica-scaling
        // event looks like from the broker's perspective: group membership
        // changed, so every existing member's partitions are revoked and
        // reassigned. Started on its own thread since KafkaConsumer.poll
        // must be driven repeatedly for the rebalance protocol to progress
        // (a consumer that joins but never polls again would leave the
        // group stuck mid-rebalance, not complete one).
        java.util.concurrent.atomic.AtomicReference<KafkaConsumer<String, String>> rivalRef =
                new java.util.concurrent.atomic.AtomicReference<>();
        Thread rivalConsumerThread = new Thread(() -> runRivalConsumerUntilWokenUp(rivalRef), "rival-consumer");
        rivalConsumerThread.setDaemon(true);

        try {
            rivalConsumerThread.start();

            // The actual claim under test: readiness must be observed FALSE
            // during the rebalance window, not merely inferred from "it was
            // true before and true after so it must have dipped in between".
            // If this await times out, the rebalance listener is not wired
            // to the real callback (or the listener never fires), which is
            // exactly the sabotage this test exists to catch -- a readiness
            // probe that always reports UP would fail here, not silently
            // pass.
            await().atMost(Duration.ofSeconds(30))
                    .pollInterval(Duration.ofMillis(50))
                    .until(() -> !consumerGroupHealth.isReady());

            // And it must recover: once the rebalance settles (the app's
            // consumer gets reassigned one of the two partitions -- the
            // rival consumer takes the other), readiness returns to UP.
            // Kubernetes routing traffic away from a replica forever after
            // any rebalance would be exactly as wrong as never routing it
            // away at all.
            await().atMost(Duration.ofSeconds(30))
                    .pollInterval(Duration.ofMillis(50))
                    .until(consumerGroupHealth::isReady);

        } finally {
            // wakeup(), not Thread.interrupt(): it's the documented,
            // thread-safe way to break a KafkaConsumer out of a blocking
            // poll() from another thread. Interrupting the thread instead
            // makes the client's own close() sequence (which itself calls
            // network client methods that check the interrupt flag) throw
            // InterruptException from inside cleanup -- harmless to this
            // test's outcome, but noisy, misleading stack traces in output
            // that isn't actually reporting a failure.
            KafkaConsumer<String, String> rival = rivalRef.get();
            if (rival != null) {
                rival.wakeup();
            }
        }
    }

    private void runRivalConsumerUntilWokenUp(
            java.util.concurrent.atomic.AtomicReference<KafkaConsumer<String, String>> rivalRef) {
        Map<String, Object> config = Map.of(
                ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA.getBootstrapServers(),
                ConsumerConfig.GROUP_ID_CONFIG, KafkaConsumerConfig.GROUP_ID,
                ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest",
                ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class,
                ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);

        try (KafkaConsumer<String, String> rival = new KafkaConsumer<>(config)) {
            rivalRef.set(rival);
            rival.subscribe(List.of(TransactionProducer.TOPIC));
            while (true) {
                ConsumerRecords<String, String> polled = rival.poll(Duration.ofMillis(200));
                // Records are neither expected nor relevant here -- this
                // consumer's only job is to exist as a second group member
                // and keep the rebalance protocol progressing via poll().
                // Nothing is committed, so it cannot interfere with the
                // application consumer's own offsets.
                if (!polled.isEmpty()) {
                    // no-op: intentionally not committing or processing
                }
            }
        } catch (org.apache.kafka.common.errors.WakeupException expected) {
            // The normal, documented way this loop ends.
        }
    }
}
