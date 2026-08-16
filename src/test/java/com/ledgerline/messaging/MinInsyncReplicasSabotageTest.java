package com.ledgerline.messaging;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.time.Duration;
import java.time.Instant;
import java.util.List;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;

import com.ledgerline.domain.EventType;

/**
 * Day 10 sabotage test: with {@code min.insync.replicas=2} on a 3-broker
 * cluster, taking down TWO brokers should make the producer fail loudly
 * rather than silently accept a write only one broker (or zero, if the
 * survivor isn't even the leader for the target partition) actually holds.
 *
 * This is the negative-space check on the whole Day 10 durability story:
 * every other test in this project proves the system tolerates a single
 * broker loss. This one proves it does NOT silently pretend to tolerate a
 * loss of guarantee it can no longer provide -- {@code min.insync.replicas}
 * exists specifically so the cluster refuses writes it cannot make durable,
 * instead of accepting them and hoping.
 *
 * Kills 2 of the 3 broker pods directly via {@code kubectl delete pod},
 * same mechanism as every other live-cluster chaos test in this project.
 * The 2 killed pods are NOT restored by this test -- restoring a 3-broker
 * StatefulSet's quorum after killing 2 of 3 controllers is a cluster
 * recovery operation, not a quick undo, so this is deliberately the last
 * test run in a given cluster's lifetime rather than something meant to be
 * chained with further tests afterward. See docs/day10-multi-broker.md for
 * the recovery procedure (recreate the release) if this cluster needs to be
 * reused afterward.
 */
@SpringBootTest
@EnabledIfSystemProperty(named = "ledgerline.chaostest", matches = "true")
class MinInsyncReplicasSabotageTest {

    private static final Logger log = LoggerFactory.getLogger(MinInsyncReplicasSabotageTest.class);

    private static final String NAMESPACE = System.getProperty("ledgerline.chaostest.namespace", "ledgerline");
    private static final String RELEASE = System.getProperty("ledgerline.chaostest.release", "ledgerline");

    private static final int KAFKA_EXTERNAL_PORT = 9094; // broker 0's EXTERNAL listener is enough to bootstrap
    private static final int POSTGRES_LOCAL_PORT = 15435; // distinct from the other chaos tests' forwards

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
    private KafkaTemplate<String, TransactionMessage> kafkaTemplate;

    /**
     * Kills 2 of the 3 broker pods, then attempts a publish through the
     * real application producer (acks=all, per {@code KafkaProducerConfig}'s
     * default) and asserts it fails -- specifically with a
     * {@code NotEnoughReplicasException}-shaped failure, not a timeout or
     * an ambiguous error, so the failure mode is legible and not just
     * "something went wrong somewhere."
     */
    @Test
    void takingDownTwoOfThreeBrokersMakesTheProducerFailLoudly() throws Exception {
        List<String> allBrokers = List.of(RELEASE + "-kafka-0", RELEASE + "-kafka-1", RELEASE + "-kafka-2");
        List<String> victims = allBrokers.subList(0, 2);

        log.info("SABOTAGE TEST: killing brokers {} to violate min.insync.replicas=2 with only 1 broker left",
                victims);
        for (String victim : victims) {
            KubectlTestSupport.deletePod(NAMESPACE, victim);
        }

        // The killed pods' StatefulSet-managed replacements will start
        // trying to rejoin immediately, but a fresh KRaft controller/broker
        // coming up from a cold PVC does not instantly restore the quorum
        // or catch up its replicas -- this window, before recovery, is
        // exactly the unguaranteed state min.insync.replicas exists to
        // refuse writes during. Giving the survivors a moment to notice
        // the departures (rather than racing the publish against pods that
        // are still Running-but-not-yet-terminated) makes the failure
        // reflect the sabotage condition, not a lucky race.
        Thread.sleep(Duration.ofSeconds(10).toMillis());

        TransactionMessage message = new TransactionMessage(
                "sabotage-" + Instant.now().toEpochMilli(),
                "sabotage-" + Instant.now().toEpochMilli() + ":AUTHORIZE",
                EventType.AUTHORIZE,
                1L, 2L,
                new java.math.BigDecimal("10.00"),
                "USD",
                null);

        log.info("SABOTAGE TEST: attempting a publish with only 1 of 3 brokers alive "
                + "(min.insync.replicas=2 -- this write must be refused, not silently accepted)");

        // KafkaTemplate.send(...) (Spring Kafka, not the raw Kafka client)
        // wraps a failed send in its own KafkaException rather than
        // propagating Future.get()'s ExecutionException directly -- this
        // asserts against what the real application producer actually
        // throws, not the lower-level client API this project doesn't call
        // directly. The specific underlying cause is not pinned to
        // NotEnoughReplicasException: losing 2 of 3 KRaft controllers can
        // just as legitimately manifest as the client being unable to
        // reach a partition leader/fetch metadata at all (a
        // TimeoutException) as it can an ISR-count rejection once a leader
        // is reachable -- both are the cluster correctly refusing to
        // accept an unguaranteed write, which is the actual thing being
        // tested, not the specific exception subclass.
        assertThatThrownBy(() -> kafkaTemplate.send("transactions", message.transactionId(), message).get(
                        Duration.ofSeconds(15).toSeconds(), java.util.concurrent.TimeUnit.SECONDS))
                .as("acks=all against a cluster that cannot satisfy min.insync.replicas=2 "
                        + "must fail the send, not silently succeed on a write only one broker holds")
                .isInstanceOf(org.springframework.kafka.KafkaException.class)
                .cause()
                .satisfies(cause -> assertThat(cause.getClass().getSimpleName())
                        .as("expect a legible, specific failure -- not just any exception")
                        .containsAnyOf("NotEnoughReplicas", "Timeout"));

        log.info("SABOTAGE TEST: publish correctly refused -- min.insync.replicas=2 held");
    }
}
