package com.ledgerline.messaging;

import static org.assertj.core.api.Assertions.assertThat;

import java.math.BigDecimal;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
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
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.ConcurrentMessageListenerContainer;
import org.springframework.kafka.listener.MessageListenerContainer;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.kafka.ConfluentKafkaContainer;

/**
 * What survives a consumer group changing shape mid-flight, and what the ledger
 * assumes about ordering.
 */
@SpringBootTest(properties = "ledgerline.consumer.concurrency=2")
class RebalanceAndOrderingTest {

    private static final int PARTITIONS = 3;

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
                    new NewTopic(TransactionProducer.TOPIC, PARTITIONS, (short) 1),
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
    private KafkaTemplate<String, String> deadLetterKafkaTemplate;

    @Autowired
    private JdbcTemplate jdbc;

    @Autowired
    private KafkaListenerEndpointRegistry listenerRegistry;

    private long alice;
    private long bob;
    private long carol;

    @BeforeEach
    void setUp() {
        jdbc.update("DELETE FROM ledger_entries");
        jdbc.update("DELETE FROM transactions");

        alice = accountId("Alice Checking");
        bob = accountId("Bob Checking");
        carol = accountId("Carol Checking");
    }

    /**
     * A rebalance mid-flight must not lose or duplicate work.
     *
     * Two consumers share three partitions. Part way through a batch one of them
     * stops, which forces the group to reassign partitions while records are
     * still being processed. Any record the stopped consumer had processed but
     * not committed is redelivered to whoever picks the partition up -- so the
     * ledger sees genuine duplicate delivery, and must absorb it.
     *
     * The assertion is exact: every transaction ends with exactly two entries.
     * Fewer means a message was lost, more means one was written twice.
     */
    @Test
    void rebalanceMidFlightLosesNothingAndDuplicatesNothing() throws Exception {
        int transferCount = 60;
        List<String> transactionIds = new ArrayList<>();

        for (int i = 0; i < transferCount; i++) {
            String transactionId = UUID.randomUUID().toString();
            transactionIds.add(transactionId);
            publish(transactionId, message(transactionId, alice, bob, "1.00"));
        }

        ConcurrentMessageListenerContainer<?, ?> container = concurrentContainer();
        awaitAtLeastOneEntry();

        /*
         * Kill one consumer without letting it commit.
         *
         * A graceful stop() commits its offsets on the way out, so the partition
         * changes hands with nothing outstanding and nothing is ever
         * redelivered -- which is not what a crash looks like. stopAbnormally
         * abandons the assignment instead, so whatever that consumer had
         * processed but not committed is handed to the survivor and processed a
         * second time, concurrently with the work it is already doing. That
         * redelivery is the pressure this test exists to apply.
         */
        container.getContainers().get(0).stopAbnormally(() -> {
        });

        awaitEntryCount(transferCount * 2L);

        // Exactly two entries per transaction, no exceptions.
        List<Integer> entryCounts = jdbc.queryForList(
                "SELECT count(*) FROM ledger_entries GROUP BY transaction_id", Integer.class);
        assertThat(entryCounts)
                .as("every transaction must have exactly one debit and one credit")
                .isNotEmpty()
                .allMatch(count -> count == 2);

        assertThat(transactionCount()).isEqualTo(transferCount);
        assertThat(entryCount()).isEqualTo(transferCount * 2L);

        // The ledger invariant still holds across the whole set.
        assertThat(systemBalance()).isEqualByComparingTo(BigDecimal.ZERO);

        // Restart for any test that runs after this one.
        container.getContainers().get(0).start();
        assertThat(transactionIds).hasSize(transferCount);
    }

    /**
     * The ledger does not depend on the order messages are processed in.
     *
     * Transfers touching the same pair of accounts are published under different
     * keys, so they scatter across partitions and are processed in whatever
     * order the group happens to interleave them. Final balances are still
     * exact, because a balance is a SUM over entries and addition does not care
     * about order, and because each transfer is a self-contained balanced pair
     * that never reads a previous balance.
     *
     * This is what makes multiple partitions safe here. A design where a
     * transfer depended on the balance before it -- an overdraft check, say --
     * would need all of an account's transfers on one partition.
     */
    @Test
    void finalBalancesAreCorrectRegardlessOfProcessingOrder() throws Exception {
        List<String> payloads = new ArrayList<>();

        // Alice -> Bob 10.00, eight times.
        for (int i = 0; i < 8; i++) {
            String id = UUID.randomUUID().toString();
            payloads.add(message(id, alice, bob, "10.00"));
        }
        // Bob -> Alice 3.00, five times.
        for (int i = 0; i < 5; i++) {
            String id = UUID.randomUUID().toString();
            payloads.add(message(id, bob, alice, "3.00"));
        }
        // Bob -> Carol 2.50, four times.
        for (int i = 0; i < 4; i++) {
            String id = UUID.randomUUID().toString();
            payloads.add(message(id, bob, carol, "2.50"));
        }

        // Deliberately shuffled, so publication order carries no information.
        Collections.shuffle(payloads);
        for (String payload : payloads) {
            publish(UUID.randomUUID().toString(), payload);
        }

        awaitEntryCount(payloads.size() * 2L);

        // alice: -80 out, +15 in  = -65
        // bob:   +80 in, -15 out, -10 out = +55
        // carol: +10 in
        assertThat(balanceOf(alice)).isEqualByComparingTo(new BigDecimal("-65.00"));
        assertThat(balanceOf(bob)).isEqualByComparingTo(new BigDecimal("55.00"));
        assertThat(balanceOf(carol)).isEqualByComparingTo(new BigDecimal("10.00"));
        assertThat(systemBalance()).isEqualByComparingTo(BigDecimal.ZERO);
    }

    private ConcurrentMessageListenerContainer<?, ?> concurrentContainer() {
        MessageListenerContainer container = listenerRegistry.getListenerContainers().iterator().next();
        return (ConcurrentMessageListenerContainer<?, ?>) container;
    }

    private void awaitAtLeastOneEntry() throws Exception {
        long deadline = System.currentTimeMillis() + Duration.ofSeconds(30).toMillis();
        while (System.currentTimeMillis() < deadline && entryCount() == 0) {
            Thread.sleep(20);
        }
    }

    private void awaitEntryCount(long expected) throws Exception {
        long deadline = System.currentTimeMillis() + Duration.ofSeconds(120).toMillis();
        while (System.currentTimeMillis() < deadline && entryCount() < expected) {
            Thread.sleep(250);
        }
        assertThat(entryCount())
                .as("expected %d entries, the consumer group did not drain in time", expected)
                .isEqualTo(expected);
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

    private String message(String transactionId, long from, long to, String amount) {
        return """
                {"transactionId":"%s","fromAccountId":"%d","toAccountId":"%d","amount":"%s","currency":"USD"}"""
                .formatted(transactionId, from, to, amount);
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

    private BigDecimal systemBalance() {
        return jdbc.queryForObject("SELECT COALESCE(SUM(amount), 0) FROM ledger_entries", BigDecimal.class);
    }
}
