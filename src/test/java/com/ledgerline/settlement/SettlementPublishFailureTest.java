package com.ledgerline.settlement;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Clock;
import java.time.Instant;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.kafka.support.SendResult;

import com.ledgerline.AbstractPostgresTest;
import com.ledgerline.domain.EventType;
import com.ledgerline.generator.GeneratorConfig;
import com.ledgerline.generator.TransactionGenerator;
import com.ledgerline.generator.TransactionGenerator.GeneratorResult;
import com.ledgerline.messaging.TransactionMessage;
import com.ledgerline.messaging.TransactionProducer;

/**
 * Proves the settlement file models payments the world saw, not payments our
 * pipeline successfully transmitted.
 *
 * A dedicated, small class rather than a method in {@code SettlementSimulatorTest}:
 * {@code @MockBean} replaces {@link TransactionProducer} for this class's
 * whole Spring context, which would be the wrong substitution for every other
 * test in that file -- those depend on messages genuinely reaching a real
 * broker so the simulator has something real to read. What this test needs
 * from Kafka is nothing at all: {@link TransactionGenerator#generate} only
 * needs {@code publish} to return a future, and neither it nor
 * {@link SettlementSimulator} ever reads a message back from a topic, so a
 * mocked producer is the right tool here, not a missing-real-infra shortcut.
 */
class SettlementPublishFailureTest extends AbstractPostgresTest {

    @Autowired
    private TransactionGenerator generator;

    @Autowired
    private SettlementSimulator simulator;

    @Autowired
    private JdbcTemplate jdbc;

    @MockBean
    private TransactionProducer producer;

    private List<Long> accountIds;

    @BeforeEach
    void setUp() {
        jdbc.update("DELETE FROM settlement_records");
        jdbc.update("DELETE FROM recon_batches");
        jdbc.update("DELETE FROM faultlab.injected_faults");
        jdbc.update("DELETE FROM faultlab.generator_runs");

        accountIds = jdbc.queryForList("SELECT id FROM accounts ORDER BY id", Long.class);
    }

    /**
     * The settlement file models the world, not our transmission log.
     *
     * A real card network settles a payment because it saw the authorization
     * happen, not because our Kafka producer happened to succeed -- so a
     * payment whose CAPTURE publish fails must still appear in the generated
     * file. If it didn't, a producer-side loss would be invisible in both the
     * ledger (never written, because the consumer never saw it) and the
     * settlement file (never recorded, because the generator dropped it too),
     * and reconciliation would report perfect agreement on a payment that was
     * actually lost.
     */
    @Test
    void settlementFileIncludesPaymentsWhosePublishFailed() throws Exception {
        // AUTHORIZE and SETTLE succeed; the CAPTURE for this run's one
        // transaction fails. Deterministic and reproducible without depending
        // on send ordering or timing: it is keyed on event type, not on which
        // call number the failure lands on.
        Mockito.when(producer.publish(Mockito.any())).thenAnswer(invocation -> {
            TransactionMessage message = invocation.getArgument(0);
            if (message.eventType() == EventType.CAPTURE) {
                CompletableFuture<SendResult<String, TransactionMessage>> failed = new CompletableFuture<>();
                failed.completeExceptionally(new RuntimeException("simulated broker rejection"));
                return failed;
            }
            return CompletableFuture.completedFuture((SendResult<String, TransactionMessage>) null);
        });

        String runId = "run-" + UUID.randomUUID();
        GeneratorResult published = generator.generate(
                GeneratorConfig.clean(runId, 909L, 1, accountIds));

        // The failed CAPTURE is still in the generator's own record...
        boolean captureRecorded = published.messages().stream()
                .anyMatch(m -> m.eventType() == EventType.CAPTURE);
        assertThat(captureRecorded)
                .as("a payment whose publish failed must still be recorded as created")
                .isTrue();
        // ...even though the successful-send counter undercounts it.
        assertThat(published.publishedMessages())
                .as("publishedMessages (successful sends only) must not count the failed capture")
                .isLessThan(published.messages().size());

        String batchId = "batch-" + UUID.randomUUID();
        var result = simulator.generate(
                SettlementConfig.clean(runId, batchId, 1L, Instant.parse("2026-01-01T00:00:00Z"),
                        Clock.systemUTC()),
                published);

        assertThat(result.rowCount())
                .as("the settlement file must contain a row for the captured payment "
                        + "even though its Kafka publish failed -- the network settled "
                        + "money it saw move, regardless of whether our own pipeline "
                        + "successfully recorded it")
                .isEqualTo(1);
    }

    /**
     * A batch is self-describing about its own generator run's transmission
     * health, so a later reader doesn't need the logs from the machine that
     * produced it to tell a clean run from a flaky one.
     *
     * Reuses the same failing-CAPTURE mock as the test above, but drives a
     * larger run so the recorded count is unambiguously the number of
     * failures, not a coincidence of a single-transaction run.
     */
    @Test
    void publishFailuresAreRecordedOnTheBatch() throws Exception {
        Mockito.when(producer.publish(Mockito.any())).thenAnswer(invocation -> {
            TransactionMessage message = invocation.getArgument(0);
            if (message.eventType() == EventType.CAPTURE) {
                CompletableFuture<SendResult<String, TransactionMessage>> failed = new CompletableFuture<>();
                failed.completeExceptionally(new RuntimeException("simulated broker rejection"));
                return failed;
            }
            return CompletableFuture.completedFuture((SendResult<String, TransactionMessage>) null);
        });

        String runId = "run-" + UUID.randomUUID();
        // Every transaction's AUTHORIZE and SETTLE succeed; every CAPTURE
        // fails, so publishFailures should equal the transaction count.
        GeneratorResult published = generator.generate(
                GeneratorConfig.clean(runId, 1234L, 5, accountIds));
        assertThat(published.publishFailures()).isEqualTo(5);

        String batchId = "batch-" + UUID.randomUUID();
        simulator.generate(
                SettlementConfig.clean(runId, batchId, 1L, Instant.parse("2026-01-01T00:00:00Z"),
                        Clock.systemUTC()),
                published);

        Integer recorded = jdbc.queryForObject(
                "SELECT publish_failures FROM recon_batches WHERE batch_id = ?", Integer.class, batchId);
        assertThat(recorded)
                .as("recon_batches.publish_failures must reflect the actual count from the run")
                .isEqualTo(5);
    }

    /** A clean run -- no mocked failures -- must record zero, not a stale or default value. */
    @Test
    void cleanRunRecordsZeroPublishFailures() throws Exception {
        Mockito.when(producer.publish(Mockito.any()))
                .thenReturn(CompletableFuture.completedFuture((SendResult<String, TransactionMessage>) null));

        String runId = "run-" + UUID.randomUUID();
        GeneratorResult published = generator.generate(
                GeneratorConfig.clean(runId, 5678L, 5, accountIds));
        assertThat(published.publishFailures()).isZero();

        String batchId = "batch-" + UUID.randomUUID();
        simulator.generate(
                SettlementConfig.clean(runId, batchId, 1L, Instant.parse("2026-01-01T00:00:00Z"),
                        Clock.systemUTC()),
                published);

        Integer recorded = jdbc.queryForObject(
                "SELECT publish_failures FROM recon_batches WHERE batch_id = ?", Integer.class, batchId);
        assertThat(recorded).as("a clean run must record zero, not leave it unset").isZero();
    }
}
