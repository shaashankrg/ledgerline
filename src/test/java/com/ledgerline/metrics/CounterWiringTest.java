package com.ledgerline.metrics;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.UUID;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;

import com.ledgerline.AbstractPostgresTest;
import com.ledgerline.domain.EventType;
import com.ledgerline.domain.IdempotencyKeyReuseException;
import com.ledgerline.domain.IllegalTransitionException;
import com.ledgerline.domain.TransactionEvent;
import com.ledgerline.reconciliation.ReconciliationService;
import com.ledgerline.transfer.TransactionEventService;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;

/**
 * One test per counter, each triggering the exact behavior its name claims
 * and asserting that exact meter moved.
 *
 * Deliberately not batched into one generic "metrics work" test. A counter
 * wired to nothing looks identical to a correctly-wired one on a dashboard --
 * the same failure mode as dead code, just at the metrics layer -- and the
 * only way to catch that is to trigger each one's specific cause and check
 * its specific effect, the same "assert per item" discipline this project
 * applies everywhere else, applied here to meters instead of ledger rows.
 *
 * DLQ and recon-exception coverage for this class deliberately use the
 * lightest fixture that reaches the real call site (LedgerWriter /
 * DeadLetterPublisher / ReconciliationService.insertException), not a mock --
 * a counter wired to a mock's invocation proves the test author believed the
 * wiring was right, not that it is.
 */
class CounterWiringTest extends AbstractPostgresTest {

    @Autowired
    private TransactionEventService eventService;

    @Autowired
    private ReconciliationService reconciliationService;

    @Autowired
    private MeterRegistry registry;

    @Autowired
    private JdbcTemplate jdbc;

    private long alice;
    private long bob;

    @BeforeEach
    void setUp() {
        jdbc.update("DELETE FROM recon_line_outcomes");
        jdbc.update("DELETE FROM recon_exceptions");
        jdbc.update("DELETE FROM recon_runs");
        jdbc.update("DELETE FROM settlement_records");
        jdbc.update("DELETE FROM recon_batches");
        jdbc.update("DELETE FROM parked_events");
        jdbc.update("DELETE FROM ledger_entries");
        jdbc.update("DELETE FROM transactions");
        jdbc.update("DELETE FROM transaction_states");

        alice = accountId("Alice Checking");
        bob = accountId("Bob Checking");
    }

    @Test
    void paymentsProcessedTotalIncrementsOnCapture() {
        double before = counterValue("payments_processed_total", "event_type", "CAPTURE");

        authorizeAndCapture(txnId());

        assertThat(counterValue("payments_processed_total", "event_type", "CAPTURE"))
                .isEqualTo(before + 1);
    }

    @Test
    void idempotentDuplicatesRejectedTotalIncrementsOnRedelivery() {
        double before = counterValue("idempotent_duplicates_rejected_total");

        String txn = txnId();
        TransactionEvent authorize = authorizeEvent(txn);
        eventService.apply(authorize);
        // Exact same event, redelivered -- same eventId, same payload.
        eventService.apply(authorize);

        assertThat(counterValue("idempotent_duplicates_rejected_total")).isEqualTo(before + 1);
    }

    @Test
    void payloadHashMismatchesTotalIncrementsOnReusedEventIdDifferentPayload() {
        double before = counterValue("payload_hash_mismatches_total");

        String txn = txnId();
        String eventId = txn + ":AUTHORIZE";
        eventService.apply(new TransactionEvent(
                txn, eventId, EventType.AUTHORIZE, null, null, null, null, Instant.now()));

        // Same eventId, materially different event (a movement this time) --
        // the hash cannot match.
        assertThatThrownBy(() -> eventService.apply(new TransactionEvent(
                        txn, eventId, EventType.AUTHORIZE, alice, bob,
                        new BigDecimal("5.00"), "USD", Instant.now())))
                .isInstanceOf(IdempotencyKeyReuseException.class);

        assertThat(counterValue("payload_hash_mismatches_total")).isEqualTo(before + 1);
    }

    // dlq_messages_total{reason} is covered in
    // com.ledgerline.messaging.TransactionConsumerTest
    // #poisonPillGoesToDeadLetterAndPartitionKeepsMoving, which already runs
    // a real broker to exercise the DLQ path -- reimplementing that
    // Kafka-container setup here just to reach the same call site would be
    // a second, weaker copy of the same test, not additional coverage.

    @Test
    void parkedEventsTotalIncrementsWhenCaptureArrivesBeforeAuthorize() {
        double before = counterValue("parked_events_total", "reason", "early");

        String txn = txnId();
        // Capture with no prior authorize: NEW state, capture is not legal,
        // and the transaction has no history yet -- the "early" park path.
        var result = eventService.apply(new TransactionEvent(
                txn, txn + ":CAPTURE", EventType.CAPTURE, alice, bob,
                new BigDecimal("5.00"), "USD", Instant.now()));

        assertThat(result.parked()).isTrue();
        assertThat(counterValue("parked_events_total", "reason", "early")).isEqualTo(before + 1);
    }

    @Test
    void parkedEventsDrainedTotalIncrementsWhenAuthorizeDrainsAParkedCapture() {
        double before = counterValue("parked_events_drained_total");

        String txn = txnId();
        eventService.apply(new TransactionEvent(
                txn, txn + ":CAPTURE", EventType.CAPTURE, alice, bob,
                new BigDecimal("5.00"), "USD", Instant.now()));

        // The authorize that unblocks the parked capture above.
        eventService.apply(authorizeEvent(txn));

        assertThat(counterValue("parked_events_drained_total")).isEqualTo(before + 1);
    }

    @Test
    void stateTransitionsRejectedTotalIncrementsOnIllegalTransition() {
        double before = counterValue("state_transitions_rejected_total", "from", "REFUNDED", "to", "CAPTURE");

        String txn = txnId();
        authorizeAndCapture(txn);
        eventService.apply(new TransactionEvent(
                txn, txn + ":SETTLE", EventType.SETTLE, null, null, null, null, Instant.now()));
        eventService.apply(new TransactionEvent(
                txn, txn + ":REFUND", EventType.REFUND, alice, bob,
                new BigDecimal("5.00"), "USD", Instant.now()));

        // REFUNDED is terminal: a second capture against it has a history it
        // contradicts, not an early arrival, so this is the WRONG/reject path.
        assertThatThrownBy(() -> eventService.apply(new TransactionEvent(
                        txn, txn + ":CAPTURE2", EventType.CAPTURE, alice, bob,
                        new BigDecimal("5.00"), "USD", Instant.now())))
                .isInstanceOf(IllegalTransitionException.class);

        assertThat(counterValue("state_transitions_rejected_total", "from", "REFUNDED", "to", "CAPTURE"))
                .isEqualTo(before + 1);
    }

    @Test
    void reconExceptionsTotalIncrementsPerExceptionType() {
        double before = counterValue("recon_exceptions_total", "type", "MISSING_IN_LEDGER");

        String batchId = "batch-" + UUID.randomUUID();
        jdbc.update(
                "INSERT INTO recon_batches (batch_id, run_id, seed, base_instant, row_count, file_sha256) "
                        + "VALUES (?, ?, 1, now(), 1, 'deadbeef')",
                batchId, "run-" + UUID.randomUUID());
        // A line with no external_txn_id at all -- unconditionally
        // MISSING_IN_LEDGER, the first branch ReconciliationService.
        // classifyAndRecord checks.
        jdbc.update(
                "INSERT INTO settlement_records "
                        + "(batch_id, line_number, external_txn_id, merchant_id, gross_amount_minor, "
                        + " fee_minor, currency, settled_at, raw_line) "
                        + "VALUES (?, 1, NULL, 'merch-1', 5000, 0, 'USD', now(), 'raw')",
                batchId);

        reconciliationService.run(batchId);

        assertThat(counterValue("recon_exceptions_total", "type", "MISSING_IN_LEDGER")).isEqualTo(before + 1);
    }

    // ledgerline_end_to_end_latency's real wiring test -- a genuine publish
    // through TransactionProducer (which stamps the header this timer reads)
    // consumed by the real TransactionConsumer listener over a real broker --
    // lives in com.ledgerline.messaging.EndToEndLatencyTimerTest. Neither
    // TransactionConsumerTest (publishes raw strings, bypassing the header)
    // nor TransactionProducerTest (never runs a consumer) exercises the
    // round trip this timer measures, which is why it needed its own class
    // instead of a borrowed assertion here.

    // ---- helpers -----------------------------------------------------------

    private void authorizeAndCapture(String txn) {
        eventService.apply(authorizeEvent(txn));
        eventService.apply(new TransactionEvent(
                txn, txn + ":CAPTURE", EventType.CAPTURE, alice, bob,
                new BigDecimal("5.00"), "USD", Instant.now()));
    }

    private TransactionEvent authorizeEvent(String txn) {
        return new TransactionEvent(
                txn, txn + ":AUTHORIZE", EventType.AUTHORIZE, null, null, null, null, Instant.now());
    }

    private static String txnId() {
        return "txn-" + UUID.randomUUID();
    }

    private long accountId(String name) {
        return jdbc.queryForObject("SELECT id FROM accounts WHERE name = ?", Long.class, name);
    }

    private double counterValue(String name, String... tags) {
        Counter counter = registry.find(name).tags(tags).counter();
        return counter == null ? 0.0 : counter.count();
    }
}
