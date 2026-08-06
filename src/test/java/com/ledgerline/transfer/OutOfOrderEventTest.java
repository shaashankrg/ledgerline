package com.ledgerline.transfer;

import static org.assertj.core.api.Assertions.assertThat;

import java.math.BigDecimal;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.UUID;

import org.assertj.core.api.SoftAssertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;

import com.ledgerline.AbstractPostgresTest;
import com.ledgerline.domain.EventType;
import com.ledgerline.domain.TransactionEvent;
import com.ledgerline.domain.TransactionState;

/**
 * Events that arrive before they can legally be applied.
 *
 * The distinction this class exists to prove: an early capture is parked and
 * later applied, while a malformed or permanently-wrong event is dead lettered.
 * Conflating the two either discards good transfers or retries bad ones
 * forever.
 */
class OutOfOrderEventTest extends AbstractPostgresTest {

    @Autowired
    private TransactionEventService eventService;

    @Autowired
    private JdbcTemplate jdbc;

    private long alice;
    private long bob;

    @BeforeEach
    void setUp() {
        jdbc.update("DELETE FROM ledger_entries");
        jdbc.update("DELETE FROM transactions");
        jdbc.update("DELETE FROM transaction_states");
        jdbc.update("DELETE FROM parked_events");

        alice = accountId("Alice Checking");
        bob = accountId("Bob Checking");
    }

    @Test
    void captureBeforeAuthorizeIsParkedNotApplied() {
        String txn = txnId();

        var result = eventService.apply(
                event(txn, EventType.CAPTURE, "50.00", Instant.now()));

        SoftAssertions softly = new SoftAssertions();
        softly.assertThat(result.parked()).as("reported as parked").isTrue();
        softly.assertThat(entryCount()).as("entries written").isZero();
        softly.assertThat(pendingParkedCount()).as("parked rows").isEqualTo(1);
        // No state row left behind: the transaction has not begun.
        softly.assertThat(eventService.stateOf(txn)).as("state").isEmpty();
        softly.assertAll();
    }

    @Test
    void authorizeDrainsTheParkedCapture() {
        String txn = txnId();
        Instant t0 = Instant.now().truncatedTo(ChronoUnit.MILLIS);

        eventService.apply(event(txn, EventType.CAPTURE, "50.00", t0.plusMillis(10)));
        assertThat(entryCount()).isZero();

        eventService.apply(event(txn, EventType.AUTHORIZE, "50.00", t0));

        SoftAssertions softly = new SoftAssertions();
        // Per-account balances first: a drained capture landing on the wrong
        // accounts would still produce two entries summing to zero.
        softly.assertThat(balanceOf(alice)).as("alice").isEqualByComparingTo(new BigDecimal("-50"));
        softly.assertThat(balanceOf(bob)).as("bob").isEqualByComparingTo(new BigDecimal("50"));
        softly.assertThat(entryCount()).as("entries").isEqualTo(2);
        softly.assertThat(eventService.stateOf(txn)).as("state").contains(TransactionState.CAPTURED);
        softly.assertThat(pendingParkedCount()).as("still parked").isZero();
        softly.assertAll();
    }

    /**
     * An orphan capture waits indefinitely and blocks nothing.
     *
     * The authorize never comes, so the capture stays parked forever -- which
     * is correct. It is not wrong, merely unanswered, and discarding it would
     * throw away the only evidence that a capture was attempted.
     */
    @Test
    void orphanCaptureStaysParkedAndBlocksNothing() {
        String orphan = txnId();
        eventService.apply(event(orphan, EventType.CAPTURE, "50.00", Instant.now()));

        // An unrelated transaction proceeds normally alongside it.
        String healthy = txnId();
        eventService.apply(event(healthy, EventType.AUTHORIZE, "25.00", Instant.now()));
        eventService.apply(event(healthy, EventType.CAPTURE, "25.00", Instant.now()));

        SoftAssertions softly = new SoftAssertions();
        softly.assertThat(balanceOf(alice)).as("alice").isEqualByComparingTo(new BigDecimal("-25"));
        softly.assertThat(balanceOf(bob)).as("bob").isEqualByComparingTo(new BigDecimal("25"));
        softly.assertThat(entryCount()).as("entries").isEqualTo(2);
        softly.assertThat(pendingParkedCount()).as("orphan still parked").isEqualTo(1);
        softly.assertThat(eventService.stateOf(orphan)).as("orphan state").isEmpty();
        softly.assertAll();
    }

    /**
     * A redelivered authorize must not drain the same parked event twice.
     *
     * The conditional claim on each parked row is what prevents it: the second
     * drain finds every row already claimed and applies nothing.
     */
    @Test
    void redeliveredAuthorizeAppliesParkedEventsExactlyOnce() {
        String txn = txnId();
        Instant t0 = Instant.now().truncatedTo(ChronoUnit.MILLIS);

        eventService.apply(event(txn, EventType.CAPTURE, "50.00", t0.plusMillis(10)));

        TransactionEvent authorize = event(txn, EventType.AUTHORIZE, "50.00", t0);
        eventService.apply(authorize);
        eventService.apply(authorize);
        eventService.apply(authorize);

        SoftAssertions softly = new SoftAssertions();
        softly.assertThat(balanceOf(alice)).as("alice").isEqualByComparingTo(new BigDecimal("-50"));
        softly.assertThat(balanceOf(bob)).as("bob").isEqualByComparingTo(new BigDecimal("50"));
        softly.assertThat(entryCount()).as("entries").isEqualTo(2);
        softly.assertAll();
    }

    /**
     * Parked events drain in the order they occurred, not the order they
     * arrived.
     *
     * A capture and a refund both arrive early and out of sequence. Replayed by
     * arrival they would fail -- a refund cannot precede its capture. Replayed
     * by occurrence they succeed, which is the whole reason the drain sorts on
     * occurred_at.
     */
    @Test
    void parkedEventsDrainInTimestampOrderNotArrivalOrder() {
        String txn = txnId();
        Instant t0 = Instant.now().truncatedTo(ChronoUnit.MILLIS);

        // Refund happened after the capture, but arrives first.
        eventService.apply(event(txn, EventType.REFUND, "50.00", t0.plusMillis(20)));
        eventService.apply(event(txn, EventType.CAPTURE, "50.00", t0.plusMillis(10)));

        assertThat(pendingParkedCount()).isEqualTo(2);

        eventService.apply(event(txn, EventType.AUTHORIZE, "50.00", t0));

        SoftAssertions softly = new SoftAssertions();
        // Capture then refund: four entries, both accounts flat again.
        softly.assertThat(balanceOf(alice)).as("alice").isEqualByComparingTo(BigDecimal.ZERO);
        softly.assertThat(balanceOf(bob)).as("bob").isEqualByComparingTo(BigDecimal.ZERO);
        softly.assertThat(entryCount()).as("entries").isEqualTo(4);
        softly.assertThat(eventService.stateOf(txn)).as("state").contains(TransactionState.REFUNDED);
        softly.assertThat(pendingParkedCount()).as("still parked").isZero();
        softly.assertAll();
    }

    /**
     * A parked event that is still illegal after draining is abandoned, not
     * retried forever.
     *
     * Two captures park. The first drains and succeeds; the second cannot
     * follow CAPTURED and is abandoned with a reason rather than left pending
     * to be retried by every future authorize.
     */
    @Test
    void parkedEventStillIllegalAfterDrainingIsAbandoned() {
        String txn = txnId();
        Instant t0 = Instant.now().truncatedTo(ChronoUnit.MILLIS);

        // Two distinct captures for one transaction: the second can never be
        // legal, since CAPTURED does not accept another CAPTURE.
        eventService.apply(eventWithId(
                txn, txn + ":CAPTURE-A", EventType.CAPTURE, "50.00", t0.plusMillis(10)));
        eventService.apply(eventWithId(
                txn, txn + ":CAPTURE-B", EventType.CAPTURE, "50.00", t0.plusMillis(20)));

        assertThat(pendingParkedCount()).isEqualTo(2);

        eventService.apply(event(txn, EventType.AUTHORIZE, "50.00", t0));

        SoftAssertions softly = new SoftAssertions();
        // Only the first capture applied.
        softly.assertThat(balanceOf(alice)).as("alice").isEqualByComparingTo(new BigDecimal("-50"));
        softly.assertThat(balanceOf(bob)).as("bob").isEqualByComparingTo(new BigDecimal("50"));
        softly.assertThat(entryCount()).as("entries").isEqualTo(2);
        // Nothing left pending: one applied, one abandoned.
        softly.assertThat(pendingParkedCount()).as("still pending").isZero();
        softly.assertThat(abandonedCount()).as("abandoned").isEqualTo(1);
        softly.assertAll();
    }

    /** Parking must not disturb the idempotency claim a drain needs. */
    @Test
    void parkedEventClaimsItsIdempotencyKeyOnlyWhenApplied() {
        String txn = txnId();
        Instant t0 = Instant.now().truncatedTo(ChronoUnit.MILLIS);

        eventService.apply(event(txn, EventType.CAPTURE, "50.00", t0.plusMillis(10)));

        // Parked, so nothing has claimed the key yet.
        assertThat(transactionCount()).isZero();

        eventService.apply(event(txn, EventType.AUTHORIZE, "50.00", t0));

        // Authorize and the drained capture, one row each.
        assertThat(transactionCount()).isEqualTo(2);
    }

    private TransactionEvent event(String txn, EventType type, String amount, Instant occurredAt) {
        return eventWithId(txn, txn + ":" + type.name(), type, amount, occurredAt);
    }

    private TransactionEvent eventWithId(
            String txn, String eventId, EventType type, String amount, Instant occurredAt) {
        return new TransactionEvent(
                txn, eventId, type, alice, bob, new BigDecimal(amount), "USD", occurredAt);
    }

    private static String txnId() {
        return UUID.randomUUID().toString();
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

    private long pendingParkedCount() {
        return jdbc.queryForObject(
                "SELECT count(*) FROM parked_events "
                        + "WHERE applied_at IS NULL AND abandoned_at IS NULL", Long.class);
    }

    private long abandonedCount() {
        return jdbc.queryForObject(
                "SELECT count(*) FROM parked_events WHERE abandoned_at IS NOT NULL", Long.class);
    }

    private BigDecimal balanceOf(long accountId) {
        return jdbc.queryForObject(
                "SELECT COALESCE(SUM(amount), 0) FROM ledger_entries WHERE account_id = ?",
                BigDecimal.class, accountId);
    }
}
