package com.ledgerline.transfer;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.assertj.core.api.SoftAssertions;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;

import com.ledgerline.AbstractPostgresTest;
import com.ledgerline.domain.AccountNotFoundException;
import com.ledgerline.domain.AmountScaleException;
import com.ledgerline.domain.CurrencyMismatchException;
import com.ledgerline.domain.EventType;
import com.ledgerline.domain.IdempotencyKeyReuseException;
import com.ledgerline.domain.SameAccountTransferException;
import com.ledgerline.domain.TransactionEvent;

/**
 * Idempotency and business validation on the event path.
 *
 * These behaviours moved here when TransferService was retired as a ledger
 * writer. Every test in this class is the event-path equivalent of one that
 * used to live in TransferServiceTest, kept so the guarantees survived the
 * migration rather than being quietly dropped with the class that held them.
 */
class EventIdempotencyAndValidationTest extends AbstractPostgresTest {

    private static final int CONCURRENT_ITERATIONS = 20;

    @Autowired
    private TransactionEventService eventService;

    @Autowired
    private JdbcTemplate jdbc;

    private static final ExecutorService EXECUTOR = Executors.newFixedThreadPool(4);

    private long alice;
    private long bob;
    private long carol;

    @BeforeEach
    void setUp() {
        jdbc.update("DELETE FROM ledger_entries");
        jdbc.update("DELETE FROM transactions");
        jdbc.update("DELETE FROM transaction_states");

        alice = accountId("Alice Checking");
        bob = accountId("Bob Checking");
        carol = accountId("Carol Checking");
    }

    @AfterAll
    static void shutdownExecutor() {
        EXECUTOR.shutdownNow();
    }

    // ---- idempotency: was sequentialDuplicateIsReplayedNotRewritten ----

    @Test
    void redeliveredEventIsReplayedNotRewritten() {
        String txn = txnId();
        eventService.apply(event(txn, "evt-auth", EventType.AUTHORIZE, alice, bob, "50.00", "USD"));

        TransactionEvent capture =
                event(txn, "evt-cap", EventType.CAPTURE, alice, bob, "50.00", "USD");

        assertThat(eventService.apply(capture).replayed()).isFalse();
        assertThat(eventService.apply(capture).replayed()).isTrue();

        // The replay wrote nothing, so the original pair is all there is.
        // Both sides checked, and ahead of the count: a replay that somehow
        // wrote a second pair on the wrong accounts would still pass a count
        // check reading "2" only by coincidence of these particular numbers.
        SoftAssertions softly = new SoftAssertions();
        softly.assertThat(balanceOf(alice)).as("alice").isEqualByComparingTo(new BigDecimal("-50"));
        softly.assertThat(balanceOf(bob)).as("bob").isEqualByComparingTo(new BigDecimal("50"));
        softly.assertThat(entryCount()).as("entry count").isEqualTo(2);
        softly.assertAll();
    }

    /** "50.0" and "50.00" are the same event, so the second is a replay. */
    @Test
    void differentlyFormattedAmountIsTreatedAsReplay() {
        String txn = txnId();
        eventService.apply(event(txn, "evt-auth", EventType.AUTHORIZE, alice, bob, "50.00", "USD"));

        assertThat(eventService.apply(
                event(txn, "evt-cap", EventType.CAPTURE, alice, bob, "50.0", "USD")).replayed())
                .isFalse();
        assertThat(eventService.apply(
                event(txn, "evt-cap", EventType.CAPTURE, alice, bob, "50.00", "USD")).replayed())
                .isTrue();
        assertThat(eventService.apply(
                event(txn, "evt-cap", EventType.CAPTURE, alice, bob, "50.0000", "USD")).replayed())
                .isTrue();

        assertThat(entryCount()).isEqualTo(2);
    }

    // ---- key reuse: was keyReusedForDifferentAmount / ForDifferentAccounts ----

    @Test
    void eventIdReusedForADifferentAmountIsRejected() {
        String txn = txnId();
        eventService.apply(event(txn, "evt-auth", EventType.AUTHORIZE, alice, bob, "50.00", "USD"));
        eventService.apply(event(txn, "evt-cap", EventType.CAPTURE, alice, bob, "50.00", "USD"));

        assertThatExceptionOfType(IdempotencyKeyReuseException.class)
                .isThrownBy(() -> eventService.apply(
                        event(txn, "evt-cap", EventType.CAPTURE, alice, bob, "75.00", "USD")));

        // The rejected reuse named 75.00, not 50.00 -- checking alice and bob
        // for exactly the original amount is what rules out a silent partial
        // application, not just a total entry count.
        SoftAssertions softly = new SoftAssertions();
        softly.assertThat(balanceOf(alice)).as("alice").isEqualByComparingTo(new BigDecimal("-50"));
        softly.assertThat(balanceOf(bob)).as("bob").isEqualByComparingTo(new BigDecimal("50"));
        softly.assertThat(entryCount()).as("entry count").isEqualTo(2);
        softly.assertAll();
    }

    @Test
    void eventIdReusedForDifferentAccountsIsRejected() {
        String txn = txnId();
        eventService.apply(event(txn, "evt-auth", EventType.AUTHORIZE, alice, bob, "50.00", "USD"));
        eventService.apply(event(txn, "evt-cap", EventType.CAPTURE, alice, bob, "50.00", "USD"));

        assertThatExceptionOfType(IdempotencyKeyReuseException.class)
                .isThrownBy(() -> eventService.apply(
                        event(txn, "evt-cap", EventType.CAPTURE, alice, carol, "50.00", "USD")));

        // The rejected reuse named carol, not bob. Checking carol's balance is
        // the actual assertion here -- a broken guard that quietly redirected
        // the capture to carol would still leave alice and the entry count
        // looking untouched or coincidentally right.
        SoftAssertions softly = new SoftAssertions();
        softly.assertThat(balanceOf(alice)).as("alice").isEqualByComparingTo(new BigDecimal("-50"));
        softly.assertThat(balanceOf(bob)).as("bob").isEqualByComparingTo(new BigDecimal("50"));
        softly.assertThat(balanceOf(carol)).as("carol").isEqualByComparingTo(BigDecimal.ZERO);
        softly.assertThat(entryCount()).as("entry count").isEqualTo(2);
        softly.assertAll();
    }

    /** A capture and a refund of the same amount must not hash alike. */
    @Test
    void eventIdReusedForADifferentEventTypeIsRejected() {
        String txn = txnId();
        eventService.apply(event(txn, "evt-auth", EventType.AUTHORIZE, alice, bob, "50.00", "USD"));
        eventService.apply(event(txn, "evt-x", EventType.CAPTURE, alice, bob, "50.00", "USD"));

        assertThatExceptionOfType(IdempotencyKeyReuseException.class)
                .isThrownBy(() -> eventService.apply(
                        event(txn, "evt-x", EventType.REFUND, alice, bob, "50.00", "USD")));
    }

    // ---- concurrency: was concurrentDuplicatesWriteExactlyOnePair ----

    /**
     * Two deliveries of one event, released together.
     *
     * The unique index on idempotency_key is what arbitrates: exactly one
     * delivery can claim the event id, so exactly one pair is written however
     * the two interleave. Carried over from the transfer path unchanged in
     * intent -- only the key's meaning moved from one transfer to one event.
     */
    @Test
    void concurrentDeliveriesOfOneEventWriteExactlyOnePair() throws Exception {
        for (int iteration = 0; iteration < CONCURRENT_ITERATIONS; iteration++) {
            jdbc.update("DELETE FROM ledger_entries");
            jdbc.update("DELETE FROM transactions");
            jdbc.update("DELETE FROM transaction_states");

            String txn = txnId();
            eventService.apply(event(txn, "evt-auth", EventType.AUTHORIZE, alice, bob, "50.00", "USD"));

            TransactionEvent capture =
                    event(txn, "evt-cap", EventType.CAPTURE, alice, bob, "50.00", "USD");

            CountDownLatch ready = new CountDownLatch(2);
            CountDownLatch release = new CountDownLatch(1);
            AtomicInteger written = new AtomicInteger();
            AtomicInteger replayed = new AtomicInteger();
            AtomicInteger failed = new AtomicInteger();
            List<RuntimeException> thrown = new CopyOnWriteArrayList<>();

            List<Callable<Void>> submissions = new ArrayList<>();
            for (int i = 0; i < 2; i++) {
                submissions.add(() -> {
                    ready.countDown();
                    release.await(5, TimeUnit.SECONDS);
                    try {
                        if (eventService.apply(capture).replayed()) {
                            replayed.incrementAndGet();
                        } else {
                            written.incrementAndGet();
                        }
                    } catch (RuntimeException e) {
                        failed.incrementAndGet();
                        thrown.add(e);
                    }
                    return null;
                });
            }

            List<Future<Void>> futures = new ArrayList<>();
            for (Callable<Void> submission : submissions) {
                futures.add(EXECUTOR.submit(submission));
            }

            assertThat(ready.await(5, TimeUnit.SECONDS)).isTrue();
            release.countDown();
            for (Future<Void> future : futures) {
                future.get(20, TimeUnit.SECONDS);
            }

            assertThat(written.get())
                    .as("iteration %d had more than one winner; %d entries, %s",
                            iteration, entryCount(), describe(thrown))
                    .isEqualTo(1);
            assertThat(replayed.get() + failed.get())
                    .as("iteration %d lost a delivery", iteration)
                    .isEqualTo(1);
            assertThat(entryCount())
                    .as("iteration %d wrote %d entries", iteration, entryCount())
                    .isEqualTo(2);
        }
    }

    // ---- validation: was the five rejection tests ----

    @Test
    void sameAccountIsRejected() {
        assertThatExceptionOfType(SameAccountTransferException.class)
                .isThrownBy(() -> eventService.apply(
                        event(txnId(), "evt-1", EventType.CAPTURE, alice, alice, "50.00", "USD")));
        assertNothingPersisted();
    }

    @Test
    void unknownPayerIsRejected() {
        assertThatExceptionOfType(AccountNotFoundException.class)
                .isThrownBy(() -> eventService.apply(
                        event(txnId(), "evt-1", EventType.CAPTURE, 999_999L, bob, "50.00", "USD")));
        assertNothingPersisted();
    }

    @Test
    void unknownPayeeIsRejected() {
        assertThatExceptionOfType(AccountNotFoundException.class)
                .isThrownBy(() -> eventService.apply(
                        event(txnId(), "evt-1", EventType.CAPTURE, alice, 999_999L, "50.00", "USD")));
        assertNothingPersisted();
    }

    @Test
    void currencyMismatchIsRejected() {
        assertThatExceptionOfType(CurrencyMismatchException.class)
                .isThrownBy(() -> eventService.apply(
                        event(txnId(), "evt-1", EventType.CAPTURE, alice, bob, "50.00", "EUR")));
        assertNothingPersisted();
    }

    @Test
    void amountBeyondLedgerScaleIsRejected() {
        assertThatExceptionOfType(AmountScaleException.class)
                .isThrownBy(() -> eventService.apply(
                        event(txnId(), "evt-1", EventType.CAPTURE, alice, bob, "50.00001", "USD")));
        assertNothingPersisted();
    }

    /** Was rejectedRequestLeavesKeyUnclaimed. */
    @Test
    void rejectedEventLeavesItsIdUnclaimed() {
        String txn = txnId();

        assertThatExceptionOfType(SameAccountTransferException.class)
                .isThrownBy(() -> eventService.apply(
                        event(txn, "evt-1", EventType.CAPTURE, alice, alice, "50.00", "USD")));
        assertNothingPersisted();

        // The same event id now works, proving the failed attempt claimed
        // nothing -- otherwise a corrected retry could never succeed.
        eventService.apply(event(txn, "evt-auth", EventType.AUTHORIZE, alice, bob, "50.00", "USD"));
        assertThat(eventService.apply(
                event(txn, "evt-1", EventType.CAPTURE, alice, bob, "50.00", "USD")).replayed())
                .isFalse();

        SoftAssertions softly = new SoftAssertions();
        softly.assertThat(balanceOf(alice)).as("alice").isEqualByComparingTo(new BigDecimal("-50"));
        softly.assertThat(balanceOf(bob)).as("bob").isEqualByComparingTo(new BigDecimal("50"));
        softly.assertThat(entryCount()).as("entry count").isEqualTo(2);
        softly.assertAll();
    }

    /** State-only events name no accounts, so validation must not demand them. */
    @Test
    void stateOnlyEventsSkipAccountValidation() {
        String txn = txnId();
        eventService.apply(event(txn, "evt-auth", EventType.AUTHORIZE, alice, bob, "50.00", "USD"));
        eventService.apply(event(txn, "evt-cap", EventType.CAPTURE, alice, bob, "50.00", "USD"));

        TransactionEvent settle = new TransactionEvent(
                txn, "evt-settle", EventType.SETTLE, null, null, null, "USD", Instant.now());

        assertThat(eventService.apply(settle).replayed()).isFalse();
        assertThat(entryCount()).isEqualTo(2);
    }

    private void assertNothingPersisted() {
        assertThat(entryCount()).isZero();
        assertThat(jdbc.queryForObject("SELECT count(*) FROM transactions", Long.class)).isZero();
    }

    private static String describe(List<RuntimeException> thrown) {
        return thrown.stream()
                .map(e -> e.getClass().getSimpleName() + ": " + e.getMessage())
                .toList().toString();
    }

    private TransactionEvent event(String txn, String eventId, EventType type,
            long from, long to, String amount, String currency) {
        return new TransactionEvent(txn, txn + ":" + eventId, type,
                from, to, new BigDecimal(amount), currency, Instant.now());
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

    private BigDecimal balanceOf(long accountId) {
        return jdbc.queryForObject(
                "SELECT COALESCE(SUM(amount), 0) FROM ledger_entries WHERE account_id = ?",
                BigDecimal.class, accountId);
    }
}
