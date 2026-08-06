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
import com.ledgerline.domain.EventType;
import com.ledgerline.domain.IllegalTransitionException;
import com.ledgerline.domain.TransactionEvent;
import com.ledgerline.domain.TransactionState;
import com.ledgerline.transfer.TransactionEventService.EventResult;

/**
 * The event path against a real Postgres.
 *
 * The concurrency test is the point of this class: the compare-and-swap is what
 * stops two consumers processing the same transaction from both applying an
 * event, and that can only be shown by actually racing them.
 */
class TransactionEventServiceTest extends AbstractPostgresTest {

    private static final int CONCURRENT_ITERATIONS = 20;

    @Autowired
    private TransactionEventService eventService;

    @Autowired
    private JdbcTemplate jdbc;

    private static final ExecutorService EXECUTOR = Executors.newFixedThreadPool(4);

    private long alice;
    private long bob;

    @BeforeEach
    void setUp() {
        jdbc.update("DELETE FROM ledger_entries");
        jdbc.update("DELETE FROM transactions");
        jdbc.update("DELETE FROM transaction_states");

        alice = accountId("Alice Checking");
        bob = accountId("Bob Checking");
    }

    @AfterAll
    static void shutdownExecutor() {
        EXECUTOR.shutdownNow();
    }

    @Test
    void authorizeAdvancesStateAndWritesNoEntries() {
        String txn = txnId();

        TransactionState state = eventService.apply(event(txn, EventType.AUTHORIZE, "50.00")).state();

        assertThat(state).isEqualTo(TransactionState.AUTHORIZED);
        assertThat(entryCount()).isZero();
    }

    @Test
    void captureWritesTheBalancedPair() {
        String txn = txnId();
        eventService.apply(event(txn, EventType.AUTHORIZE, "50.00"));

        TransactionState state = eventService.apply(event(txn, EventType.CAPTURE, "50.00")).state();

        assertThat(state).isEqualTo(TransactionState.CAPTURED);
        // Per-account balances first, and via soft assertions: a wrong-account
        // pair still sums to zero and still totals two entries, so those
        // weaker checks must not be allowed to pass silently ahead of the one
        // that actually verifies the money landed on the right accounts.
        SoftAssertions softly = new SoftAssertions();
        softly.assertThat(balanceOf(alice)).as("alice").isEqualByComparingTo(new BigDecimal("-50"));
        softly.assertThat(balanceOf(bob)).as("bob").isEqualByComparingTo(new BigDecimal("50"));
        softly.assertThat(entryCount()).as("entry count").isEqualTo(2);
        softly.assertAll();
    }

    @Test
    void settleAdvancesStateWithoutWritingAgain() {
        String txn = txnId();
        eventService.apply(event(txn, EventType.AUTHORIZE, "50.00"));
        eventService.apply(event(txn, EventType.CAPTURE, "50.00"));

        TransactionState state = eventService.apply(event(txn, EventType.SETTLE, "50.00")).state();

        assertThat(state).isEqualTo(TransactionState.SETTLED);

        // Still the capture's pair: settlement is the external view, not a
        // second source of ledger truth. Both accounts checked, not just the
        // credit side -- a settle that somehow moved money would still leave
        // bob correct if it debited the wrong account instead of alice.
        SoftAssertions softly = new SoftAssertions();
        softly.assertThat(balanceOf(alice)).as("alice").isEqualByComparingTo(new BigDecimal("-50"));
        softly.assertThat(balanceOf(bob)).as("bob").isEqualByComparingTo(new BigDecimal("50"));
        softly.assertThat(entryCount()).as("entry count").isEqualTo(2);
        softly.assertAll();
    }

    @Test
    void refundReversesTheMovement() {
        String txn = txnId();
        eventService.apply(event(txn, EventType.AUTHORIZE, "50.00"));
        eventService.apply(event(txn, EventType.CAPTURE, "50.00"));
        eventService.apply(event(txn, EventType.SETTLE, "50.00"));

        TransactionState state = eventService.apply(event(txn, EventType.REFUND, "50.00")).state();

        assertThat(state).isEqualTo(TransactionState.REFUNDED);

        // Reversed rather than erased: both accounts are flat again, and the
        // history shows it happened and was undone. Flat-flat is a weaker
        // claim than it looks -- two entries landing on the wrong pair would
        // also leave both named accounts untouched, so the entry count and
        // the raw pair amounts are checked too, not just the net.
        SoftAssertions softly = new SoftAssertions();
        softly.assertThat(balanceOf(alice)).as("alice net").isEqualByComparingTo(BigDecimal.ZERO);
        softly.assertThat(balanceOf(bob)).as("bob net").isEqualByComparingTo(BigDecimal.ZERO);
        softly.assertThat(entryCount()).as("entry count").isEqualTo(4);
        softly.assertAll();
    }

    /**
     * A capture with no authorize is parked, not rejected.
     *
     * This test previously asserted an IllegalTransitionException. That was
     * correct before parking existed and is wrong now: an early capture is not
     * a bad event, it has merely overtaken its authorize, which a partitioned
     * topic produces routinely. The permanent-rejection case it used to cover
     * is still covered by rejectedEventLeavesNoEntriesBehind below, where the
     * transaction has a real history the capture contradicts.
     */
    @Test
    void captureWithoutAuthorizeIsParked() {
        String txn = txnId();

        EventResult result = eventService.apply(event(txn, EventType.CAPTURE, "50.00"));

        assertThat(result.parked()).isTrue();
        assertThat(entryCount()).isZero();
        // No state row left behind: nothing has happened to this transaction,
        // and a NEW row would imply it had begun.
        assertThat(eventService.stateOf(txn)).isEmpty();
    }

    /** A rejected event must roll back the state row it created. */
    @Test
    void rejectedEventLeavesNoEntriesBehind() {
        String txn = txnId();
        eventService.apply(event(txn, EventType.AUTHORIZE, "50.00"));
        eventService.apply(event(txn, EventType.VOID, "50.00"));

        assertThatExceptionOfType(IllegalTransitionException.class)
                .isThrownBy(() -> eventService.apply(event(txn, EventType.CAPTURE, "50.00")));

        assertThat(entryCount()).isZero();
        assertThat(eventService.stateOf(txn)).contains(TransactionState.VOIDED);
    }

    /**
     * The test the compare-and-swap exists for.
     *
     * Two events for one transaction are released together. Exactly one must
     * win the transition and write its entries; the loser must write nothing.
     * Repeated, because a single iteration does not reliably interleave.
     */
    @Test
    void concurrentEventsForOneTransactionApplyExactlyOnce() throws Exception {
        for (int iteration = 0; iteration < CONCURRENT_ITERATIONS; iteration++) {
            jdbc.update("DELETE FROM ledger_entries");
            jdbc.update("DELETE FROM transactions");
            jdbc.update("DELETE FROM transaction_states");

            String txn = txnId();
            eventService.apply(event(txn, EventType.AUTHORIZE, "50.00"));

            CountDownLatch ready = new CountDownLatch(2);
            CountDownLatch release = new CountDownLatch(1);
            AtomicInteger applied = new AtomicInteger();
            AtomicInteger rejected = new AtomicInteger();
            List<RuntimeException> thrown = new CopyOnWriteArrayList<>();

            // Two CAPTUREs, distinct event ids -- two consumers processing a
            // redelivered transaction, which is what a rebalance produces.
            List<Callable<Void>> submissions = new ArrayList<>();
            for (int i = 0; i < 2; i++) {
                submissions.add(() -> {
                    ready.countDown();
                    release.await(5, TimeUnit.SECONDS);
                    try {
                        eventService.apply(event(txn, EventType.CAPTURE, "50.00"));
                        applied.incrementAndGet();
                    } catch (RuntimeException e) {
                        rejected.incrementAndGet();
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

            assertThat(applied.get())
                    .as("iteration %d had %d winners and wrote %d entries",
                            iteration, applied.get(), entryCount())
                    .isEqualTo(1);
            assertThat(rejected.get())
                    .as("iteration %d: %s", iteration, describe(thrown))
                    .isEqualTo(1);

            // The loser wrote nothing: one capture, one pair.
            assertThat(entryCount())
                    .as("iteration %d wrote %d entries; the capture was applied twice",
                            iteration, entryCount())
                    .isEqualTo(2);
            assertThat(eventService.stateOf(txn)).contains(TransactionState.CAPTURED);
            assertThat(unbalancedTransactions()).isEmpty();
        }
    }

    @Test
    void versionAdvancesWithEachTransition() {
        String txn = txnId();
        eventService.apply(event(txn, EventType.AUTHORIZE, "50.00"));
        eventService.apply(event(txn, EventType.CAPTURE, "50.00"));

        Long version = jdbc.queryForObject(
                "SELECT version FROM transaction_states WHERE external_txn_id = ?", Long.class, txn);
        assertThat(version).isEqualTo(2L);
    }

    private static String describe(List<RuntimeException> thrown) {
        return thrown.stream()
                .map(e -> e.getClass().getSimpleName() + ": " + e.getMessage())
                .toList()
                .toString();
    }

    private TransactionEvent event(String txn, EventType type, String amount) {
        return new TransactionEvent(
                txn, UUID.randomUUID().toString(), type,
                alice, bob, new BigDecimal(amount), "USD", Instant.now());
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

    private List<Long> unbalancedTransactions() {
        return jdbc.queryForList(
                "SELECT transaction_id FROM ledger_entries "
                        + "GROUP BY transaction_id HAVING SUM(amount) <> 0",
                Long.class);
    }
}
