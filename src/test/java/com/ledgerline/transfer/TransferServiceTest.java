package com.ledgerline.transfer;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;

import java.math.BigDecimal;
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

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;

import com.ledgerline.AbstractPostgresTest;

/**
 * Covers validation, idempotent replay, and concurrent duplicate submission.
 *
 * All numeric assertions use isEqualByComparingTo rather than isEqualTo:
 * BigDecimal equality counts scale, so 50.0000 read back from NUMERIC(19,4)
 * does not equal a 50.00 literal despite being the same amount.
 */
class TransferServiceTest extends AbstractPostgresTest {

    private static final int CONCURRENT_ITERATIONS = 20;

    @Autowired
    private TransferService transferService;

    @Autowired
    private JdbcTemplate jdbc;

    private static final ExecutorService EXECUTOR = Executors.newFixedThreadPool(4);

    private long alice;
    private long bob;

    @BeforeEach
    void setUp() {
        jdbc.update("DELETE FROM ledger_entries");
        jdbc.update("DELETE FROM transactions");

        alice = accountId("Alice Checking");
        bob = accountId("Bob Checking");
    }

    @AfterAll
    static void shutdownExecutor() {
        EXECUTOR.shutdownNow();
    }

    @Test
    void transferWritesOneBalancedPair() {
        TransferResult result = transferService.transfer(request(key(), alice, bob, "50.00"));

        assertThat(result.replayed()).isFalse();
        assertThat(entryCount()).isEqualTo(2);
        assertThat(balanceOf(alice)).isEqualByComparingTo(new BigDecimal("-50"));
        assertThat(balanceOf(bob)).isEqualByComparingTo(new BigDecimal("50"));
        assertThat(entriesFor(result.transactionId())).isEqualTo(2);
    }

    @Test
    void sequentialDuplicateIsReplayedNotRewritten() {
        String sharedKey = key();

        TransferResult first = transferService.transfer(request(sharedKey, alice, bob, "50.00"));
        TransferResult second = transferService.transfer(request(sharedKey, alice, bob, "50.00"));

        assertThat(first.replayed()).isFalse();
        assertThat(second.replayed()).isTrue();
        assertThat(second.transactionId()).isEqualTo(first.transactionId());

        // The replay wrote nothing, so the original pair is all there is.
        assertThat(entryCount()).isEqualTo(2);
        assertThat(transactionCount()).isEqualTo(1);
        assertThat(balanceOf(alice)).isEqualByComparingTo(new BigDecimal("-50"));
    }

    /**
     * The case the unique index exists for.
     *
     * Two threads submit the same request and are released together by a latch.
     * Without the index arbitrating, both can pass any pre-check and write a
     * pair each, leaving 4 entries. Repeated because a single iteration does
     * not reliably interleave.
     */
    @Test
    void concurrentDuplicatesWriteExactlyOnePair() throws Exception {
        for (int iteration = 0; iteration < CONCURRENT_ITERATIONS; iteration++) {
            jdbc.update("DELETE FROM ledger_entries");
            jdbc.update("DELETE FROM transactions");

            String sharedKey = key();
            TransferRequest request = request(sharedKey, alice, bob, "50.00");

            CountDownLatch release = new CountDownLatch(1);
            CountDownLatch ready = new CountDownLatch(2);
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
                        TransferResult result = transferService.transfer(request);
                        (result.replayed() ? replayed : written).incrementAndGet();
                    } catch (RuntimeException e) {
                        // A loser may surface as a constraint violation rather
                        // than a clean replay depending on interleaving. Either
                        // is acceptable; writing a second pair is not.
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

            // Both threads parked at the latch before any of them proceeds.
            assertThat(ready.await(5, TimeUnit.SECONDS)).isTrue();
            release.countDown();

            for (Future<Void> future : futures) {
                future.get(15, TimeUnit.SECONDS);
            }

            assertThat(entryCount())
                    .as("iteration %d wrote %d pair(s); exceptions: %s",
                            iteration, entryCount() / 2, describe(thrown))
                    .isEqualTo(2);
            assertThat(transactionCount())
                    .as("iteration %d created more than one transaction", iteration)
                    .isEqualTo(1);
            assertThat(written.get())
                    .as("iteration %d had more than one winner", iteration)
                    .isEqualTo(1);
            assertThat(replayed.get() + failed.get())
                    .as("iteration %d lost a submission", iteration)
                    .isEqualTo(1);
            // The loser must replay cleanly, not surface a raw constraint
            // violation. A SELECT-then-INSERT pre-check keeps the data correct
            // (the unique index still rejects the second insert) but fails this,
            // because the caller gets an exception instead of the original id.
            assertThat(failed.get())
                    .as("iteration %d loser did not replay cleanly: %s", iteration, describe(thrown))
                    .isZero();
            assertThat(unbalancedTransactions()).isEmpty();
        }
    }

    @Test
    void keyReusedForDifferentAmountIsRejected() {
        String sharedKey = key();
        TransferResult original = transferService.transfer(request(sharedKey, alice, bob, "50.00"));

        assertThatExceptionOfType(IdempotencyKeyReuseException.class)
                .isThrownBy(() -> transferService.transfer(request(sharedKey, alice, bob, "75.00")));

        assertThat(entryCount()).isEqualTo(2);
        assertThat(transactionCount()).isEqualTo(1);
        assertThat(transactionIds()).containsExactly(original.transactionId());
        assertThat(balanceOf(alice)).isEqualByComparingTo(new BigDecimal("-50"));
    }

    @Test
    void keyReusedForDifferentAccountsIsRejected() {
        String sharedKey = key();
        long carol = accountId("Carol Checking");
        transferService.transfer(request(sharedKey, alice, bob, "50.00"));

        assertThatExceptionOfType(IdempotencyKeyReuseException.class)
                .isThrownBy(() -> transferService.transfer(request(sharedKey, alice, carol, "50.00")));

        assertThat(entryCount()).isEqualTo(2);
        assertThat(transactionCount()).isEqualTo(1);
    }

    /**
     * "50.0" and "50.00" are the same transfer, so the second submission must
     * replay rather than trip the reuse check.
     */
    @Test
    void differentlyFormattedAmountIsTreatedAsReplay() {
        String sharedKey = key();

        TransferResult first = transferService.transfer(request(sharedKey, alice, bob, "50.0"));
        TransferResult second = transferService.transfer(request(sharedKey, alice, bob, "50.00"));
        TransferResult third = transferService.transfer(request(sharedKey, alice, bob, "50.0000"));

        assertThat(first.replayed()).isFalse();
        assertThat(second.replayed()).isTrue();
        assertThat(third.replayed()).isTrue();
        assertThat(second.transactionId()).isEqualTo(first.transactionId());
        assertThat(third.transactionId()).isEqualTo(first.transactionId());
        assertThat(entryCount()).isEqualTo(2);
    }

    @Test
    void transferToSameAccountIsRejected() {
        assertThatExceptionOfType(SameAccountTransferException.class)
                .isThrownBy(() -> transferService.transfer(request(key(), alice, alice, "50.00")));

        assertNothingPersisted();
    }

    @Test
    void transferFromUnknownAccountIsRejected() {
        assertThatExceptionOfType(AccountNotFoundException.class)
                .isThrownBy(() -> transferService.transfer(request(key(), 999_999L, bob, "50.00")));

        assertNothingPersisted();
    }

    @Test
    void transferToUnknownAccountIsRejected() {
        assertThatExceptionOfType(AccountNotFoundException.class)
                .isThrownBy(() -> transferService.transfer(request(key(), alice, 999_999L, "50.00")));

        assertNothingPersisted();
    }

    @Test
    void currencyMismatchIsRejected() {
        assertThatExceptionOfType(CurrencyMismatchException.class)
                .isThrownBy(() -> transferService.transfer(
                        new TransferRequest(key(), alice, bob, new BigDecimal("50.00"), "EUR")));

        assertNothingPersisted();
    }

    @Test
    void amountBeyondLedgerScaleIsRejected() {
        // Five decimal places against a NUMERIC(19,4) column.
        assertThatExceptionOfType(AmountScaleException.class)
                .isThrownBy(() -> transferService.transfer(request(key(), alice, bob, "50.00001")));

        assertNothingPersisted();
    }

    /**
     * A rejected request must not leave its idempotency key claimed. Were the
     * row to survive the rollback, a corrected retry would read as a reuse and
     * the caller could never succeed with that key.
     */
    @Test
    void rejectedRequestLeavesKeyUnclaimed() {
        String sharedKey = key();

        assertThatExceptionOfType(SameAccountTransferException.class)
                .isThrownBy(() -> transferService.transfer(request(sharedKey, alice, alice, "50.00")));

        assertNothingPersisted();

        // The same key now works, proving the failed attempt claimed nothing.
        TransferResult retry = transferService.transfer(request(sharedKey, alice, bob, "50.00"));
        assertThat(retry.replayed()).isFalse();
        assertThat(entryCount()).isEqualTo(2);
    }

    /** Summarizes thrown exceptions so a concurrency failure names its cause. */
    private static String describe(List<RuntimeException> thrown) {
        if (thrown.isEmpty()) {
            return "none";
        }
        return thrown.stream()
                .map(e -> e.getClass().getSimpleName() + ": " + String.valueOf(e.getMessage()).lines().findFirst().orElse(""))
                .toList()
                .toString();
    }

    private void assertNothingPersisted() {
        assertThat(entryCount()).isZero();
        assertThat(transactionCount()).isZero();
    }

    private TransferRequest request(String idempotencyKey, long from, long to, String amount) {
        return new TransferRequest(idempotencyKey, from, to, new BigDecimal(amount), "USD");
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

    private List<Long> transactionIds() {
        return jdbc.queryForList("SELECT id FROM transactions ORDER BY id", Long.class);
    }

    private long entriesFor(long transactionId) {
        return jdbc.queryForObject(
                "SELECT count(*) FROM ledger_entries WHERE transaction_id = ?", Long.class, transactionId);
    }

    private BigDecimal balanceOf(long accountId) {
        return jdbc.queryForObject(
                "SELECT COALESCE(SUM(amount), 0) FROM ledger_entries WHERE account_id = ?",
                BigDecimal.class, accountId);
    }

    private List<Long> unbalancedTransactions() {
        return jdbc.queryForList(
                "SELECT transaction_id FROM ledger_entries GROUP BY transaction_id HAVING SUM(amount) <> 0",
                Long.class);
    }

    private static String key() {
        return UUID.randomUUID().toString();
    }
}
