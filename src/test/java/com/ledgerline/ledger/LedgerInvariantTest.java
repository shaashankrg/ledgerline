package com.ledgerline.ledger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.List;
import java.util.UUID;

import org.assertj.core.api.SoftAssertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.jdbc.core.JdbcTemplate;

import com.ledgerline.AbstractPostgresTest;
import com.ledgerline.domain.EventType;
import com.ledgerline.domain.TransactionEvent;
import com.ledgerline.transfer.TransactionEventService;

/**
 * The test that should stay green for the life of the project.
 *
 * Asserts the ledger invariant against TransactionEventService, the only
 * component that writes to the ledger in production. An earlier version of
 * this test wrote through LedgerWriter.recordTransfer directly, which meant it
 * was exercising a path nothing in the running system actually calls -- a
 * rogue writer bypassing the event service entirely could have broken the
 * invariant here and this test would never have seen it.
 *
 * Also pins the Day 2 schema constraints (zero amount, unknown transaction,
 * unknown account, duplicate idempotency key), which are properties of the
 * table and are asserted directly against raw SQL rather than through any
 * particular write path.
 */
class LedgerInvariantTest extends AbstractPostgresTest {

    @Autowired
    private TransactionEventService eventService;

    @Autowired
    private JdbcTemplate jdbc;

    private long alice;
    private long bob;
    private long carol;
    private long reservePool;

    @BeforeEach
    void setUp() {
        // Entries reference transactions, so they go first.
        jdbc.update("DELETE FROM ledger_entries");
        jdbc.update("DELETE FROM transactions");
        jdbc.update("DELETE FROM transaction_states");

        alice = accountId("Alice Checking");
        bob = accountId("Bob Checking");
        carol = accountId("Carol Checking");
        reservePool = accountId("Reserve Pool");
    }

    @Test
    void ledgerStaysBalancedAfterTransfers() {
        transfer(alice, bob, "50.00");
        transfer(bob, carol, "20.00");
        transfer(carol, reservePool, "12.3456");

        // alice:  -50.00
        // bob:    +50.00 - 20.00 = +30.00
        // carol:  +20.00 - 12.3456 = +7.6544
        // reservePool: +12.3456
        assertBalancesAndInvariant(
                balance(alice, "-50.00"),
                balance(bob, "30.00"),
                balance(carol, "7.6544"),
                balance(reservePool, "12.3456"));
    }

    /**
     * Amounts that are awkward in binary floating point. Were the column a
     * FLOAT, the running total here would drift off zero instead of landing on
     * it exactly.
     */
    @Test
    void ledgerStaysBalancedForAmountsFloatingPointCannotRepresent() {
        transfer(alice, bob, "0.10");
        transfer(alice, bob, "0.20");
        transfer(bob, carol, "0.30");
        transfer(carol, alice, "0.0001");

        // alice: -0.10 - 0.20 + 0.0001 = -0.2999
        // bob:   +0.10 + 0.20 - 0.30 = 0
        // carol: +0.30 - 0.0001 = 0.2999
        assertBalancesAndInvariant(
                balance(alice, "-0.2999"),
                balance(bob, "0"),
                balance(carol, "0.2999"));
    }

    @Test
    void balanceHoldsAcrossManyTransfers() {
        for (int i = 0; i < 100; i++) {
            transfer(alice, bob, "1.01");
        }

        assertBalancesAndInvariant(
                balance(alice, "-101.00"),
                balance(bob, "101.00"));
        assertThat(entryCount()).isEqualTo(200);
    }

    /**
     * A transfer naming a nonexistent account must leave nothing behind.
     * EventValidator rejects the AUTHORIZE before anything is written, so
     * unlike the raw-SQL cases below this never reaches the foreign key --
     * the whole point is that a missing account is caught earlier than that.
     */
    @Test
    void failedTransferLeavesNoPartialEntries() {
        long missingAccount = 999_999L;

        assertThatExceptionOfType(RuntimeException.class)
                .isThrownBy(() -> transfer(alice, missingAccount, "50.00"));

        assertThat(entryCount()).isZero();
        assertThat(jdbc.queryForObject("SELECT count(*) FROM transactions", Long.class)).isZero();
        assertSystemWideBalance();
        assertEveryTransactionBalances();
    }

    @Test
    void duplicateEventIdIsRejected() {
        String key = key();
        transfer(alice, bob, "50.00", key);

        // Same eventId, different amount: the payload hash rejects it before
        // any second entry could be written.
        assertThatExceptionOfType(RuntimeException.class)
                .isThrownBy(() -> transfer(bob, carol, "10.00", key));

        // The rejected retry contributed nothing, so only the original pair
        // is there, and it is on the accounts the original named -- not carol.
        assertBalancesAndInvariant(
                balance(alice, "-50.00"),
                balance(bob, "50.00"),
                balance(carol, "0"));
    }

    @Test
    void zeroAmountEntryIsRejected() {
        long transactionId = insertBareTransaction();

        assertThatExceptionOfType(DataIntegrityViolationException.class)
                .isThrownBy(() -> insertRawEntry(transactionId, alice, BigDecimal.ZERO));
    }

    @Test
    void entryWithUnknownTransactionIsRejected() {
        assertThatExceptionOfType(DataIntegrityViolationException.class)
                .isThrownBy(() -> insertRawEntry(999_999L, alice, new BigDecimal("10.00")));
    }

    @Test
    void entryWithUnknownAccountIsRejected() {
        long transactionId = insertBareTransaction();

        assertThatExceptionOfType(DataIntegrityViolationException.class)
                .isThrownBy(() -> insertRawEntry(transactionId, 999_999L, new BigDecimal("10.00")));
    }

    /**
     * Asserts every named account balance, then the invariant, in one soft
     * block so a failure anywhere does not hide a failure elsewhere.
     *
     * Order matters when assertions differ in strength. Per-account balance is
     * the strongest claim here: two entries on the wrong accounts still sum to
     * zero, so a plain sum-to-zero check cannot catch misdirected money and
     * must not be allowed to report "pass" while masking a balance failure
     * behind it. Soft assertions remove the ordering hazard entirely -- every
     * check runs and every failure is reported, so which one is "first" stops
     * mattering.
     */
    private void assertBalancesAndInvariant(AccountBalance... expected) {
        SoftAssertions softly = new SoftAssertions();

        for (AccountBalance each : expected) {
            softly.assertThat(balanceOf(each.accountId()))
                    .as("balance of account %d", each.accountId())
                    .isEqualByComparingTo(each.expectedAmount());
        }

        softly.assertThat(systemBalance())
                .as("system-wide sum of ledger_entries")
                .isEqualByComparingTo(BigDecimal.ZERO);

        softly.assertThat(unbalancedTransactions())
                .as("transactions whose entries do not sum to zero")
                .isEmpty();

        softly.assertAll();
    }

    private void assertSystemWideBalance() {
        assertThat(systemBalance()).isEqualByComparingTo(BigDecimal.ZERO);
    }

    private void assertEveryTransactionBalances() {
        assertThat(unbalancedTransactions()).isEmpty();
    }

    private BigDecimal systemBalance() {
        return jdbc.queryForObject(
                "SELECT COALESCE(SUM(amount), 0) FROM ledger_entries", BigDecimal.class);
    }

    private List<Long> unbalancedTransactions() {
        return jdbc.queryForList(
                "SELECT transaction_id FROM ledger_entries "
                        + "GROUP BY transaction_id HAVING SUM(amount) <> 0",
                Long.class);
    }

    private BigDecimal balanceOf(long accountId) {
        return jdbc.queryForObject(
                "SELECT COALESCE(SUM(amount), 0) FROM ledger_entries WHERE account_id = ?",
                BigDecimal.class, accountId);
    }

    private static AccountBalance balance(long accountId, String expectedAmount) {
        return new AccountBalance(accountId, new BigDecimal(expectedAmount));
    }

    private record AccountBalance(long accountId, BigDecimal expectedAmount) {
    }

    /** AUTHORIZE then CAPTURE: the pair that writes one balanced set of entries. */
    private void transfer(long from, long to, String amount) {
        transfer(from, to, amount, key());
    }

    private void transfer(long from, long to, String amount, String externalTxnId) {
        BigDecimal parsed = new BigDecimal(amount);
        Instant now = Instant.now();

        eventService.apply(new TransactionEvent(
                externalTxnId, externalTxnId + ":AUTHORIZE", EventType.AUTHORIZE,
                from, to, parsed, "USD", now));
        eventService.apply(new TransactionEvent(
                externalTxnId, externalTxnId + ":CAPTURE", EventType.CAPTURE,
                from, to, parsed, "USD", now));
    }

    private long accountId(String name) {
        return jdbc.queryForObject("SELECT id FROM accounts WHERE name = ?", Long.class, name);
    }

    private long entryCount() {
        return jdbc.queryForObject("SELECT count(*) FROM ledger_entries", Long.class);
    }

    private long insertBareTransaction() {
        return jdbc.queryForObject(
                "INSERT INTO transactions (idempotency_key) VALUES (?) RETURNING id",
                Long.class, key());
    }

    /** Bypasses the write path to poke the schema constraints directly. */
    private void insertRawEntry(long transactionId, long accountId, BigDecimal amount) {
        jdbc.update("INSERT INTO ledger_entries (transaction_id, account_id, amount) VALUES (?, ?, ?)",
                transactionId, accountId, amount);
    }

    private static String key() {
        return UUID.randomUUID().toString();
    }
}
