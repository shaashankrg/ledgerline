package com.ledgerline.ledger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;

import java.math.BigDecimal;
import java.util.List;
import java.util.UUID;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.jdbc.core.JdbcTemplate;

import com.ledgerline.AbstractPostgresTest;

/**
 * The test that should stay green for the life of the project.
 *
 * Asserts the ledger invariant at two levels, and pins the Day 2 schema
 * constraints so a future loosening of them fails here.
 */
class LedgerInvariantTest extends AbstractPostgresTest {

    @Autowired
    private LedgerWriter ledgerWriter;

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

        alice = accountId("Alice Checking");
        bob = accountId("Bob Checking");
        carol = accountId("Carol Checking");
        reservePool = accountId("Reserve Pool");
    }

    @Test
    void ledgerStaysBalancedAfterTransfers() {
        ledgerWriter.recordTransfer(alice, bob, new BigDecimal("50.00"), key());
        ledgerWriter.recordTransfer(bob, carol, new BigDecimal("20.00"), key());
        ledgerWriter.recordTransfer(carol, reservePool, new BigDecimal("12.3456"), key());

        assertSystemWideBalance();
        assertEveryTransactionBalances();
    }

    /**
     * Amounts that are awkward in binary floating point. Were the column a
     * FLOAT, the running total here would drift off zero instead of landing on
     * it exactly.
     */
    @Test
    void ledgerStaysBalancedForAmountsFloatingPointCannotRepresent() {
        ledgerWriter.recordTransfer(alice, bob, new BigDecimal("0.10"), key());
        ledgerWriter.recordTransfer(alice, bob, new BigDecimal("0.20"), key());
        ledgerWriter.recordTransfer(bob, carol, new BigDecimal("0.30"), key());
        ledgerWriter.recordTransfer(carol, alice, new BigDecimal("0.0001"), key());

        assertSystemWideBalance();
        assertEveryTransactionBalances();
    }

    @Test
    void balanceHoldsAcrossManyTransfers() {
        for (int i = 0; i < 100; i++) {
            ledgerWriter.recordTransfer(alice, bob, new BigDecimal("1.01"), key());
        }

        assertSystemWideBalance();
        assertEveryTransactionBalances();
        assertThat(entryCount()).isEqualTo(200);
    }

    /**
     * A transfer that fails partway must leave nothing behind. The credit here
     * targets a nonexistent account, so the foreign key rejects it after the
     * debit has already been inserted -- the rollback is what keeps a lone
     * debit out of the table.
     */
    @Test
    void failedTransferLeavesNoPartialEntries() {
        long missingAccount = 999_999L;

        assertThatExceptionOfType(DataIntegrityViolationException.class)
                .isThrownBy(() -> ledgerWriter.recordTransfer(
                        alice, missingAccount, new BigDecimal("50.00"), key()));

        assertThat(entryCount()).isZero();
        assertThat(jdbc.queryForObject("SELECT count(*) FROM transactions", Long.class)).isZero();
        assertSystemWideBalance();
        assertEveryTransactionBalances();
    }

    @Test
    void duplicateIdempotencyKeyIsRejected() {
        String reusedKey = key();
        ledgerWriter.recordTransfer(alice, bob, new BigDecimal("50.00"), reusedKey);

        assertThatExceptionOfType(DataIntegrityViolationException.class)
                .isThrownBy(() -> ledgerWriter.recordTransfer(
                        bob, carol, new BigDecimal("10.00"), reusedKey));

        // The rejected retry contributed nothing, so only the original pair remains.
        assertThat(entryCount()).isEqualTo(2);
        assertSystemWideBalance();
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

    /** Every entry in the table sums to exactly zero. */
    private void assertSystemWideBalance() {
        BigDecimal systemTotal = jdbc.queryForObject(
                "SELECT COALESCE(SUM(amount), 0) FROM ledger_entries", BigDecimal.class);

        // isEqualByComparingTo, not isEqualTo: BigDecimal equality counts scale,
        // so 0.0000 does not equal ZERO despite being the same number.
        assertThat(systemTotal).isEqualByComparingTo(BigDecimal.ZERO);
    }

    /** No individual transaction is unbalanced. */
    private void assertEveryTransactionBalances() {
        List<Long> unbalanced = jdbc.queryForList(
                "SELECT transaction_id FROM ledger_entries "
                        + "GROUP BY transaction_id HAVING SUM(amount) <> 0",
                Long.class);

        assertThat(unbalanced).isEmpty();
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

    /** Bypasses LedgerWriter to poke the schema constraints directly. */
    private void insertRawEntry(long transactionId, long accountId, BigDecimal amount) {
        jdbc.update("INSERT INTO ledger_entries (transaction_id, account_id, amount) VALUES (?, ?, ?)",
                transactionId, accountId, amount);
    }

    private static String key() {
        return UUID.randomUUID().toString();
    }
}
