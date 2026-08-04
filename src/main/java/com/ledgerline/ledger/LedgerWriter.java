package com.ledgerline.ledger;

import java.math.BigDecimal;

import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.jdbc.support.GeneratedKeyHolder;
import org.springframework.jdbc.support.KeyHolder;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

/**
 * Minimal write path for balanced transactions.
 *
 * Intentionally bare: no idempotency conflict handling, no validation, no API
 * surface. Those arrive with the real service layer. This exists so the
 * invariant test has a code path producing entries to assert against.
 */
@Component
public class LedgerWriter {

    private final NamedParameterJdbcTemplate jdbc;

    LedgerWriter(NamedParameterJdbcTemplate jdbc) {
        this.jdbc = jdbc;
    }

    /**
     * Records a transfer as one transaction and two opposing entries.
     *
     * The whole method is a single database transaction. Without that, a crash
     * between the two entry inserts would leave a debit with no matching credit
     * committed to the table, which is exactly the unbalanced state the schema
     * is shaped to prevent. Atomicity is what makes the balance guarantee hold
     * at write time; the schema alone cannot express it.
     *
     * @return the id of the created transaction
     */
    @Transactional
    public long recordTransfer(long fromAccountId, long toAccountId, BigDecimal amount, String idempotencyKey) {
        long transactionId = insertTransaction(idempotencyKey);
        recordEntries(transactionId, fromAccountId, toAccountId, amount);
        return transactionId;
    }

    /**
     * Writes the balanced pair against a transaction row that already exists.
     *
     * Callers that must create the transaction row themselves -- the transfer
     * service claims an idempotency key by inserting it -- use this instead of
     * {@link #recordTransfer}, which would insert a second row for the same
     * key. The responsibility is unchanged either way: this writes a balanced
     * pair and nothing else, and knows nothing about idempotency or validation.
     *
     * Joins the caller's transaction when there is one, so the entries commit
     * or roll back together with whatever else that caller has written.
     */
    @Transactional
    public void recordEntries(long transactionId, long fromAccountId, long toAccountId, BigDecimal amount) {
        // Debit the source, credit the destination. The pair sums to zero by
        // construction: same magnitude, opposite signs.
        insertEntry(transactionId, fromAccountId, amount.negate());
        insertEntry(transactionId, toAccountId, amount);
    }

    private long insertTransaction(String idempotencyKey) {
        KeyHolder keys = new GeneratedKeyHolder();
        jdbc.update(
                "INSERT INTO transactions (idempotency_key) VALUES (:idempotencyKey)",
                new MapSqlParameterSource("idempotencyKey", idempotencyKey),
                keys,
                new String[] { "id" });
        return ((Number) keys.getKeys().get("id")).longValue();
    }

    private void insertEntry(long transactionId, long accountId, BigDecimal amount) {
        jdbc.update(
                "INSERT INTO ledger_entries (transaction_id, account_id, amount) "
                        + "VALUES (:transactionId, :accountId, :amount)",
                new MapSqlParameterSource()
                        .addValue("transactionId", transactionId)
                        .addValue("accountId", accountId)
                        .addValue("amount", amount));
    }
}
