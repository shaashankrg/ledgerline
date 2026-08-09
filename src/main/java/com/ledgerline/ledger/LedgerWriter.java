package com.ledgerline.ledger;

import java.math.BigDecimal;
import java.util.Optional;

import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.jdbc.support.GeneratedKeyHolder;
import org.springframework.jdbc.support.KeyHolder;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

import com.ledgerline.domain.EntryGroup;
import com.ledgerline.domain.IdempotencyKeyReuseException;
import com.ledgerline.domain.LedgerEntry;

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

    /**
     * Writes an already-balanced group of entries against an existing
     * transaction row.
     *
     * The general form of {@link #recordEntries}: that one builds a two-sided
     * pair from a single amount, this one writes whatever set the caller has
     * produced. Used by the event path, where an event's entries come from
     * EntryPolicy rather than from a from/to/amount triple.
     *
     * The group's own constructor has already enforced that it balances, so
     * there is no second check here -- re-verifying it would imply the type
     * could be constructed unbalanced, which it cannot.
     */
    @Transactional
    public void recordEntryGroup(long transactionId, EntryGroup group) {
        for (LedgerEntry entry : group.entries()) {
            insertEntry(transactionId, entry.accountId(), entry.amount());
        }
    }

    /**
     * Claims an event id, creating the transaction row that entries hang off.
     *
     * The idempotency mechanism carried over from the retired transfer path,
     * unchanged in substance. Written as INSERT ... ON CONFLICT DO NOTHING
     * rather than SELECT-then-INSERT because two concurrent deliveries of one
     * event can both pass a pre-check and both go on to write entries; letting
     * the unique index arbitrate means exactly one can win, whatever the
     * interleaving.
     *
     * What changed is only what the key identifies. It was one transfer; it is
     * now one event, so a capture and a later refund of the same transaction
     * claim separate rows. The constraint keeps meaning "this message was
     * applied once", which is what it always meant.
     *
     * @return the new row id, or empty if this event id was already claimed
     */
    @Transactional
    public Optional<Long> claimEvent(String eventId, String payloadHash, String description) {
        return claimEvent(eventId, payloadHash, description, null);
    }

    /**
     * Same as {@link #claimEvent(String, String, String)}, additionally
     * recording which merchant the payment is for, if known.
     *
     * A purely additive column: nothing about the idempotency mechanism
     * above changes. merchantId is passed through unvalidated -- whether it
     * is a sensible value is the caller's business, same as description.
     */
    @Transactional
    public Optional<Long> claimEvent(String eventId, String payloadHash, String description, String merchantId) {
        // RETURNING yields no row when the conflict clause suppresses the
        // insert, which is precisely the "someone else won" signal.
        return jdbc.query(
                "INSERT INTO transactions (idempotency_key, payload_hash, description, merchant_id) "
                        + "VALUES (:eventId, :payloadHash, :description, :merchantId) "
                        + "ON CONFLICT (idempotency_key) DO NOTHING "
                        + "RETURNING id",
                new MapSqlParameterSource()
                        .addValue("eventId", eventId)
                        .addValue("payloadHash", payloadHash)
                        .addValue("description", description)
                        .addValue("merchantId", merchantId),
                rs -> rs.next() ? Optional.of(rs.getLong("id")) : Optional.<Long>empty());
    }

    /**
     * Verifies a claimed event id was used for this same payload.
     *
     * A redelivery carries an identical payload and is harmless. A different
     * payload under the same id means the producer reused the key for
     * something else, and honouring either reading would be wrong: returning
     * the original silently drops the new event, writing a new one breaks the
     * key's promise.
     *
     * @throws IdempotencyKeyReuseException when the stored hash differs
     */
    @Transactional(readOnly = true)
    public void requireMatchingPayload(String eventId, String payloadHash) {
        String storedHash = jdbc.queryForObject(
                "SELECT payload_hash FROM transactions WHERE idempotency_key = :eventId",
                new MapSqlParameterSource("eventId", eventId),
                String.class);

        // A row written without a hash cannot be shown to match, and treating
        // an absent hash as a match would let any payload replay it.
        if (storedHash == null || !storedHash.trim().equals(payloadHash)) {
            throw new IdempotencyKeyReuseException(eventId);
        }
    }

    /**
     * Releases an idempotency claim taken by an event that was not applied.
     *
     * Used when an event is parked rather than processed: it has claimed its
     * key but written nothing, and leaving the claim in place would make the
     * event look already-applied when it eventually drains, silently skipping
     * it. Deleting the row returns the key to unclaimed so the drain can take
     * it for real.
     *
     * Safe because a claimed-but-unapplied row has no entries hanging off it --
     * the foreign key would refuse the delete otherwise, which is a useful
     * second opinion on that invariant.
     */
    @Transactional
    public void releaseClaim(String eventId) {
        jdbc.update(
                "DELETE FROM transactions WHERE idempotency_key = :eventId",
                new MapSqlParameterSource("eventId", eventId));
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
