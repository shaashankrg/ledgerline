package com.ledgerline.transfer;

import java.math.BigDecimal;
import java.util.Locale;
import java.util.Optional;

import org.springframework.dao.EmptyResultDataAccessException;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import com.ledgerline.ledger.LedgerWriter;

/**
 * Validates transfers and makes them idempotent, then delegates the actual
 * double-entry write to {@link LedgerWriter}.
 *
 * The idempotency guarantee rests on the unique index over
 * transactions.idempotency_key. The insert below is written as
 * INSERT ... ON CONFLICT DO NOTHING rather than SELECT-then-INSERT because two
 * concurrent duplicates can both pass a pre-check and both proceed to write
 * entries. Letting the index arbitrate means exactly one caller can win,
 * whatever the interleaving.
 */
@Service
public class TransferService {

    private final NamedParameterJdbcTemplate jdbc;
    private final LedgerWriter ledgerWriter;

    TransferService(NamedParameterJdbcTemplate jdbc, LedgerWriter ledgerWriter) {
        this.jdbc = jdbc;
        this.ledgerWriter = ledgerWriter;
    }

    /**
     * Records a transfer, or recognizes it as a replay of one already recorded.
     *
     * The idempotency insert and the ledger writes share one transaction. Split
     * across two, a crash in between would leave a claimed idempotency key with
     * no entries behind it -- and every retry would then be treated as a replay
     * of a transfer that never actually happened.
     */
    @Transactional
    public TransferResult transfer(TransferRequest request) {
        validate(request);

        String payloadHash = PayloadHasher.hash(request);
        Optional<Long> insertedId = claimIdempotencyKey(request.idempotencyKey(), payloadHash);

        if (insertedId.isEmpty()) {
            return replayOrReject(request, payloadHash);
        }

        long transactionId = insertedId.get();
        ledgerWriter.recordEntries(
                transactionId,
                request.fromAccountId(),
                request.toAccountId(),
                PayloadHasher.normalizedAmount(request.amount()));

        return new TransferResult(transactionId, false);
    }

    /** Rules that Bean Validation annotations cannot express. */
    private void validate(TransferRequest request) {
        if (request.fromAccountId().equals(request.toAccountId())) {
            throw new SameAccountTransferException(request.fromAccountId());
        }

        // Rejected rather than rounded, so a sub-cent fraction can never be
        // silently dropped on the way into the ledger.
        if (request.amount().scale() > PayloadHasher.LEDGER_SCALE) {
            throw new AmountScaleException(request.amount(), PayloadHasher.LEDGER_SCALE);
        }

        String requestCurrency = request.currency().toUpperCase(Locale.ROOT);
        requireAccountInCurrency(request.fromAccountId(), requestCurrency);
        requireAccountInCurrency(request.toAccountId(), requestCurrency);
    }

    private void requireAccountInCurrency(long accountId, String requestCurrency) {
        String accountCurrency;
        try {
            accountCurrency = jdbc.queryForObject(
                    "SELECT currency FROM accounts WHERE id = :id",
                    new MapSqlParameterSource("id", accountId),
                    String.class);
        } catch (EmptyResultDataAccessException e) {
            throw new AccountNotFoundException(accountId);
        }

        // CHAR(3) comes back space-padded when the value is shorter.
        accountCurrency = accountCurrency.trim();

        if (!accountCurrency.equalsIgnoreCase(requestCurrency)) {
            throw new CurrencyMismatchException(accountId, accountCurrency, requestCurrency);
        }
    }

    /**
     * Attempts to claim the key by inserting the transaction row.
     *
     * @return the new transaction id, or empty if the key was already taken
     */
    private Optional<Long> claimIdempotencyKey(String idempotencyKey, String payloadHash) {
        // RETURNING yields no row when the conflict clause suppresses the
        // insert, which is precisely the "someone else won" signal.
        return jdbc.query(
                "INSERT INTO transactions (idempotency_key, payload_hash) "
                        + "VALUES (:idempotencyKey, :payloadHash) "
                        + "ON CONFLICT (idempotency_key) DO NOTHING "
                        + "RETURNING id",
                new MapSqlParameterSource()
                        .addValue("idempotencyKey", idempotencyKey)
                        .addValue("payloadHash", payloadHash),
                rs -> rs.next() ? Optional.of(rs.getLong("id")) : Optional.<Long>empty());
    }

    /**
     * Handles a key that was already claimed: same payload replays, different
     * payload is a reuse error. Either way, no entries are written.
     */
    private TransferResult replayOrReject(TransferRequest request, String payloadHash) {
        MapSqlParameterSource params = new MapSqlParameterSource("idempotencyKey", request.idempotencyKey());

        Long existingId = jdbc.queryForObject(
                "SELECT id FROM transactions WHERE idempotency_key = :idempotencyKey",
                params, Long.class);
        String existingHash = jdbc.queryForObject(
                "SELECT payload_hash FROM transactions WHERE idempotency_key = :idempotencyKey",
                params, String.class);

        // A row written by something other than this service (LedgerWriter used
        // directly, or a pre-V4 row) has no hash to compare against. Treating an
        // absent hash as a match would let any payload replay it, so it is a
        // reuse error instead.
        if (existingHash == null || !existingHash.trim().equals(payloadHash)) {
            throw new IdempotencyKeyReuseException(request.idempotencyKey());
        }

        return new TransferResult(existingId, true);
    }

    /** Exposed for tests that need the canonical hash of a request. */
    static String canonicalHash(TransferRequest request) {
        return PayloadHasher.hash(request);
    }

    /** Exposed for tests asserting scale normalization. */
    static BigDecimal normalize(BigDecimal amount) {
        return PayloadHasher.normalizedAmount(amount);
    }
}
