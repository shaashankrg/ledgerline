package com.ledgerline.reconciliation;

import java.math.BigDecimal;
import java.sql.ResultSet;
import java.time.Instant;
import java.util.List;

import org.springframework.jdbc.core.RowMapper;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.stereotype.Component;

/**
 * The ledger's own view of what was captured, for a given generator run.
 *
 * Not a stored table -- balances and captured amounts are derived by query
 * here exactly as {@link com.ledgerline.ledger.LedgerQueries} derives account
 * balances, for the same reason: a second stored copy of an amount the
 * ledger already knows is a second source of truth that can drift from it.
 *
 * There is no column anywhere named "captured amount". It is derived by
 * joining {@code transactions} to {@code ledger_entries} through the
 * {@code idempotency_key = external_txn_id || ':CAPTURE'} convention that
 * {@code TransactionEventService} already establishes for every CAPTURE
 * event (one {@code transactions} row per event, keyed on its event id), and
 * reading the credit side of the balanced pair {@link
 * com.ledgerline.domain.EntryPolicy} writes for CAPTURE -- the positive
 * entry, whose magnitude equals the amount moved regardless of which account
 * happened to be debited.
 *
 * Both callers this view exists for -- the exact-match join in {@link
 * ReconciliationService} and the {@code MISSING_IN_SETTLEMENT} sweep -- read
 * the same query, so "what does the ledger say was captured" is answered in
 * exactly one place.
 */
@Component
public class CapturedLedgerView {

    private final NamedParameterJdbcTemplate jdbc;

    CapturedLedgerView(NamedParameterJdbcTemplate jdbc) {
        this.jdbc = jdbc;
    }

    /**
     * Every captured payment in {@code runId}, keyed by {@code external_txn_id}.
     *
     * Scoped by run_id (recon_batches.run_id) because that is what defines
     * which payments a settlement batch is expected to cover -- a payment
     * from a different run is out of scope for this batch's reconciliation
     * even if it happens to share an id format.
     *
     * @param runId the generator run whose captured payments to return
     */
    public List<CapturedPayment> capturedPaymentsFor(String runId) {
        return jdbc.query(
                """
                SELECT ts.external_txn_id AS external_txn_id,
                       t.merchant_id AS merchant_id,
                       e.amount AS captured_amount,
                       a.currency AS currency,
                       t.created_at AS captured_at,
                       ts.state AS state
                FROM transaction_states ts
                JOIN transactions t
                     ON t.idempotency_key = ts.external_txn_id || ':CAPTURE'
                JOIN ledger_entries e
                     ON e.transaction_id = t.id AND e.amount > 0
                JOIN accounts a
                     ON a.id = e.account_id
                WHERE ts.external_txn_id LIKE :runIdPrefix
                """,
                new MapSqlParameterSource("runIdPrefix", runId + "-%"),
                CAPTURED_PAYMENT_MAPPER);
    }

    private static final RowMapper<CapturedPayment> CAPTURED_PAYMENT_MAPPER = (ResultSet rs, int rowNum) ->
            new CapturedPayment(
                    rs.getString("external_txn_id"),
                    rs.getString("merchant_id"),
                    rs.getBigDecimal("captured_amount"),
                    rs.getString("currency").trim(),
                    rs.getString("state"),
                    rs.getTimestamp("captured_at").toInstant());

    /** One payment's captured facts, as the ledger itself would report them. */
    public record CapturedPayment(
            String externalTxnId,
            String merchantId,
            BigDecimal amount,
            String currency,
            String state,
            Instant capturedAt) {
    }
}
