package com.ledgerline.reconciliation;

import java.math.BigDecimal;
import java.sql.ResultSet;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collection;
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

    /**
     * Payments that could plausibly be the one a settlement line describes,
     * when the line's own identifier could not identify it.
     *
     * All three attributes must hold, and the amount comparison is exact --
     * pass 2 relaxes *identity*, not *arithmetic*. A tolerance on the amount
     * would let the matcher paper over an amount fault and a mangled id at
     * the same time, reporting a confident match where two separate things
     * went wrong.
     *
     * The exclusion list is the payments already claimed in this run,
     * whether by pass 1's exact match or by an earlier fuzzy match in the
     * same pass. A claimed payment is spoken for; offering it again is how a
     * one-to-one matcher turns into a many-to-one one.
     *
     * ORDER BY is explicit and total (capture time, then id) rather than
     * left to result-set order, so that "which candidate came first" is a
     * property of the data and not of the plan Postgres happened to choose.
     * Nothing downstream is allowed to *break a tie* by taking the first --
     * see ReconOutcome.AMBIGUOUS -- but the ordering still has to be stable
     * for a rerun to reach the same conclusion, and for the sabotage pass
     * that removes ambiguity refusal to report a meaningful answer about
     * which candidate a guessing matcher would have taken.
     *
     * @param runId          the generator run whose payments are in scope
     * @param amountMinor    the settlement line's gross, in minor units
     * @param merchantId     the settlement line's merchant
     * @param settledAt      the settlement line's settled_at
     * @param windowSeconds  half-width of the accepted capture-time window
     * @param excludedTxnIds payments already claimed in this run; may be empty
     */
    public List<CapturedPayment> fuzzyCandidatesFor(
            String runId, long amountMinor, String merchantId,
            Instant settledAt, int windowSeconds, Collection<String> excludedTxnIds) {

        // A never-matching sentinel keeps the NOT IN list non-empty; an empty
        // IN () list is a syntax error in Postgres, and rewriting the query
        // shape depending on whether exclusions exist would mean two query
        // plans to reason about instead of one.
        List<String> excluded = new ArrayList<>(excludedTxnIds);
        excluded.add("");

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
                  AND t.merchant_id = :merchantId
                  -- Mirrors toMinorUnits in ReconciliationService exactly:
                  -- movePointRight(2) then HALF_UP to whole cents. Postgres
                  -- ROUND on NUMERIC is half-up away from zero, matching
                  -- BigDecimal.HALF_UP for the non-negative amounts a credit
                  -- entry carries. A plain ::bigint cast would round
                  -- half-to-even instead and disagree with the Java path on
                  -- sub-cent amounts.
                  AND ROUND(e.amount * 100)::bigint = :amountMinor
                  AND t.created_at BETWEEN :windowStart AND :windowEnd
                  AND ts.external_txn_id NOT IN (:excludedTxnIds)
                ORDER BY t.created_at, ts.external_txn_id
                """,
                new MapSqlParameterSource()
                        .addValue("runIdPrefix", runId + "-%")
                        .addValue("merchantId", merchantId)
                        .addValue("amountMinor", amountMinor)
                        .addValue("windowStart", Timestamp.from(settledAt.minusSeconds(windowSeconds)))
                        .addValue("windowEnd", Timestamp.from(settledAt.plusSeconds(windowSeconds)))
                        .addValue("excludedTxnIds", excluded),
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
