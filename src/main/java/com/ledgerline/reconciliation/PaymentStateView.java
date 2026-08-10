package com.ledgerline.reconciliation;

import java.sql.ResultSet;
import java.util.List;
import java.util.Map;

import org.springframework.jdbc.core.RowMapper;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.stereotype.Component;

/**
 * Whether a payment is known at all, and if so what state it's in --
 * independent of whether it was ever captured.
 *
 * Exists to separate two questions {@link CapturedLedgerView} cannot answer
 * on its own, because it only contains payments that reached CAPTURED:
 * "does this payment exist" and "was it captured." A payment voided from
 * AUTHORIZED exists -- it has a transaction_states row -- but never appears
 * in CapturedLedgerView, since it never captured. Testing presence in
 * CapturedLedgerView alone cannot tell "this payment was voided before
 * capture" apart from "this id names no payment we have ever heard of";
 * both come back empty. This view answers "does it exist" directly against
 * transaction_states, the repo's authoritative per-payment state store
 * (see V5's migration comment), so that question no longer has to be
 * inferred from a capture-derived view's absence.
 *
 * Sourced from transaction_states rather than transactions: transactions is
 * the per-*message* idempotency ledger (one row per accepted event, several
 * per payment over its lifecycle); transaction_states is one row per
 * payment, which is the granularity this lookup needs.
 *
 * Scoping: transaction_states carries no run_id column and no foreign key to
 * any run or batch table -- it is keyed purely on external_txn_id. The only
 * available run-scoping mechanism is the same one {@link CapturedLedgerView}
 * already relies on: TransactionGenerator constructs every id as {@code
 * runId + "-txn-" + index} (see TransactionGenerator.planTransaction), so a
 * LIKE prefix match on that convention is what "scoped by run" means here.
 * This is a naming convention, not a structural guarantee the schema
 * enforces -- reported explicitly rather than presented as a join, per the
 * fix spec's instruction not to invent one where none exists.
 */
@Component
public class PaymentStateView {

    private final NamedParameterJdbcTemplate jdbc;

    PaymentStateView(NamedParameterJdbcTemplate jdbc) {
        this.jdbc = jdbc;
    }

    /**
     * Every payment known in {@code runId}, regardless of state, keyed by
     * {@code external_txn_id}. A payment absent from this map is a payment
     * this run never heard of at all -- the only case that is genuinely
     * MISSING_IN_LEDGER after this fix.
     */
    public Map<String, String> statesFor(String runId) {
        List<PaymentState> rows = jdbc.query(
                "SELECT external_txn_id, state FROM transaction_states WHERE external_txn_id LIKE :runIdPrefix",
                new MapSqlParameterSource("runIdPrefix", runId + "-%"),
                PAYMENT_STATE_MAPPER);
        return rows.stream().collect(
                java.util.stream.Collectors.toMap(PaymentState::externalTxnId, PaymentState::state));
    }

    private static final RowMapper<PaymentState> PAYMENT_STATE_MAPPER = (ResultSet rs, int rowNum) ->
            new PaymentState(rs.getString("external_txn_id"), rs.getString("state"));

    private record PaymentState(String externalTxnId, String state) {
    }
}
