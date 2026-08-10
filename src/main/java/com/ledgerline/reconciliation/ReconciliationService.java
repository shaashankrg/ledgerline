package com.ledgerline.reconciliation;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.sql.ResultSet;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import org.springframework.jdbc.core.RowMapper;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import com.ledgerline.reconciliation.CapturedLedgerView.CapturedPayment;

/**
 * Matcher pass 1: exact matching and exception classification.
 *
 * {@link #run(String)} is the single public entry point, and runs inside one
 * database transaction. Every settlement line in the batch gets exactly one
 * outcome, evaluated against {@link ReconOutcome} in that enum's declared
 * order -- see its Javadoc for the reasoning behind the precedence, most
 * importantly why DUPLICATE_SETTLEMENT is checked before STATE_CONFLICT and
 * AMOUNT_MISMATCH.
 *
 * This class must never read {@code faultlab}. That is enforced by Postgres,
 * not by this class's discipline: the {@link NamedParameterJdbcTemplate} it
 * is constructed with is wired (see the recon-role datasource configuration)
 * to a connection authenticated as {@code recon_role}, which V8 and V11
 * revoke all access to {@code faultlab} from. A query written here that
 * named {@code faultlab.injected_faults} would fail at execution time with a
 * permission error, not silently succeed.
 */
@Service
public class ReconciliationService {

    /**
     * States in which a payment is settleable at all. Everything else that
     * still has a transaction_states row -- NEW, AUTHORIZED, VOIDED,
     * REFUNDED, EXPIRED -- is identifiable but not something a settlement
     * row should ever arrive for, so it is STATE_CONFLICT rather than
     * MISSING_IN_LEDGER. AUTHORIZED is explicitly included in "not
     * settleable" here: money has not moved yet, so a settlement row for it
     * is exactly the same kind of conflict as one for a VOIDED payment, not
     * a different case.
     */
    private static final java.util.Set<String> SETTLEABLE_STATES = java.util.Set.of("CAPTURED", "SETTLED");

    private final NamedParameterJdbcTemplate jdbc;
    private final CapturedLedgerView capturedLedgerView;
    private final PaymentStateView paymentStateView;

    ReconciliationService(
            @ReconRoleDataSource NamedParameterJdbcTemplate jdbc,
            CapturedLedgerView capturedLedgerView,
            PaymentStateView paymentStateView) {
        this.jdbc = jdbc;
        this.capturedLedgerView = capturedLedgerView;
        this.paymentStateView = paymentStateView;
    }

    /**
     * Classifies every line of {@code batchId} and records the result.
     *
     * Idempotent by construction: every write below is {@code INSERT ...
     * ON CONFLICT DO NOTHING}, the same pattern {@code LedgerWriter.claimEvent},
     * {@code TransactionStateRepository.createIfAbsent}, and {@code
     * ParkedEventRepository.park} already use for the same reason -- a
     * read-then-write existence check is a race, and letting the batch's
     * UNIQUE constraints arbitrate closes that window entirely. A second call
     * with the same batchId finds every row it would have inserted already
     * present and inserts nothing new.
     */
    @Transactional("reconTransactionManager")
    public void run(String batchId) {
        BatchInfo batch = loadBatch(batchId);
        List<SettlementLine> lines = loadSettlementLines(batchId);
        List<CapturedPayment> captured = capturedLedgerView.capturedPaymentsFor(batch.runId());
        Map<String, String> paymentStates = paymentStateView.statesFor(batch.runId());

        Map<String, CapturedPayment> byExternalTxnId = new HashMap<>();
        for (CapturedPayment payment : captured) {
            byExternalTxnId.put(payment.externalTxnId(), payment);
        }

        Map<String, List<SettlementLine>> byExternalTxnIdLines = new HashMap<>();
        for (SettlementLine line : lines) {
            if (line.externalTxnId() != null) {
                byExternalTxnIdLines
                        .computeIfAbsent(line.externalTxnId(), k -> new ArrayList<>())
                        .add(line);
            }
        }

        for (SettlementLine line : lines) {
            classifyAndRecord(batchId, line, byExternalTxnId, byExternalTxnIdLines, paymentStates);
        }

        recordMissingInSettlement(batchId, captured, byExternalTxnIdLines);
    }

    private void classifyAndRecord(
            String batchId,
            SettlementLine line,
            Map<String, CapturedPayment> byExternalTxnId,
            Map<String, List<SettlementLine>> byExternalTxnIdLines,
            Map<String, String> paymentStates) {

        String state = line.externalTxnId() == null ? null : paymentStates.get(line.externalTxnId());

        // 1. MISSING_IN_LEDGER -- no id, or the id names no payment we have
        // ever heard of at all (absent from transaction_states entirely).
        // This now means exactly one thing: the payment cannot be
        // identified. A payment that exists but was never captured is a
        // STATE_CONFLICT (step 3 below), not this.
        if (line.externalTxnId() == null || state == null) {
            String subjectKey = subjectKeyFor(line);
            insertException(batchId, subjectKey, ReconExceptionType.MISSING_IN_LEDGER,
                    line.externalTxnId(), List.of(line.lineNumber()),
                    line.grossAmountMinor(), null, null, null);
            insertLineOutcome(batchId, line.lineNumber(), ReconOutcome.MISSING_IN_LEDGER, subjectKey);
            return;
        }

        CapturedPayment payment = byExternalTxnId.get(line.externalTxnId());

        // 2. DUPLICATE_SETTLEMENT -- this external_txn_id appears on more than
        // one line in the batch. Checked above STATE_CONFLICT and
        // AMOUNT_MISMATCH: see ReconOutcome for why.
        List<SettlementLine> siblings = byExternalTxnIdLines.get(line.externalTxnId());
        if (siblings != null && siblings.size() > 1) {
            String subjectKey = line.externalTxnId();
            List<Integer> lineNumbers = siblings.stream().map(SettlementLine::lineNumber)
                    .sorted().toList();
            Long ledgerAmountMinor = payment == null ? null : toMinorUnits(payment.amount());
            insertException(batchId, subjectKey, ReconExceptionType.DUPLICATE_SETTLEMENT,
                    line.externalTxnId(), lineNumbers,
                    line.grossAmountMinor(), ledgerAmountMinor, null, state);
            insertLineOutcome(batchId, line.lineNumber(), ReconOutcome.DUPLICATE_SETTLEMENT, subjectKey);
            return;
        }

        // 3. STATE_CONFLICT -- the payment is identifiable but is not in a
        // state where settlement is expected. Only CAPTURED and SETTLED are
        // settleable; every other state a payment can be identified in --
        // NEW, AUTHORIZED, VOIDED, REFUNDED, EXPIRED -- lands here, with the
        // real state recorded so Day 4 can subdivide without a schema
        // change. AUTHORIZED is deliberately included: money has not moved,
        // so a settlement row for it is the same kind of conflict as one for
        // a VOIDED payment, not a different case requiring different
        // handling.
        if (!SETTLEABLE_STATES.contains(state)) {
            String subjectKey = line.externalTxnId();
            Long ledgerAmountMinor = payment == null ? null : toMinorUnits(payment.amount());
            insertException(batchId, subjectKey, ReconExceptionType.STATE_CONFLICT,
                    line.externalTxnId(), List.of(line.lineNumber()),
                    line.grossAmountMinor(), ledgerAmountMinor, null, state);
            insertLineOutcome(batchId, line.lineNumber(), ReconOutcome.STATE_CONFLICT, subjectKey);
            return;
        }

        // A settleable state (CAPTURED/SETTLED) implies a CapturedLedgerView
        // row must exist -- every CAPTURE writes both a transaction_states
        // transition and the entry pair CapturedLedgerView reads, in the
        // same database transaction (TransactionEventService.apply). If this
        // were ever null here, that atomicity guarantee would have been
        // violated, which is a defect in the ledger write path, not
        // something for this classifier to paper over.
        if (payment == null) {
            throw new IllegalStateException(
                    "payment " + line.externalTxnId() + " is " + state
                            + " but has no CapturedLedgerView row -- ledger write path invariant violated");
        }

        // 4. AMOUNT_MISMATCH -- gross_amount_minor vs. the ledger's captured
        // amount. fee_minor is carried on the settlement row but deliberately
        // excluded here: the file reports gross with fee broken out
        // separately, and the ledger's captured amount is the gross charge,
        // not gross-minus-fee.
        long ledgerAmountMinor = toMinorUnits(payment.amount());
        // Signed as settlement minus ledger: positive means the network
        // reported more than we captured, negative means less. Direction, not
        // just magnitude, is asserted in ReconciliationServiceTest.
        long deltaMinor = line.grossAmountMinor() - ledgerAmountMinor;
        if (deltaMinor != 0) {
            String subjectKey = line.externalTxnId();
            insertException(batchId, subjectKey, ReconExceptionType.AMOUNT_MISMATCH,
                    line.externalTxnId(), List.of(line.lineNumber()),
                    line.grossAmountMinor(), ledgerAmountMinor, deltaMinor, state);
            insertLineOutcome(batchId, line.lineNumber(), ReconOutcome.AMOUNT_MISMATCH, subjectKey);
            return;
        }

        // 5. MATCHED -- no exception row; only a line outcome.
        insertLineOutcome(batchId, line.lineNumber(), ReconOutcome.MATCHED, null);
    }

    /**
     * Every captured payment in the run with no settlement line in this batch
     * bearing its id. These produce no recon_line_outcomes row -- there is no
     * settlement line to attach one to, which is exactly why
     * MISSING_IN_SETTLEMENT is absent from the recon_line_outcomes outcome
     * CHECK constraint.
     */
    private void recordMissingInSettlement(
            String batchId, List<CapturedPayment> captured,
            Map<String, List<SettlementLine>> byExternalTxnIdLines) {

        for (CapturedPayment payment : captured) {
            if (byExternalTxnIdLines.containsKey(payment.externalTxnId())) {
                continue;
            }
            String subjectKey = payment.externalTxnId();
            insertException(batchId, subjectKey, ReconExceptionType.MISSING_IN_SETTLEMENT,
                    payment.externalTxnId(), List.of(),
                    null, toMinorUnits(payment.amount()), null, payment.state());
        }
    }

    private void insertException(
            String batchId, String subjectKey, ReconExceptionType type,
            String externalTxnId, List<Integer> lineNumbers,
            Long settlementAmountMinor, Long ledgerAmountMinor, Long deltaMinor, String paymentState) {

        jdbc.update(
                "INSERT INTO recon_exceptions "
                        + "(batch_id, subject_key, type, external_txn_id, settlement_line_numbers, "
                        + " settlement_amount_minor, ledger_amount_minor, delta_minor, payment_state) "
                        + "VALUES (:batchId, :subjectKey, :type, :externalTxnId, :lineNumbers, "
                        + " :settlementAmountMinor, :ledgerAmountMinor, :deltaMinor, :paymentState) "
                        + "ON CONFLICT (batch_id, subject_key, type) DO NOTHING",
                new MapSqlParameterSource()
                        .addValue("batchId", batchId)
                        .addValue("subjectKey", subjectKey)
                        .addValue("type", type.name())
                        .addValue("externalTxnId", externalTxnId)
                        .addValue("lineNumbers", lineNumbers.stream().mapToInt(Integer::intValue).toArray())
                        .addValue("settlementAmountMinor", settlementAmountMinor)
                        .addValue("ledgerAmountMinor", ledgerAmountMinor)
                        .addValue("deltaMinor", deltaMinor)
                        .addValue("paymentState", paymentState));
    }

    private void insertLineOutcome(
            String batchId, int lineNumber, ReconOutcome outcome, String subjectKeyForException) {

        Long exceptionId = subjectKeyForException == null
                ? null
                : findExceptionId(batchId, subjectKeyForException, outcome);

        jdbc.update(
                "INSERT INTO recon_line_outcomes (batch_id, line_number, outcome, exception_id) "
                        + "VALUES (:batchId, :lineNumber, :outcome, :exceptionId) "
                        + "ON CONFLICT (batch_id, line_number) DO NOTHING",
                new MapSqlParameterSource()
                        .addValue("batchId", batchId)
                        .addValue("lineNumber", lineNumber)
                        .addValue("outcome", outcome.name())
                        .addValue("exceptionId", exceptionId));
    }

    private Long findExceptionId(String batchId, String subjectKey, ReconOutcome outcome) {
        return jdbc.queryForObject(
                "SELECT id FROM recon_exceptions WHERE batch_id = :batchId "
                        + "AND subject_key = :subjectKey AND type = :type",
                new MapSqlParameterSource()
                        .addValue("batchId", batchId)
                        .addValue("subjectKey", subjectKey)
                        .addValue("type", outcome.asExceptionType().name()),
                Long.class);
    }

    /**
     * subject_key for a settlement-sourced exception: the external_txn_id
     * when present, or 'line:' + line_number when it is not. See the V11
     * migration comment for why this, rather than external_txn_id alone, is
     * the dedup key.
     */
    private static String subjectKeyFor(SettlementLine line) {
        return line.externalTxnId() != null ? line.externalTxnId() : "line:" + line.lineNumber();
    }

    private static long toMinorUnits(BigDecimal amount) {
        return amount.movePointRight(2).setScale(0, RoundingMode.HALF_UP).longValueExact();
    }

    private BatchInfo loadBatch(String batchId) {
        return jdbc.queryForObject(
                "SELECT batch_id, run_id, row_count FROM recon_batches WHERE batch_id = :batchId",
                new MapSqlParameterSource("batchId", batchId),
                (rs, rowNum) -> new BatchInfo(
                        rs.getString("batch_id"), rs.getString("run_id"), rs.getInt("row_count")));
    }

    private List<SettlementLine> loadSettlementLines(String batchId) {
        return jdbc.query(
                "SELECT line_number, external_txn_id, gross_amount_minor "
                        + "FROM settlement_records WHERE batch_id = :batchId",
                new MapSqlParameterSource("batchId", batchId),
                SETTLEMENT_LINE_MAPPER);
    }

    private static final RowMapper<SettlementLine> SETTLEMENT_LINE_MAPPER = (ResultSet rs, int rowNum) ->
            new SettlementLine(
                    rs.getInt("line_number"),
                    rs.getString("external_txn_id"),
                    rs.getLong("gross_amount_minor"));

    private record BatchInfo(String batchId, String runId, int rowCount) {
    }

    private record SettlementLine(int lineNumber, String externalTxnId, long grossAmountMinor) {
    }
}
