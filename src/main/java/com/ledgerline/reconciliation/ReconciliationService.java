package com.ledgerline.reconciliation;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.sql.ResultSet;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

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

    /**
     * Bumped by hand whenever matching logic changes in a way that could
     * change a decision. Stored on every recon_runs row so a Day 4 sweep
     * cannot silently compare results produced by different matchers -- a
     * window sweep is only interpretable if the only thing that varied was
     * the window.
     *
     * "pass2-fuzzy" is the first value: pass 1 alone was never recorded
     * under a version, because before recon_runs existed there was no row to
     * record it on.
     */
    static final String MATCHER_VERSION = "pass2-fuzzy";

    /**
     * The window used by {@link #run(String)}. Half-width, in seconds, around
     * a settlement line's settled_at within which a payment's capture time is
     * considered plausible.
     *
     * 24 hours, chosen to sit just above the simulator's honest settlement lag
     * of 18 hours (SettlementSimulator.HONEST_SETTLEMENT_LAG) -- a genuine
     * settlement arrives about 18h after capture, so a window narrower than
     * that would reject nearly every true match on timing alone. This is a
     * default, not a claim that 24h is optimal; finding the right value is
     * exactly what Day 4's sweep is for, which is why the window is a
     * parameter of a run rather than a constant of the matcher.
     */
    public static final int DEFAULT_WINDOW_SECONDS = 24 * 60 * 60;

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
        run(batchId, DEFAULT_WINDOW_SECONDS);
    }

    /**
     * Reconciles {@code batchId} with an explicit fuzzy window.
     *
     * Resolves or creates the recon_runs row for
     * (batchId, windowSeconds, MATCHER_VERSION) first, and keys every
     * exception and outcome on it. Same parameters resolve to the same run,
     * so the ON CONFLICT DO NOTHING writes below still make a rerun a
     * provable no-op; different parameters get their own run, so two windows
     * over one batch coexist instead of colliding. That coexistence is what
     * Day 4's window sweep needs and what keying on batch_id alone
     * prevented.
     */
    @Transactional("reconTransactionManager")
    public void run(String batchId, int windowSeconds) {
        BatchInfo batch = loadBatch(batchId);
        long reconRunId = resolveOrCreateRun(batchId, windowSeconds);

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

        // Lines this run has already settled on. Empty on a first run; on a
        // rerun it holds everything both passes decided last time.
        //
        // Pass 1 skips them. Without that, a rerun re-raises the
        // MISSING_IN_LEDGER exception for a line that pass 2 has since
        // recovered and whose exception it therefore deleted -- pass 1 would
        // insert it afresh (ON CONFLICT DO NOTHING does nothing when there is
        // no conflicting row), and pass 2 would decline to clean it up a
        // second time because the line is no longer MISSING_IN_LEDGER. The
        // run would end with a stale exception the first run did not produce,
        // making the rerun observably not a no-op.
        Set<Integer> alreadyDecided = new java.util.HashSet<>(decidedLineNumbers(reconRunId));

        // Pass 1: exact matching on external_txn_id.
        for (SettlementLine line : lines) {
            if (alreadyDecided.contains(line.lineNumber())) {
                continue;
            }
            classifyAndRecord(reconRunId, batchId, line, byExternalTxnId, byExternalTxnIdLines, paymentStates);
        }

        // Pass 2: fuzzy recovery over what pass 1 could not identify.
        runFuzzyPass(reconRunId, batchId, batch.runId(), lines, windowSeconds);

        // Deliberately after pass 2, and reading the claims pass 2 recorded.
        //
        // A payment recovered fuzzily *was* settled -- by a line whose
        // identifier was unusable, which is why pass 1 could not see the
        // connection. Sweeping for missing settlements before pass 2 ran, or
        // sweeping on external_txn_id alone, would raise MISSING_IN_SETTLEMENT
        // against exactly those payments and report every successful fuzzy
        // recovery as a second, contradictory finding: identified by one part
        // of the engine and declared missing by another.
        recordMissingInSettlement(reconRunId, captured, byExternalTxnIdLines);
    }

    /**
     * Pass 2. Re-examines only the lines pass 1 left as MISSING_IN_LEDGER,
     * and upgrades those it can identify by attribute to FUZZY_MATCHED, or
     * marks them AMBIGUOUS when more than one payment fits.
     *
     * Eligibility is deliberately narrow: MISSING_IN_LEDGER only.
     *
     * A line that is AMOUNT_MISMATCH, STATE_CONFLICT, or
     * DUPLICATE_SETTLEMENT already found its payment by identifier. Its
     * problem is the amount, the payment's state, or the file's structure --
     * not identification. Re-matching those fuzzily could reassign them to a
     * *different* payment that happens to fit the attributes better, which
     * would not be a better answer; it would be the engine manufacturing a
     * more plausible-looking wrong one, and quietly erasing a real finding
     * (the amount disagreement, the void) in the process.
     */
    private void runFuzzyPass(
            long reconRunId, String batchId, String runId,
            List<SettlementLine> lines, int windowSeconds) {

        // Payments already spoken for. Seeded with everything pass 1 claimed
        // exactly, and grown as pass 2 claims more, so a payment can never be
        // handed to two lines. The database enforces this too, via the
        // partial unique index on (recon_run_id, matched_txn_id) -- this set
        // is what stops the matcher from *attempting* a double claim; the
        // index is what guarantees one never lands.
        Set<String> claimed = new java.util.HashSet<>(claimedPaymentsIn(reconRunId));

        for (SettlementLine line : eligibleForFuzzy(reconRunId, batchId, lines)) {
            List<CapturedPayment> candidates = capturedLedgerView.fuzzyCandidatesFor(
                    runId, line.grossAmountMinor(), line.merchantId(),
                    line.settledAt(), windowSeconds, claimed);

            if (candidates.isEmpty()) {
                // Nothing plausible. The line keeps pass 1's MISSING_IN_LEDGER
                // outcome and its exception; only candidate_count is recorded,
                // to distinguish "pass 2 looked and found nothing" from "pass 2
                // never looked at this line".
                updateFuzzyOutcome(reconRunId, line.lineNumber(),
                        ReconOutcome.MISSING_IN_LEDGER, null, 0, null);
                continue;
            }

            if (candidates.size() > 1) {
                // Refuse. Do not take the first, the nearest in time, or the
                // lowest id.
                //
                // Two payments that fit equally well are not a tie to be
                // broken -- they are evidence that the attributes available
                // do not identify this line, which is a fact about the data
                // and not a decision the matcher is entitled to make on the
                // reader's behalf. Guessing here would not fail loudly when
                // wrong; it would emit a confident FUZZY_MATCHED that nothing
                // downstream could distinguish from a real one, and Day 4
                // would score the guess as a true positive whenever it
                // happened to land. The candidate count is recorded so the
                // refusal carries its own evidence.
                updateFuzzyOutcome(reconRunId, line.lineNumber(),
                        ReconOutcome.AMBIGUOUS, null, candidates.size(), null);
                continue;
            }

            CapturedPayment match = candidates.get(0);
            long timeDeltaSeconds = java.time.Duration.between(
                    match.capturedAt(), line.settledAt()).getSeconds();
            updateFuzzyOutcome(reconRunId, line.lineNumber(),
                    ReconOutcome.FUZZY_MATCHED, match.externalTxnId(), 1, timeDeltaSeconds);
            claimed.add(match.externalTxnId());

            // The line is identified now, so the MISSING_IN_LEDGER exception
            // pass 1 raised for it no longer describes reality. Removing it
            // is what makes "the unmatched set shrank" true in the exception
            // table and not only in the outcome table.
            deleteExceptionFor(reconRunId, line);
        }
    }

    private void classifyAndRecord(
            long reconRunId,
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
            insertException(reconRunId, subjectKey, ReconExceptionType.MISSING_IN_LEDGER,
                    line.externalTxnId(), List.of(line.lineNumber()),
                    line.grossAmountMinor(), null, null, null);
            insertLineOutcome(reconRunId, batchId, line.lineNumber(), ReconOutcome.MISSING_IN_LEDGER, subjectKey);
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
            insertException(reconRunId, subjectKey, ReconExceptionType.DUPLICATE_SETTLEMENT,
                    line.externalTxnId(), lineNumbers,
                    line.grossAmountMinor(), ledgerAmountMinor, null, state);
            insertLineOutcome(reconRunId, batchId, line.lineNumber(), ReconOutcome.DUPLICATE_SETTLEMENT, subjectKey);
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
            insertException(reconRunId, subjectKey, ReconExceptionType.STATE_CONFLICT,
                    line.externalTxnId(), List.of(line.lineNumber()),
                    line.grossAmountMinor(), ledgerAmountMinor, null, state);
            insertLineOutcome(reconRunId, batchId, line.lineNumber(), ReconOutcome.STATE_CONFLICT, subjectKey);
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
        // settled_at minus capture time, signed the same way delta_minor is:
        // positive means settlement ran behind capture. Computed once and
        // used by both remaining branches, since both are exact matches and
        // both know the payment's capture time.
        long timeDeltaSeconds = Duration.between(payment.capturedAt(), line.settledAt()).getSeconds();
        if (deltaMinor != 0) {
            String subjectKey = line.externalTxnId();
            insertException(reconRunId, subjectKey, ReconExceptionType.AMOUNT_MISMATCH,
                    line.externalTxnId(), List.of(line.lineNumber()),
                    line.grossAmountMinor(), ledgerAmountMinor, deltaMinor, state);
            insertLineOutcome(reconRunId, batchId, line.lineNumber(),
                    ReconOutcome.AMOUNT_MISMATCH, subjectKey, timeDeltaSeconds);
            return;
        }

        // 5. MATCHED -- no exception row; only a line outcome. The subject
        // key doubles as the claimed payment id here: an exactly matched line
        // claims the payment its own external_txn_id names, and
        // insertLineOutcome stamps it into matched_txn_id so the partial
        // unique index can see the claim.
        insertLineOutcome(reconRunId, batchId, line.lineNumber(),
                ReconOutcome.MATCHED, line.externalTxnId(), timeDeltaSeconds);
    }

    /**
     * Every captured payment in the run with no settlement line in this batch
     * bearing its id. These produce no recon_line_outcomes row -- there is no
     * settlement line to attach one to, which is exactly why
     * MISSING_IN_SETTLEMENT is absent from the recon_line_outcomes outcome
     * CHECK constraint.
     */
    private void recordMissingInSettlement(
            long reconRunId, List<CapturedPayment> captured,
            Map<String, List<SettlementLine>> byExternalTxnIdLines) {

        // Payments some line claimed, by either pass. A fuzzy claim counts:
        // the payment was settled by a line whose id happened to be unusable,
        // which is not the same as not being settled at all.
        Set<String> claimed = new java.util.HashSet<>(claimedPaymentsIn(reconRunId));

        for (CapturedPayment payment : captured) {
            if (byExternalTxnIdLines.containsKey(payment.externalTxnId())
                    || claimed.contains(payment.externalTxnId())) {
                continue;
            }
            String subjectKey = payment.externalTxnId();
            insertException(reconRunId, subjectKey, ReconExceptionType.MISSING_IN_SETTLEMENT,
                    payment.externalTxnId(), List.of(),
                    null, toMinorUnits(payment.amount()), null, payment.state());
        }
    }

    /**
     * Resolves the recon_runs row for these parameters, creating it if this
     * is the first run with them.
     *
     * Same ON CONFLICT DO NOTHING shape as every other claim in this
     * codebase, for the same reason: a SELECT-then-INSERT would let two
     * concurrent reconciliations of one batch both miss and both insert.
     * The follow-up SELECT is what reads back whichever row won, and runs
     * unconditionally rather than only on conflict, so there is one code
     * path instead of two.
     */
    private long resolveOrCreateRun(String batchId, int windowSeconds) {
        MapSqlParameterSource params = new MapSqlParameterSource()
                .addValue("batchId", batchId)
                .addValue("windowSeconds", windowSeconds)
                .addValue("matcherVersion", MATCHER_VERSION);

        jdbc.update(
                "INSERT INTO recon_runs (batch_id, window_seconds, matcher_version) "
                        + "VALUES (:batchId, :windowSeconds, :matcherVersion) "
                        + "ON CONFLICT (batch_id, window_seconds, matcher_version) DO NOTHING",
                params);

        return jdbc.queryForObject(
                "SELECT recon_run_id FROM recon_runs WHERE batch_id = :batchId "
                        + "AND window_seconds = :windowSeconds AND matcher_version = :matcherVersion",
                params, Long.class);
    }

    private void insertException(
            long reconRunId, String subjectKey, ReconExceptionType type,
            String externalTxnId, List<Integer> lineNumbers,
            Long settlementAmountMinor, Long ledgerAmountMinor, Long deltaMinor, String paymentState) {

        jdbc.update(
                "INSERT INTO recon_exceptions "
                        + "(recon_run_id, subject_key, type, external_txn_id, settlement_line_numbers, "
                        + " settlement_amount_minor, ledger_amount_minor, delta_minor, payment_state) "
                        + "VALUES (:reconRunId, :subjectKey, :type, :externalTxnId, :lineNumbers, "
                        + " :settlementAmountMinor, :ledgerAmountMinor, :deltaMinor, :paymentState) "
                        // The conflict target names the same partial index
                        // V13 replaced the plain UNIQUE constraint with:
                        // Postgres only infers a conflict target from an
                        // index whose predicate matches exactly, so the
                        // WHERE clause has to be repeated here, not just on
                        // the index definition.
                        + "ON CONFLICT (recon_run_id, subject_key, type) WHERE superseded_at IS NULL "
                        + "DO NOTHING",
                new MapSqlParameterSource()
                        .addValue("reconRunId", reconRunId)
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
            long reconRunId, String batchId, int lineNumber,
            ReconOutcome outcome, String subjectKeyForException) {
        insertLineOutcome(reconRunId, batchId, lineNumber, outcome, subjectKeyForException, null);
    }

    /**
     * @param timeDeltaSeconds settled_at minus the payment's capture time,
     *                         signed, for the two outcomes that know both:
     *                         MATCHED and AMOUNT_MISMATCH. Null for anything
     *                         pass 1 could not attach a payment to -- there
     *                         is no capture time to measure against.
     *
     *                         Populated for exact matches for the same reason
     *                         it was already populated for fuzzy ones: a
     *                         settlement row that is otherwise perfect but
     *                         arrives 20 days late currently exact-matches on
     *                         id and raises no exception at all (see
     *                         LateSettlementReachabilityTest), so this column
     *                         was the only place that evidence could have
     *                         been recorded and, until now, wasn't. This does
     *                         not make lateness a classification -- it keeps
     *                         the raw fact on the row so a later Day 4
     *                         decision has data to work from instead of an
     *                         empty column.
     */
    private void insertLineOutcome(
            long reconRunId, String batchId, int lineNumber,
            ReconOutcome outcome, String subjectKeyForException, Long timeDeltaSeconds) {

        Long exceptionId = subjectKeyForException == null
                ? null
                : findExceptionId(reconRunId, subjectKeyForException, outcome);

        jdbc.update(
                "INSERT INTO recon_line_outcomes "
                        + "(recon_run_id, batch_id, line_number, outcome, exception_id, match_method, "
                        + " matched_txn_id, time_delta_seconds) "
                        + "VALUES (:reconRunId, :batchId, :lineNumber, :outcome, :exceptionId, "
                        + " :matchMethod, :matchedTxnId, :timeDeltaSeconds) "
                        + "ON CONFLICT (recon_run_id, line_number) DO NOTHING",
                new MapSqlParameterSource()
                        .addValue("reconRunId", reconRunId)
                        .addValue("batchId", batchId)
                        .addValue("lineNumber", lineNumber)
                        .addValue("outcome", outcome.name())
                        .addValue("exceptionId", exceptionId)
                        .addValue("matchMethod", outcome.matchMethod().name())
                        // Only MATCHED carries a payment claim out of pass 1;
                        // see ReconOutcome.matchMethod for why AMOUNT_MISMATCH
                        // and friends do not, despite knowing their payment.
                        .addValue("matchedTxnId",
                                outcome.matchMethod() == ReconOutcome.MatchMethod.EXACT
                                        ? subjectKeyForExactMatch(outcome, subjectKeyForException)
                                        : null)
                        .addValue("timeDeltaSeconds", timeDeltaSeconds));
    }

    /**
     * For MATCHED, the subject key is the external_txn_id, which is exactly
     * the payment claimed. MATCHED passes a null subject key (it raises no
     * exception), so the id has to come from the caller's line instead --
     * see the MATCHED branch in classifyAndRecord, which passes it
     * explicitly.
     */
    private static String subjectKeyForExactMatch(ReconOutcome outcome, String subjectKey) {
        return outcome == ReconOutcome.MATCHED ? subjectKey : null;
    }

    /**
     * Rewrites a line's outcome after pass 2 has examined it.
     *
     * An UPDATE rather than an insert: pass 1 already wrote a row for every
     * line, and the primary key means there is exactly one to move. Guarded
     * on the outcome still being MISSING_IN_LEDGER so a second run -- which
     * finds the line already FUZZY_MATCHED or AMBIGUOUS -- matches no row
     * and changes nothing, keeping the rerun a no-op by the same
     * compare-and-swap logic TransactionStateRepository.compareAndAdvance
     * uses for state transitions.
     */
    private void updateFuzzyOutcome(
            long reconRunId, int lineNumber, ReconOutcome outcome,
            String matchedTxnId, Integer candidateCount, Long timeDeltaSeconds) {

        // A fuzzy match no longer raises an exception, so its outcome row must
        // stop pointing at the MISSING_IN_LEDGER one pass 1 created (which
        // deleteExceptionFor then removes). Every other pass-2 result keeps
        // whatever exception it already had.
        String exceptionIdClause = outcome == ReconOutcome.FUZZY_MATCHED
                ? ", exception_id = NULL"
                : "";

        jdbc.update(
                "UPDATE recon_line_outcomes "
                        + "SET outcome = :outcome, match_method = :matchMethod, "
                        + "    matched_txn_id = :matchedTxnId, candidate_count = :candidateCount, "
                        + "    time_delta_seconds = :timeDeltaSeconds"
                        + exceptionIdClause
                        + " WHERE recon_run_id = :reconRunId AND line_number = :lineNumber "
                        + "  AND outcome = 'MISSING_IN_LEDGER'",
                new MapSqlParameterSource()
                        .addValue("reconRunId", reconRunId)
                        .addValue("lineNumber", lineNumber)
                        .addValue("outcome", outcome.name())
                        .addValue("matchMethod", outcome.matchMethod().name())
                        .addValue("matchedTxnId", matchedTxnId)
                        .addValue("candidateCount", candidateCount)
                        .addValue("timeDeltaSeconds", timeDeltaSeconds));
    }

    /**
     * Line numbers this run has already recorded an outcome for. Non-empty
     * only on a rerun, and what makes a rerun skip pass 1 rather than
     * re-deriving decisions it already committed to.
     */
    private List<Integer> decidedLineNumbers(long reconRunId) {
        return jdbc.queryForList(
                "SELECT line_number FROM recon_line_outcomes WHERE recon_run_id = :reconRunId",
                new MapSqlParameterSource("reconRunId", reconRunId), Integer.class);
    }

    /** Payments already claimed by some line in this run. */
    private List<String> claimedPaymentsIn(long reconRunId) {
        return jdbc.queryForList(
                "SELECT matched_txn_id FROM recon_line_outcomes "
                        + "WHERE recon_run_id = :reconRunId AND matched_txn_id IS NOT NULL",
                new MapSqlParameterSource("reconRunId", reconRunId), String.class);
    }

    /**
     * The lines pass 2 may examine, in deterministic line order.
     *
     * Read back from recon_line_outcomes rather than recomputed, so
     * eligibility is decided by what pass 1 actually recorded rather than by
     * re-deriving it and risking the two disagreeing.
     */
    private List<SettlementLine> eligibleForFuzzy(
            long reconRunId, String batchId, List<SettlementLine> lines) {

        List<Integer> eligibleLineNumbers = jdbc.queryForList(
                "SELECT line_number FROM recon_line_outcomes "
                        + "WHERE recon_run_id = :reconRunId AND outcome = 'MISSING_IN_LEDGER' "
                        + "ORDER BY line_number",
                new MapSqlParameterSource("reconRunId", reconRunId), Integer.class);

        Map<Integer, SettlementLine> byLineNumber = new HashMap<>();
        for (SettlementLine line : lines) {
            byLineNumber.put(line.lineNumber(), line);
        }

        List<SettlementLine> eligible = new ArrayList<>();
        for (Integer lineNumber : eligibleLineNumbers) {
            SettlementLine line = byLineNumber.get(lineNumber);
            if (line != null) {
                eligible.add(line);
            }
        }
        return eligible;
    }

    /**
     * Marks the MISSING_IN_LEDGER exception raised for a line pass 2 has
     * since identified as no longer active, rather than deleting it.
     *
     * "Pass 1 could not identify this row; pass 2 recovered it" is a fact
     * worth keeping, the same way this project keeps a settlement line's
     * raw_line verbatim and never updates a ledger entry in place -- evidence
     * is retired, not erased. superseded_at is what every "what's currently
     * open" query has to filter on; see V13's migration comment.
     *
     * Only when no *other* line still points at that exception: a
     * MISSING_IN_LEDGER exception is keyed on subject_key, and two lines
     * sharing one (both null-id lines resolve to different keys, but a
     * repeated unknown id does not) would otherwise mark an exception
     * inactive that still describes the remaining line.
     */
    private void deleteExceptionFor(long reconRunId, SettlementLine line) {
        jdbc.update(
                "UPDATE recon_exceptions e SET superseded_at = now() "
                        + "WHERE e.recon_run_id = :reconRunId AND e.subject_key = :subjectKey "
                        + "  AND e.type = 'MISSING_IN_LEDGER' AND e.superseded_at IS NULL "
                        + "  AND NOT EXISTS ("
                        + "      SELECT 1 FROM recon_line_outcomes o "
                        + "      WHERE o.exception_id = e.id AND o.line_number <> :lineNumber)",
                new MapSqlParameterSource()
                        .addValue("reconRunId", reconRunId)
                        .addValue("subjectKey", subjectKeyFor(line))
                        .addValue("lineNumber", line.lineNumber()));
    }

    private Long findExceptionId(long reconRunId, String subjectKey, ReconOutcome outcome) {
        ReconExceptionType type = outcome.exceptionType().orElse(null);
        if (type == null) {
            return null;
        }
        return jdbc.queryForObject(
                "SELECT id FROM recon_exceptions WHERE recon_run_id = :reconRunId "
                        + "AND subject_key = :subjectKey AND type = :type AND superseded_at IS NULL",
                new MapSqlParameterSource()
                        .addValue("reconRunId", reconRunId)
                        .addValue("subjectKey", subjectKey)
                        .addValue("type", type.name()),
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

    /**
     * ORDER BY line_number, not incidental result-set order: the classify
     * loop and pass 2's candidate claiming both walk this list, and which
     * line reaches a contested payment first must be a property of the file
     * rather than of the query plan. See
     * {@code paymentAbsorbsAtMostOneSettlementLine}.
     */
    private List<SettlementLine> loadSettlementLines(String batchId) {
        return jdbc.query(
                "SELECT line_number, external_txn_id, merchant_id, gross_amount_minor, settled_at "
                        + "FROM settlement_records WHERE batch_id = :batchId "
                        + "ORDER BY line_number",
                new MapSqlParameterSource("batchId", batchId),
                SETTLEMENT_LINE_MAPPER);
    }

    private static final RowMapper<SettlementLine> SETTLEMENT_LINE_MAPPER = (ResultSet rs, int rowNum) ->
            new SettlementLine(
                    rs.getInt("line_number"),
                    rs.getString("external_txn_id"),
                    rs.getString("merchant_id"),
                    rs.getLong("gross_amount_minor"),
                    rs.getTimestamp("settled_at").toInstant());

    private record BatchInfo(String batchId, String runId, int rowCount) {
    }

    private record SettlementLine(
            int lineNumber, String externalTxnId, String merchantId,
            long grossAmountMinor, Instant settledAt) {
    }
}
