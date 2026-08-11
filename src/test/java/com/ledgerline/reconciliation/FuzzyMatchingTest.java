package com.ledgerline.reconciliation;

import static org.assertj.core.api.Assertions.assertThat;

import java.math.BigDecimal;
import java.sql.Timestamp;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;

import com.ledgerline.AbstractPostgresTest;
import com.ledgerline.domain.EventType;
import com.ledgerline.domain.TransactionEvent;
import com.ledgerline.reconciliation.ReconciliationAudit.AuditResult;
import com.ledgerline.transfer.TransactionEventService;

/**
 * Matcher pass 2 -- fuzzy recovery -- against real Postgres.
 *
 * Capture time is {@code transactions.created_at}, which the ledger writes as
 * {@code now()} at insert; it is not the event's {@code occurredAt}. Tests
 * that care about the time window therefore backdate {@code created_at}
 * explicitly after capturing, rather than trying to steer it through the
 * event. Doing it that way keeps the window arithmetic exact instead of
 * relative to whenever the suite happened to run, which is what makes
 * {@code candidateOutsideWindowIsNotMatched} a real boundary test rather than
 * a coincidence.
 */
class FuzzyMatchingTest extends AbstractPostgresTest {

    @Autowired
    private TransactionEventService eventService;

    @Autowired
    private ReconciliationService reconciliationService;

    @Autowired
    private ReconciliationAudit audit;

    @Autowired
    private JdbcTemplate jdbc;

    /** Capture time every fixture payment is pinned to unless stated otherwise. */
    private static final Instant CAPTURED_AT = Instant.parse("2026-01-15T06:00:00Z");
    /** 18h after capture -- the simulator's honest settlement lag. */
    private static final Instant SETTLED_AT = CAPTURED_AT.plus(Duration.ofHours(18));

    private long alice;
    private long merchant;

    @BeforeEach
    void setUp() {
        jdbc.update("DELETE FROM recon_line_outcomes");
        jdbc.update("DELETE FROM recon_exceptions");
        jdbc.update("DELETE FROM recon_runs");
        jdbc.update("DELETE FROM settlement_records");
        jdbc.update("DELETE FROM recon_batches");
        jdbc.update("DELETE FROM ledger_entries");
        jdbc.update("DELETE FROM transactions");
        jdbc.update("DELETE FROM transaction_states");
        jdbc.update("DELETE FROM parked_events");

        alice = jdbc.queryForObject("SELECT id FROM accounts WHERE name = 'Alice Checking'", Long.class);
        merchant = jdbc.queryForObject("SELECT id FROM accounts WHERE name = 'Merchant Revenue'", Long.class);
    }

    // ---- Fixtures --------------------------------------------------------

    private String runId() {
        return "run-" + UUID.randomUUID();
    }

    private String batchId() {
        return "batch-" + UUID.randomUUID();
    }

    /**
     * Captures a payment and pins its capture time to {@code capturedAt}.
     *
     * The backdate is a direct UPDATE on transactions.created_at rather than
     * anything routed through the event path, because the event path has no
     * way to set it -- created_at is a database default, deliberately, so the
     * ledger records when it actually wrote rather than when a caller claimed
     * something happened.
     */
    private void captureAt(String externalTxnId, BigDecimal amountMajor,
            String merchantId, Instant capturedAt) {
        eventService.apply(new TransactionEvent(
                externalTxnId, externalTxnId + ":AUTHORIZE", EventType.AUTHORIZE,
                null, null, null, null, capturedAt, merchantId));
        eventService.apply(new TransactionEvent(
                externalTxnId, externalTxnId + ":CAPTURE", EventType.CAPTURE,
                alice, merchant, amountMajor, "USD", capturedAt, merchantId));

        jdbc.update("UPDATE transactions SET created_at = ? WHERE idempotency_key = ?",
                Timestamp.from(capturedAt), externalTxnId + ":CAPTURE");
    }

    private void insertBatch(String batchId, String runId, int rowCount) {
        jdbc.update(
                "INSERT INTO recon_batches (batch_id, run_id, seed, base_instant, row_count, file_sha256) "
                        + "VALUES (?, ?, 1, ?, ?, 'deadbeef')",
                batchId, runId, Timestamp.from(CAPTURED_AT), rowCount);
    }

    private void insertLine(String batchId, int lineNumber, String externalTxnId,
            String merchantId, long grossAmountMinor, Instant settledAt) {
        jdbc.update(
                "INSERT INTO settlement_records "
                        + "(batch_id, line_number, external_txn_id, merchant_id, gross_amount_minor, "
                        + " fee_minor, currency, settled_at, raw_line) "
                        + "VALUES (?, ?, ?, ?, ?, 0, 'USD', ?, 'raw')",
                batchId, lineNumber, externalTxnId, merchantId, grossAmountMinor,
                Timestamp.from(settledAt));
    }

    private long runIdOf(String batchId, int windowSeconds) {
        return jdbc.queryForObject(
                "SELECT recon_run_id FROM recon_runs WHERE batch_id = ? AND window_seconds = ?",
                Long.class, batchId, windowSeconds);
    }

    private Map<String, Object> outcomeRow(String batchId, int windowSeconds, int lineNumber) {
        return jdbc.queryForMap(
                "SELECT * FROM recon_line_outcomes WHERE recon_run_id = ? AND line_number = ?",
                runIdOf(batchId, windowSeconds), lineNumber);
    }

    private Map<String, Object> outcomeRow(String batchId, int lineNumber) {
        return outcomeRow(batchId, ReconciliationService.DEFAULT_WINDOW_SECONDS, lineNumber);
    }

    private int countOutcome(String batchId, int windowSeconds, String outcome) {
        return jdbc.queryForObject(
                "SELECT count(*) FROM recon_line_outcomes WHERE recon_run_id = ? AND outcome = ?",
                Integer.class, runIdOf(batchId, windowSeconds), outcome);
    }

    // ---- Behavioural -----------------------------------------------------

    @Test
    void fuzzyRecoversMangledIdWithinWindow() {
        String runId = runId();
        String batchId = batchId();
        captureAt(runId + "-txn-1", new BigDecimal("50.00"), "merch-1", CAPTURED_AT);

        insertBatch(batchId, runId, 1);
        // Id absent -- the line cannot be identified by identifier, but the
        // amount, merchant, and settlement time all point at one payment.
        insertLine(batchId, 1, null, "merch-1", 5000, SETTLED_AT);

        reconciliationService.run(batchId);

        Map<String, Object> row = outcomeRow(batchId, 1);
        assertThat(row.get("outcome")).isEqualTo("FUZZY_MATCHED");
        assertThat(row.get("matched_txn_id")).isEqualTo(runId + "-txn-1");
        assertThat(row.get("match_method")).isEqualTo("FUZZY");
        assertThat(row.get("candidate_count")).isEqualTo(1);
        // 18h, positive: the network settled after we captured.
        assertThat(((Number) row.get("time_delta_seconds")).longValue())
                .isEqualTo(Duration.ofHours(18).toSeconds());

        // The MISSING_IN_LEDGER exception pass 1 raised is superseded, not
        // deleted -- "pass 1 could not identify this row; pass 2 recovered
        // it" is evidence this project keeps, the same way raw_line and
        // ledger entries are never destroyed once written. No *active*
        // exception remains for this subject, but the row itself is still
        // there with superseded_at set.
        long reconRunId = runIdOf(batchId, ReconciliationService.DEFAULT_WINDOW_SECONDS);
        assertThat(jdbc.queryForObject(
                "SELECT count(*) FROM recon_exceptions WHERE recon_run_id = ? AND superseded_at IS NULL",
                Integer.class, reconRunId))
                .as("no exception should still be active for a recovered line")
                .isZero();
        Map<String, Object> supersededException = jdbc.queryForMap(
                "SELECT * FROM recon_exceptions WHERE recon_run_id = ?", reconRunId);
        assertThat(supersededException.get("type")).isEqualTo("MISSING_IN_LEDGER");
        assertThat(supersededException.get("superseded_at")).isNotNull();
    }

    @Test
    void ambiguousCandidatesAreRefusedNotGuessed() {
        String runId = runId();
        String batchId = batchId();
        // Two payments identical on every attribute pass 2 can see.
        captureAt(runId + "-txn-1", new BigDecimal("50.00"), "merch-1", CAPTURED_AT);
        captureAt(runId + "-txn-2", new BigDecimal("50.00"), "merch-1", CAPTURED_AT);

        insertBatch(batchId, runId, 1);
        insertLine(batchId, 1, null, "merch-1", 5000, SETTLED_AT);

        reconciliationService.run(batchId);

        Map<String, Object> row = outcomeRow(batchId, 1);
        // Printed so the sabotage pass that removes ambiguity refusal can
        // report *which* candidate a guessing matcher would have taken --
        // that is what says whether the ordering was deterministic or
        // incidental. Candidates here are txn-1 and txn-2, identical on every
        // attribute, so only the ORDER BY separates them.
        System.out.println("[ambiguousCandidatesAreRefusedNotGuessed] outcome=" + row.get("outcome")
                + " matched_txn_id=" + row.get("matched_txn_id")
                + " (candidates were " + runId + "-txn-1 and " + runId + "-txn-2)");
        assertThat(row.get("outcome")).isEqualTo("AMBIGUOUS");
        assertThat(row.get("candidate_count")).isEqualTo(2);

        // The point of the test: no match was recorded. Asserting only that
        // candidate_count is 2 would pass even if the matcher had also
        // stamped a guess into matched_txn_id.
        assertThat(row.get("matched_txn_id")).isNull();
        assertThat(row.get("match_method")).isEqualTo("NONE");
        assertThat(row.get("time_delta_seconds")).isNull();

        // Still unmatched, so it still raises MISSING_IN_LEDGER -- what
        // distinguishes it from an ordinary miss is candidate_count, not a
        // separate exception type.
        assertThat(jdbc.queryForList(
                "SELECT type FROM recon_exceptions WHERE recon_run_id = ? AND subject_key = 'line:1'",
                String.class, runIdOf(batchId, ReconciliationService.DEFAULT_WINDOW_SECONDS)))
                .containsExactly("MISSING_IN_LEDGER");

        // Both candidate payments remain unsettled, correctly: the matcher
        // refused to say either of them was the one this line settled, so
        // neither has been accounted for by any line.
        assertThat(jdbc.queryForList(
                "SELECT subject_key FROM recon_exceptions WHERE recon_run_id = ? "
                        + "AND type = 'MISSING_IN_SETTLEMENT' ORDER BY subject_key",
                String.class, runIdOf(batchId, ReconciliationService.DEFAULT_WINDOW_SECONDS)))
                .containsExactly(runId + "-txn-1", runId + "-txn-2");
    }

    @Test
    void candidateOutsideWindowIsNotMatched() {
        String runId = runId();
        String batchId = batchId();
        captureAt(runId + "-txn-1", new BigDecimal("50.00"), "merch-1", CAPTURED_AT);

        insertBatch(batchId, runId, 1);
        // One second beyond the default 24h window, and otherwise a perfect
        // fit -- so the only thing that can reject it is the window.
        Instant justOutside = CAPTURED_AT
                .plusSeconds(ReconciliationService.DEFAULT_WINDOW_SECONDS + 1);
        insertLine(batchId, 1, null, "merch-1", 5000, justOutside);

        reconciliationService.run(batchId);

        Map<String, Object> row = outcomeRow(batchId, 1);
        assertThat(row.get("outcome")).isEqualTo("MISSING_IN_LEDGER");
        assertThat(row.get("matched_txn_id")).isNull();
        assertThat(row.get("candidate_count")).isEqualTo(0);
    }

    @Test
    void alreadyExactMatchedPaymentIsNotAFuzzyCandidate() {
        String runId = runId();
        String batchId = batchId();
        captureAt(runId + "-txn-1", new BigDecimal("50.00"), "merch-1", CAPTURED_AT);

        insertBatch(batchId, runId, 2);
        // Line 1 claims the payment by id in pass 1.
        insertLine(batchId, 1, runId + "-txn-1", "merch-1", 5000, SETTLED_AT);
        // Line 2 would fit the same payment perfectly on attributes.
        insertLine(batchId, 2, null, "merch-1", 5000, SETTLED_AT);

        reconciliationService.run(batchId);

        assertThat(outcomeRow(batchId, 1).get("outcome")).isEqualTo("MATCHED");

        Map<String, Object> second = outcomeRow(batchId, 2);
        assertThat(second.get("outcome")).isEqualTo("MISSING_IN_LEDGER");
        assertThat(second.get("matched_txn_id")).isNull();
        // Zero candidates, not one-that-was-rejected: the payment is excluded
        // from the pool outright because it is already spoken for.
        assertThat(second.get("candidate_count")).isEqualTo(0);
    }

    @Test
    void paymentAbsorbsAtMostOneSettlementLine() {
        String runId = runId();
        String batchId = batchId();
        captureAt(runId + "-txn-1", new BigDecimal("50.00"), "merch-1", CAPTURED_AT);

        insertBatch(batchId, runId, 2);
        // Two eligible lines, one candidate payment between them.
        insertLine(batchId, 1, null, "merch-1", 5000, SETTLED_AT);
        insertLine(batchId, 2, null, "merch-1", 5000, SETTLED_AT);

        reconciliationService.run(batchId);

        Map<String, Object> first = outcomeRow(batchId, 1);
        Map<String, Object> second = outcomeRow(batchId, 2);

        // Line 1 wins, deterministically: loadSettlementLines orders by
        // line_number, so pass 2 examines lines in file order and the earlier
        // line reaches the contested payment first. Nothing about this is
        // incidental to the query plan.
        assertThat(first.get("outcome")).isEqualTo("FUZZY_MATCHED");
        assertThat(first.get("matched_txn_id")).isEqualTo(runId + "-txn-1");

        // Line 2 finds the payment already claimed and excluded from its pool.
        assertThat(second.get("outcome")).isEqualTo("MISSING_IN_LEDGER");
        assertThat(second.get("matched_txn_id")).isNull();
        assertThat(second.get("candidate_count")).isEqualTo(0);

        // And the database agrees no payment was claimed twice.
        assertThat(audit.auditRun(runIdOf(batchId, ReconciliationService.DEFAULT_WINDOW_SECONDS))
                .problems()).isEmpty();
    }

    @Test
    void amountMismatchLineIsNotReMatchedFuzzily() {
        String runId = runId();
        String batchId = batchId();
        // The line's own payment, disagreeing on amount.
        captureAt(runId + "-txn-1", new BigDecimal("50.00"), "merch-1", CAPTURED_AT);
        // A decoy that fits the settlement line's amount exactly, and would
        // be a tempting fuzzy match if the line were eligible for one.
        captureAt(runId + "-txn-2", new BigDecimal("55.00"), "merch-1", CAPTURED_AT);

        insertBatch(batchId, runId, 1);
        insertLine(batchId, 1, runId + "-txn-1", "merch-1", 5500, SETTLED_AT);

        reconciliationService.run(batchId);

        Map<String, Object> row = outcomeRow(batchId, 1);
        // Stays an amount problem. Re-matching it to txn-2 would have
        // produced a tidier-looking result that erased the real finding.
        assertThat(row.get("outcome")).isEqualTo("AMOUNT_MISMATCH");
        assertThat(row.get("matched_txn_id")).isNull();
        assertThat(row.get("candidate_count")).isNull();

        assertThat(jdbc.queryForList(
                "SELECT type FROM recon_exceptions WHERE recon_run_id = ? AND subject_key = ?",
                String.class, runIdOf(batchId, ReconciliationService.DEFAULT_WINDOW_SECONDS),
                runId + "-txn-1"))
                .containsExactly("AMOUNT_MISMATCH");
    }

    @Test
    void unmatchedSetShrinksVersusPassOne() {
        String runId = runId();
        String batchId = batchId();
        // Four payments; three of their settlement lines have lost their id
        // and are recoverable by attribute, one line names a payment that
        // does not exist at all and is not recoverable by anything.
        captureAt(runId + "-txn-1", new BigDecimal("10.00"), "merch-1", CAPTURED_AT);
        captureAt(runId + "-txn-2", new BigDecimal("20.00"), "merch-2", CAPTURED_AT);
        captureAt(runId + "-txn-3", new BigDecimal("30.00"), "merch-3", CAPTURED_AT);
        captureAt(runId + "-txn-4", new BigDecimal("40.00"), "merch-4", CAPTURED_AT);

        insertBatch(batchId, runId, 5);
        insertLine(batchId, 1, null, "merch-1", 1000, SETTLED_AT);
        insertLine(batchId, 2, null, "merch-2", 2000, SETTLED_AT);
        insertLine(batchId, 3, null, "merch-3", 3000, SETTLED_AT);
        insertLine(batchId, 4, runId + "-txn-4", "merch-4", 4000, SETTLED_AT);
        insertLine(batchId, 5, runId + "-txn-nonexistent", "merch-9", 9900, SETTLED_AT);

        // Pass 1 alone: window 0 admits no candidate whose capture time is not
        // exactly the settlement instant, which none are, so nothing is
        // fuzzily recoverable and the result is pass 1's unmatched set.
        reconciliationService.run(batchId, 0);
        int unmatchedAfterPassOne = countOutcome(batchId, 0, "MISSING_IN_LEDGER");

        reconciliationService.run(batchId, ReconciliationService.DEFAULT_WINDOW_SECONDS);
        int unmatchedAfterPassTwo = countOutcome(
                batchId, ReconciliationService.DEFAULT_WINDOW_SECONDS, "MISSING_IN_LEDGER");
        int recovered = countOutcome(
                batchId, ReconciliationService.DEFAULT_WINDOW_SECONDS, "FUZZY_MATCHED");

        System.out.println("[unmatchedSetShrinksVersusPassOne] "
                + "MISSING_IN_LEDGER after pass 1 = " + unmatchedAfterPassOne
                + ", after pass 2 = " + unmatchedAfterPassTwo
                + ", FUZZY_MATCHED = " + recovered);

        assertThat(unmatchedAfterPassOne).isEqualTo(4);
        assertThat(unmatchedAfterPassTwo).isLessThan(unmatchedAfterPassOne);
        assertThat(unmatchedAfterPassTwo).isEqualTo(1);
        assertThat(recovered).isEqualTo(3);
    }

    // ---- Structural ------------------------------------------------------

    @Test
    void differentWindowsCoexistOnSameBatch() {
        String runId = runId();
        String batchId = batchId();
        captureAt(runId + "-txn-1", new BigDecimal("50.00"), "merch-1", CAPTURED_AT);

        insertBatch(batchId, runId, 1);
        insertLine(batchId, 1, null, "merch-1", 5000, SETTLED_AT);

        int narrow = 60;                 // too tight to admit an 18h gap
        int wide = 24 * 60 * 60;         // admits it

        reconciliationService.run(batchId, narrow);
        reconciliationService.run(batchId, wide);

        List<Map<String, Object>> runs = jdbc.queryForList(
                "SELECT recon_run_id, window_seconds FROM recon_runs WHERE batch_id = ? "
                        + "ORDER BY window_seconds", batchId);
        assertThat(runs).hasSize(2);
        assertThat(runs.get(0).get("window_seconds")).isEqualTo(narrow);
        assertThat(runs.get(1).get("window_seconds")).isEqualTo(wide);

        // Two independent results over one batch, neither overwriting the
        // other. This is what Day 4's window sweep needs and what keying on
        // batch_id alone made impossible.
        assertThat(outcomeRow(batchId, narrow, 1).get("outcome")).isEqualTo("MISSING_IN_LEDGER");
        assertThat(outcomeRow(batchId, wide, 1).get("outcome")).isEqualTo("FUZZY_MATCHED");
    }

    @Test
    void rerunWithSameWindowIsANoOp() {
        String runId = runId();
        String batchId = batchId();
        captureAt(runId + "-txn-1", new BigDecimal("50.00"), "merch-1", CAPTURED_AT);
        captureAt(runId + "-txn-2", new BigDecimal("70.00"), "merch-2", CAPTURED_AT);

        insertBatch(batchId, runId, 2);
        insertLine(batchId, 1, null, "merch-1", 5000, SETTLED_AT);
        insertLine(batchId, 2, runId + "-txn-2", "merch-2", 7000, SETTLED_AT);

        reconciliationService.run(batchId);
        long firstRunId = runIdOf(batchId, ReconciliationService.DEFAULT_WINDOW_SECONDS);
        List<Map<String, Object>> outcomesFirst = outcomesOf(firstRunId);
        List<Map<String, Object>> exceptionsFirst = exceptionsOf(firstRunId);

        reconciliationService.run(batchId);
        long secondRunId = runIdOf(batchId, ReconciliationService.DEFAULT_WINDOW_SECONDS);

        assertThat(secondRunId).isEqualTo(firstRunId);
        // Content, not counts: a count comparison passes even if every field
        // changed underneath it.
        assertThat(outcomesOf(secondRunId)).isEqualTo(outcomesFirst);
        assertThat(exceptionsOf(secondRunId)).isEqualTo(exceptionsFirst);
        assertThat(jdbc.queryForObject(
                "SELECT count(*) FROM recon_runs WHERE batch_id = ?", Integer.class, batchId))
                .isEqualTo(1);
    }

    private List<Map<String, Object>> outcomesOf(long reconRunId) {
        return jdbc.queryForList(
                "SELECT recon_run_id, line_number, outcome, match_method, matched_txn_id, "
                        + "candidate_count, time_delta_seconds "
                        + "FROM recon_line_outcomes WHERE recon_run_id = ? ORDER BY line_number",
                reconRunId);
    }

    private List<Map<String, Object>> exceptionsOf(long reconRunId) {
        return jdbc.queryForList(
                "SELECT subject_key, type, external_txn_id, settlement_amount_minor, "
                        + "ledger_amount_minor, delta_minor, payment_state "
                        + "FROM recon_exceptions WHERE recon_run_id = ? ORDER BY subject_key, type",
                reconRunId);
    }

    @Test
    void everyLineHasExactlyOneOutcomeIncludingNewOutcomeValues() {
        String runId = runId();
        String batchId = batchId();

        captureAt(runId + "-txn-1", new BigDecimal("10.00"), "merch-1", CAPTURED_AT); // MATCHED
        captureAt(runId + "-txn-2", new BigDecimal("20.00"), "merch-2", CAPTURED_AT); // AMOUNT_MISMATCH
        captureAt(runId + "-txn-3", new BigDecimal("30.00"), "merch-3", CAPTURED_AT); // STATE_CONFLICT
        eventService.apply(new TransactionEvent(
                runId + "-txn-3", runId + "-txn-3:REFUND", EventType.REFUND,
                alice, merchant, new BigDecimal("30.00"), "USD", CAPTURED_AT, "merch-3"));
        captureAt(runId + "-txn-4", new BigDecimal("40.00"), "merch-4", CAPTURED_AT); // DUPLICATE
        captureAt(runId + "-txn-5", new BigDecimal("50.00"), "merch-5", CAPTURED_AT); // FUZZY_MATCHED
        // Two identical payments so line 8 has to refuse.
        captureAt(runId + "-txn-6", new BigDecimal("60.00"), "merch-6", CAPTURED_AT);
        captureAt(runId + "-txn-7", new BigDecimal("60.00"), "merch-6", CAPTURED_AT);

        insertBatch(batchId, runId, 8);
        insertLine(batchId, 1, runId + "-txn-1", "merch-1", 1000, SETTLED_AT);
        insertLine(batchId, 2, runId + "-txn-2", "merch-2", 2500, SETTLED_AT);
        insertLine(batchId, 3, runId + "-txn-3", "merch-3", 3000, SETTLED_AT);
        insertLine(batchId, 4, runId + "-txn-4", "merch-4", 4000, SETTLED_AT);
        insertLine(batchId, 5, runId + "-txn-4", "merch-4", 4000, SETTLED_AT);
        insertLine(batchId, 6, runId + "-txn-unknown", "merch-9", 9900, SETTLED_AT);
        insertLine(batchId, 7, null, "merch-5", 5000, SETTLED_AT);
        insertLine(batchId, 8, null, "merch-6", 6000, SETTLED_AT);

        reconciliationService.run(batchId);

        long reconRunId = runIdOf(batchId, ReconciliationService.DEFAULT_WINDOW_SECONDS);

        // Per item first, aggregate second.
        assertThat(outcomeRow(batchId, 1).get("outcome")).isEqualTo("MATCHED");
        assertThat(outcomeRow(batchId, 2).get("outcome")).isEqualTo("AMOUNT_MISMATCH");
        assertThat(outcomeRow(batchId, 3).get("outcome")).isEqualTo("STATE_CONFLICT");
        assertThat(outcomeRow(batchId, 4).get("outcome")).isEqualTo("DUPLICATE_SETTLEMENT");
        assertThat(outcomeRow(batchId, 5).get("outcome")).isEqualTo("DUPLICATE_SETTLEMENT");
        assertThat(outcomeRow(batchId, 6).get("outcome")).isEqualTo("MISSING_IN_LEDGER");
        assertThat(outcomeRow(batchId, 7).get("outcome")).isEqualTo("FUZZY_MATCHED");
        assertThat(outcomeRow(batchId, 8).get("outcome")).isEqualTo("AMBIGUOUS");

        AuditResult result = audit.auditRun(reconRunId);
        assertThat(result.problems()).isEmpty();
        assertThat(result.complete()).isTrue();
    }

    /**
     * The run-scoping check the Day 3 spec asks for before trusting any
     * number: the candidate pool for a run must be exactly that run's
     * payments, and no more.
     *
     * Scoping relies on the {@code external_txn_id LIKE runId || '-%'}
     * naming convention rather than a schema-enforced relationship, so this
     * asserts the convention actually holds against a second run present in
     * the same database with identical attributes -- the case where a leak
     * would show up as a false candidate.
     */
    @Test
    void candidatePoolIsScopedToTheRunsOwnPayments() {
        String runA = runId();
        String runB = runId();
        String batchId = batchId();

        captureAt(runA + "-txn-1", new BigDecimal("50.00"), "merch-1", CAPTURED_AT);
        // Same amount, same merchant, same capture time -- a candidate in
        // every respect except that it belongs to a different run.
        captureAt(runB + "-txn-1", new BigDecimal("50.00"), "merch-1", CAPTURED_AT);

        insertBatch(batchId, runA, 1);
        insertLine(batchId, 1, null, "merch-1", 5000, SETTLED_AT);

        reconciliationService.run(batchId);

        int paymentsInRunA = jdbc.queryForObject(
                "SELECT count(*) FROM transaction_states WHERE external_txn_id LIKE ?",
                Integer.class, runA + "-%");
        Map<String, Object> row = outcomeRow(batchId, 1);

        System.out.println("[candidatePoolIsScopedToTheRunsOwnPayments] "
                + "payments in run = " + paymentsInRunA
                + ", candidates considered = " + row.get("candidate_count"));

        assertThat(paymentsInRunA).isEqualTo(1);
        // One candidate, not two: run B's identical payment is out of scope.
        // Had scoping leaked, this would be AMBIGUOUS with a count of 2.
        assertThat(row.get("candidate_count")).isEqualTo(1);
        assertThat(row.get("outcome")).isEqualTo("FUZZY_MATCHED");
        assertThat(row.get("matched_txn_id")).isEqualTo(runA + "-txn-1");
    }
}
