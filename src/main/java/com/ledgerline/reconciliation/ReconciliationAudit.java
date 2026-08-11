package com.ledgerline.reconciliation;

import java.util.ArrayList;
import java.util.List;

import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.stereotype.Service;

/**
 * Verifies a completed {@link ReconciliationService#run(String, int)}
 * accounted for its batch completely. This is a check over what {@code run}
 * already wrote, not part of classification itself -- kept as a separate
 * class so "did we classify this line" and "did classification cover
 * everything" stay answerable independently, the same separation of concerns
 * {@code TransactionStateMachine} and {@code EntryPolicy} model for a
 * different pair of questions.
 *
 * Audits a *run*, not a batch. One batch can now have several runs (one per
 * window), and completeness is a property of each independently -- auditing
 * a batch would silently mix two runs' outcomes and report a line count
 * double what any single run produced.
 */
@Service
public class ReconciliationAudit {

    private final NamedParameterJdbcTemplate jdbc;

    ReconciliationAudit(@ReconRoleDataSource NamedParameterJdbcTemplate jdbc) {
        this.jdbc = jdbc;
    }

    /**
     * Audits the most recent run for {@code batchId}, at the default window.
     * Convenience for callers that only ever run one reconciliation per
     * batch; anything sweeping windows should name the run explicitly.
     */
    public AuditResult audit(String batchId) {
        return auditRun(resolveRunId(batchId, ReconciliationService.DEFAULT_WINDOW_SECONDS));
    }

    /** Audits the run for {@code batchId} at {@code windowSeconds}. */
    public AuditResult audit(String batchId, int windowSeconds) {
        return auditRun(resolveRunId(batchId, windowSeconds));
    }

    /**
     * Runs all four completeness checks for one run and returns a result
     * naming exactly which line numbers, if any, fell off the edge -- a
     * boolean would say something is wrong without saying what.
     */
    public AuditResult auditRun(long reconRunId) {
        List<String> problems = new ArrayList<>();

        checkOutcomeCountMatchesRowCount(reconRunId, problems);

        List<Integer> linesWithNoOutcome = checkEveryLineHasAtLeastOneOutcome(reconRunId);
        if (!linesWithNoOutcome.isEmpty()) {
            problems.add("settlement lines with no recon_line_outcomes row: " + linesWithNoOutcome);
        }

        List<Integer> exceptionsMissingLink = checkOutcomesPointAtTheRightExceptionType(reconRunId);
        if (!exceptionsMissingLink.isEmpty()) {
            problems.add("line_number(s) whose outcome does not point at an exception of the "
                    + "type its outcome maps to: " + exceptionsMissingLink);
        }

        checkExceptionLineNumbersPartitionExceptionRaisingLines(reconRunId, problems);
        checkNoPaymentIsClaimedTwice(reconRunId, problems);

        return new AuditResult(reconRunId, problems.isEmpty(), List.copyOf(problems));
    }

    private long resolveRunId(String batchId, int windowSeconds) {
        return jdbc.queryForObject(
                "SELECT recon_run_id FROM recon_runs WHERE batch_id = :batchId "
                        + "AND window_seconds = :windowSeconds AND matcher_version = :matcherVersion",
                new MapSqlParameterSource()
                        .addValue("batchId", batchId)
                        .addValue("windowSeconds", windowSeconds)
                        .addValue("matcherVersion", ReconciliationService.MATCHER_VERSION),
                Long.class);
    }

    /** COUNT(*) in recon_line_outcomes for the run equals recon_batches.row_count. */
    private void checkOutcomeCountMatchesRowCount(long reconRunId, List<String> problems) {
        Integer expected = jdbc.queryForObject(
                "SELECT b.row_count FROM recon_batches b "
                        + "JOIN recon_runs r ON r.batch_id = b.batch_id "
                        + "WHERE r.recon_run_id = :reconRunId",
                new MapSqlParameterSource("reconRunId", reconRunId), Integer.class);
        Integer actual = jdbc.queryForObject(
                "SELECT count(*) FROM recon_line_outcomes WHERE recon_run_id = :reconRunId",
                new MapSqlParameterSource("reconRunId", reconRunId), Integer.class);
        if (expected == null || actual == null || !expected.equals(actual)) {
            problems.add("recon_line_outcomes count (" + actual
                    + ") does not equal recon_batches.row_count (" + expected + ")");
        }
    }

    /**
     * Every settlement_records line in the run's batch has exactly one
     * outcome row. The composite primary key already guarantees
     * at-most-one; this is the at-least-one half of "exactly one."
     */
    private List<Integer> checkEveryLineHasAtLeastOneOutcome(long reconRunId) {
        return jdbc.queryForList(
                "SELECT sr.line_number FROM settlement_records sr "
                        + "JOIN recon_runs r ON r.batch_id = sr.batch_id "
                        + "LEFT JOIN recon_line_outcomes o "
                        + "  ON o.recon_run_id = r.recon_run_id AND o.line_number = sr.line_number "
                        + "WHERE r.recon_run_id = :reconRunId AND o.line_number IS NULL "
                        + "ORDER BY sr.line_number",
                new MapSqlParameterSource("reconRunId", reconRunId), Integer.class);
    }

    /**
     * Every outcome that maps to an exception type points at an exception of
     * exactly that type, and every outcome that maps to none points at none.
     *
     * The mapping is {@link ReconOutcome#exceptionType()}, read out of the
     * enum into SQL rather than restated here, because pass 2 broke the
     * identity this check used to assume. It used to read "outcome <>
     * 'MATCHED' implies an exception whose type equals the outcome", which
     * is now wrong twice over: FUZZY_MATCHED is not MATCHED but raises no
     * exception, and AMBIGUOUS raises a MISSING_IN_LEDGER exception whose
     * type does not equal its own name.
     */
    private List<Integer> checkOutcomesPointAtTheRightExceptionType(long reconRunId) {
        List<Integer> wrong = new ArrayList<>();

        for (ReconOutcome outcome : ReconOutcome.values()) {
            String expectedType = outcome.exceptionType().map(Enum::name).orElse(null);

            if (expectedType == null) {
                wrong.addAll(jdbc.queryForList(
                        "SELECT line_number FROM recon_line_outcomes "
                                + "WHERE recon_run_id = :reconRunId AND outcome = :outcome "
                                + "  AND exception_id IS NOT NULL "
                                + "ORDER BY line_number",
                        new MapSqlParameterSource()
                                .addValue("reconRunId", reconRunId)
                                .addValue("outcome", outcome.name()),
                        Integer.class));
            } else {
                wrong.addAll(jdbc.queryForList(
                        "SELECT o.line_number FROM recon_line_outcomes o "
                                + "LEFT JOIN recon_exceptions e "
                                + "  ON e.id = o.exception_id AND e.type = :expectedType "
                                + "WHERE o.recon_run_id = :reconRunId AND o.outcome = :outcome "
                                + "  AND e.id IS NULL "
                                + "ORDER BY o.line_number",
                        new MapSqlParameterSource()
                                .addValue("reconRunId", reconRunId)
                                .addValue("outcome", outcome.name())
                                .addValue("expectedType", expectedType),
                        Integer.class));
            }
        }

        return wrong.stream().distinct().sorted().toList();
    }

    /**
     * The union of settlement_line_numbers across the run's exceptions
     * equals the set of lines whose outcome raises an exception, with no
     * line appearing twice.
     *
     * "Raises an exception" rather than "is not MATCHED", for the same
     * reason as above: FUZZY_MATCHED lines are not MATCHED but name no
     * exception, so the old formulation would report every fuzzy recovery as
     * a hole.
     */
    private void checkExceptionLineNumbersPartitionExceptionRaisingLines(
            long reconRunId, List<String> problems) {

        List<String> exceptionRaisingOutcomes = new ArrayList<>();
        for (ReconOutcome outcome : ReconOutcome.values()) {
            if (outcome.exceptionType().isPresent()) {
                exceptionRaisingOutcomes.add(outcome.name());
            }
        }

        List<Integer> exceptionRaisingLines = jdbc.queryForList(
                "SELECT line_number FROM recon_line_outcomes "
                        + "WHERE recon_run_id = :reconRunId AND outcome IN (:outcomes) "
                        + "ORDER BY line_number",
                new MapSqlParameterSource()
                        .addValue("reconRunId", reconRunId)
                        .addValue("outcomes", exceptionRaisingOutcomes),
                Integer.class);

        // superseded_at IS NULL: a superseded MISSING_IN_LEDGER exception's
        // settlement_line_numbers still contains the line pass 2 has since
        // recovered, and it is kept rather than deleted (see V13). Without
        // this filter every fuzzy recovery would look like a completeness
        // violation -- a line whose outcome no longer raises an exception but
        // is still named by one, because the one naming it is retired
        // evidence, not an active finding.
        List<Integer> exceptionLines = jdbc.queryForList(
                "SELECT unnest(settlement_line_numbers) FROM recon_exceptions "
                        + "WHERE recon_run_id = :reconRunId AND superseded_at IS NULL ORDER BY 1",
                new MapSqlParameterSource("reconRunId", reconRunId), Integer.class);

        List<Integer> inExceptionsNotInOutcomes = exceptionLines.stream()
                .filter(l -> !exceptionRaisingLines.contains(l)).distinct().sorted().toList();
        if (!inExceptionsNotInOutcomes.isEmpty()) {
            problems.add("line_number(s) named by an exception but whose outcome raises none: "
                    + inExceptionsNotInOutcomes);
        }

        List<Integer> inOutcomesNotInExceptions = exceptionRaisingLines.stream()
                .filter(l -> !exceptionLines.contains(l)).distinct().sorted().toList();
        if (!inOutcomesNotInExceptions.isEmpty()) {
            problems.add("line_number(s) whose outcome raises an exception but named by none: "
                    + inOutcomesNotInExceptions);
        }

        List<Integer> duplicated = exceptionLines.stream()
                .filter(l -> java.util.Collections.frequency(exceptionLines, l) > 1)
                .distinct().sorted().toList();
        if (!duplicated.isEmpty()) {
            problems.add("line_number(s) named by more than one exception: " + duplicated);
        }
    }

    /**
     * No payment is claimed by two lines in this run.
     *
     * The partial unique index already makes this unrepresentable, so this
     * check can only fail if the index were dropped -- which is exactly what
     * the Day 3 sabotage does. Asserting it here as well means the audit,
     * not only the write, reports the breach.
     */
    private void checkNoPaymentIsClaimedTwice(long reconRunId, List<String> problems) {
        List<String> doubleClaimed = jdbc.queryForList(
                "SELECT matched_txn_id FROM recon_line_outcomes "
                        + "WHERE recon_run_id = :reconRunId AND matched_txn_id IS NOT NULL "
                        + "GROUP BY matched_txn_id HAVING count(*) > 1 "
                        + "ORDER BY matched_txn_id",
                new MapSqlParameterSource("reconRunId", reconRunId), String.class);
        if (!doubleClaimed.isEmpty()) {
            problems.add("payment(s) claimed by more than one settlement line: " + doubleClaimed);
        }
    }

    /**
     * @param complete true only when every check passed
     * @param problems empty when complete; otherwise one entry per failed
     *                 check, each naming the specific line numbers involved
     */
    public record AuditResult(long reconRunId, boolean complete, List<String> problems) {
    }
}
