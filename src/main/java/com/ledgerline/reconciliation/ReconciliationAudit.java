package com.ledgerline.reconciliation;

import java.util.ArrayList;
import java.util.List;

import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.stereotype.Service;

/**
 * Verifies a completed {@link ReconciliationService#run(String)} accounted
 * for its batch completely. This is a check over what {@code run} already
 * wrote, not part of classification itself -- kept as a separate class so
 * "did we classify this line" and "did classification cover everything" stay
 * answerable independently, the same separation of concerns
 * {@code TransactionStateMachine} and {@code EntryPolicy} model for a
 * different pair of questions.
 */
@Service
public class ReconciliationAudit {

    private final NamedParameterJdbcTemplate jdbc;

    ReconciliationAudit(@ReconRoleDataSource NamedParameterJdbcTemplate jdbc) {
        this.jdbc = jdbc;
    }

    /**
     * Runs all four completeness checks for {@code batchId} and returns a
     * result naming exactly which line numbers, if any, fell off the edge --
     * a boolean would say something is wrong without saying what.
     */
    public AuditResult audit(String batchId) {
        List<String> problems = new ArrayList<>();

        checkOutcomeCountMatchesRowCount(batchId, problems);
        List<Integer> linesWithNoOutcome = checkEveryLineHasAtLeastOneOutcome(batchId);
        if (!linesWithNoOutcome.isEmpty()) {
            problems.add("settlement lines with no recon_line_outcomes row: " + linesWithNoOutcome);
        }
        List<Integer> exceptionsMissingLink = checkNonMatchedOutcomesHaveMatchingException(batchId);
        if (!exceptionsMissingLink.isEmpty()) {
            problems.add("line_number(s) with a non-MATCHED outcome but no matching exception: "
                    + exceptionsMissingLink);
        }
        checkExceptionLineNumbersPartitionNonMatchedLines(batchId, problems);

        return new AuditResult(batchId, problems.isEmpty(), List.copyOf(problems));
    }

    /** COUNT(*) in recon_line_outcomes for the batch equals recon_batches.row_count. */
    private void checkOutcomeCountMatchesRowCount(String batchId, List<String> problems) {
        Integer expected = jdbc.queryForObject(
                "SELECT row_count FROM recon_batches WHERE batch_id = :batchId",
                new MapSqlParameterSource("batchId", batchId), Integer.class);
        Integer actual = jdbc.queryForObject(
                "SELECT count(*) FROM recon_line_outcomes WHERE batch_id = :batchId",
                new MapSqlParameterSource("batchId", batchId), Integer.class);
        if (expected == null || actual == null || !expected.equals(actual)) {
            problems.add("recon_line_outcomes count (" + actual
                    + ") does not equal recon_batches.row_count (" + expected + ")");
        }
    }

    /**
     * Every settlement_records line in the batch has exactly one outcome row.
     * The composite primary key on recon_line_outcomes already guarantees
     * at-most-one; this is the at-least-one half of "exactly one."
     */
    private List<Integer> checkEveryLineHasAtLeastOneOutcome(String batchId) {
        return jdbc.queryForList(
                "SELECT sr.line_number FROM settlement_records sr "
                        + "LEFT JOIN recon_line_outcomes o "
                        + "  ON o.batch_id = sr.batch_id AND o.line_number = sr.line_number "
                        + "WHERE sr.batch_id = :batchId AND o.line_number IS NULL "
                        + "ORDER BY sr.line_number",
                new MapSqlParameterSource("batchId", batchId), Integer.class);
    }

    /** Every non-MATCHED outcome has a non-null exception_id pointing at a matching-type exception. */
    private List<Integer> checkNonMatchedOutcomesHaveMatchingException(String batchId) {
        return jdbc.queryForList(
                "SELECT o.line_number FROM recon_line_outcomes o "
                        + "LEFT JOIN recon_exceptions e ON e.id = o.exception_id AND e.type = o.outcome "
                        + "WHERE o.batch_id = :batchId AND o.outcome <> 'MATCHED' AND e.id IS NULL "
                        + "ORDER BY o.line_number",
                new MapSqlParameterSource("batchId", batchId), Integer.class);
    }

    /**
     * The union of settlement_line_numbers across the batch's exceptions
     * equals the set of non-MATCHED line numbers, with no line appearing
     * twice. Checked as two directions of a set comparison plus a duplicate
     * check, rather than one clever query, so a failure names which specific
     * direction broke.
     */
    private void checkExceptionLineNumbersPartitionNonMatchedLines(String batchId, List<String> problems) {
        List<Integer> nonMatchedLines = jdbc.queryForList(
                "SELECT line_number FROM recon_line_outcomes "
                        + "WHERE batch_id = :batchId AND outcome <> 'MATCHED' ORDER BY line_number",
                new MapSqlParameterSource("batchId", batchId), Integer.class);

        List<Integer> exceptionLines = jdbc.queryForList(
                "SELECT unnest(settlement_line_numbers) FROM recon_exceptions "
                        + "WHERE batch_id = :batchId ORDER BY 1",
                new MapSqlParameterSource("batchId", batchId), Integer.class);

        List<Integer> inExceptionsNotInOutcomes = exceptionLines.stream()
                .filter(l -> !nonMatchedLines.contains(l)).distinct().sorted().toList();
        if (!inExceptionsNotInOutcomes.isEmpty()) {
            problems.add("line_number(s) named by an exception but not a non-MATCHED outcome: "
                    + inExceptionsNotInOutcomes);
        }

        List<Integer> inOutcomesNotInExceptions = nonMatchedLines.stream()
                .filter(l -> !exceptionLines.contains(l)).distinct().sorted().toList();
        if (!inOutcomesNotInExceptions.isEmpty()) {
            problems.add("line_number(s) with a non-MATCHED outcome but named by no exception: "
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
     * @param complete true only when every check passed
     * @param problems empty when complete; otherwise one entry per failed
     *                 check, each naming the specific line numbers involved
     */
    public record AuditResult(String batchId, boolean complete, List<String> problems) {
    }
}
