package com.ledgerline.reconciliation;

import java.util.ArrayList;
import java.util.EnumMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.stereotype.Component;

import com.ledgerline.reconciliation.FaultAccuracyMapping.Expectation;
import com.ledgerline.settlement.NetworkFaultType;

/**
 * Grades a completed {@link ReconciliationService#run(String, int)} against
 * {@code faultlab.injected_faults} -- the answer key the engine itself must
 * never read.
 *
 * <h2>Why this class is allowed to read {@code faultlab}</h2>
 *
 * This is not the reconciliation engine. It is, in the same sense {@code
 * FaultReachabilityTest} is not the engine (see that class's Javadoc for the
 * full argument): code that computes a property <em>about</em> the engine's
 * output, from outside it, after the fact, with no causal path back into any
 * classification decision. It is constructed with the default, application
 * -privileged {@link NamedParameterJdbcTemplate} -- explicitly not the
 * {@code @ReconRoleDataSource}-qualified one {@link ReconciliationService}
 * and {@link ReconciliationAudit} use -- so the isolation the {@code
 * recon_role} REVOKE enforces is never at risk of being bypassed by this
 * class's own queries. Postgres would refuse a {@code faultlab} query issued
 * as {@code recon_role} outright; this class simply never authenticates as
 * that role in the first place.
 */
@Component
public class ReconAccuracyScorer {

    private final NamedParameterJdbcTemplate jdbc;

    ReconAccuracyScorer(NamedParameterJdbcTemplate jdbc) {
        this.jdbc = jdbc;
    }

    /**
     * Scores one completed run. {@code batchId} and {@code windowSeconds}
     * resolve the same {@code recon_runs} row {@link ReconciliationService}
     * created, keyed together with the matcher version so a sweep can never
     * silently mix results from two different matcher builds -- see {@link
     * #assertSingleMatcherVersion}.
     */
    public AccuracyReport score(String batchId, int windowSeconds) {
        Long reconRunId = jdbc.queryForObject(
                "SELECT recon_run_id FROM recon_runs WHERE batch_id = :batchId "
                        + "AND window_seconds = :windowSeconds AND matcher_version = :matcherVersion",
                new MapSqlParameterSource()
                        .addValue("batchId", batchId)
                        .addValue("windowSeconds", windowSeconds)
                        .addValue("matcherVersion", ReconciliationService.MATCHER_VERSION),
                Long.class);

        assertCandidatePoolMatchesPaymentCount(batchId, reconRunId);

        Map<NetworkFaultType, List<String>> groundTruth = loadGroundTruth(batchId);
        Set<String> currentExceptionSubjects = loadCurrentExceptionSubjects(reconRunId);
        Map<NetworkFaultType, Set<String>> exceptionHitsByType = loadExceptionHitsByType(reconRunId);
        Set<String> fuzzyMatchedTxnIds = loadFuzzyMatchedTxnIds(reconRunId);

        // Every subject named by ground truth of *any* scored type, regardless
        // of which type it belongs to -- used below to strip out false
        // positives explainable by identity (e.g. a recovered fuzzy match
        // whose payment happens to also appear, wrongly, in another type's
        // detections).
        Set<String> allGroundTruthSubjects = new HashSet<>();
        for (List<String> txnIds : groundTruth.values()) {
            allGroundTruthSubjects.addAll(txnIds);
        }

        // NETWORK_MANGLED_TXN_ID's false negatives (fuzzy recovery that
        // failed -- candidate outside the window, or ambiguous) computed
        // first, because they spill into two other types' false-positive
        // counts and have to be subtracted from both.
        //
        // The spillover cannot be resolved by identity: an unrecovered
        // mangled row's MISSING_IN_LEDGER exception carries the *corrupted*
        // id as written on the settlement line (ReconciliationService
        // .classifyAndRecord's step 1 uses line.externalTxnId()), while
        // NETWORK_MANGLED_TXN_ID's ground truth row records the *original*
        // id (SettlementBatchRepository.NetworkFault.mangledTxnId) -- the two
        // ids are, by construction, never equal for a fault the matcher
        // could not recover, and the only place the corrupted id is written
        // to the answer key is a free-form detail string documented as "not
        // the primary record of anything," not something to parse for
        // scoring. So this is a structural, count-based correction instead
        // of an identity join: exactly as many unexplained MISSING_IN_LEDGER
        // / MISSING_IN_SETTLEMENT detections as there are unrecovered
        // mangled-id faults are attributable to that spillover, and are
        // excluded from NETWORK_UNKNOWN_TXN / NETWORK_DROPPED_ROW's false
        // positive counts rather than charged to the wrong type.
        int mangledFalseNegatives = 0;
        List<String> mangledTruth = groundTruth.getOrDefault(NetworkFaultType.NETWORK_MANGLED_TXN_ID, List.of());
        for (String txnId : mangledTruth) {
            if (!fuzzyMatchedTxnIds.contains(txnId)) {
                mangledFalseNegatives++;
            }
        }

        Map<NetworkFaultType, TypeScore> perType = new EnumMap<>(NetworkFaultType.class);

        for (NetworkFaultType faultType : FaultAccuracyMapping.scoredFaultTypes()) {
            Expectation expectation = FaultAccuracyMapping.expectationFor(faultType).orElseThrow();
            List<String> truthTxnIds = groundTruth.getOrDefault(faultType, List.of());

            Set<String> detectedForThisType = switch (expectation.signal()) {
                case EXCEPTION -> exceptionHitsByType.getOrDefault(faultType, Set.of());
                case FUZZY_MATCH -> fuzzyMatchedTxnIds;
            };

            int tp = 0;
            int fn = 0;
            for (String txnId : truthTxnIds) {
                if (detectedForThisType.contains(txnId)) {
                    tp++;
                } else {
                    fn++;
                }
            }

            // False positives for this type: lines the engine flagged with
            // this type's signal, on a subject with no ground-truth
            // explanation by identity at all.
            Set<String> truthSet = new HashSet<>(truthTxnIds);
            int fpByIdentity = 0;
            for (String subject : detectedForThisType) {
                if (!truthSet.contains(subject) && !allGroundTruthSubjects.contains(subject)) {
                    fpByIdentity++;
                }
            }

            // The structural correction: NETWORK_UNKNOWN_TXN shares
            // MISSING_IN_LEDGER with unrecovered mangled-id rows;
            // NETWORK_DROPPED_ROW shares MISSING_IN_SETTLEMENT with them the
            // same way (recordMissingInSettlement fires for exactly the same
            // "nothing claims this payment" condition an unrecovered
            // mangled-id payment also satisfies). Up to mangledFalseNegatives
            // of this type's identity-unexplained detections are attributable
            // to that spillover rather than to a genuine defect in this type.
            boolean sharesBucketWithMangled = expectation.signal() == FaultAccuracyMapping.Signal.EXCEPTION
                    && (expectation.exceptionType() == ReconExceptionType.MISSING_IN_LEDGER
                            || expectation.exceptionType() == ReconExceptionType.MISSING_IN_SETTLEMENT);
            int fp = sharesBucketWithMangled
                    ? Math.max(0, fpByIdentity - mangledFalseNegatives)
                    : fpByIdentity;

            perType.put(faultType, new TypeScore(faultType, tp, fp, fn));
        }

        // Ground-truth rows for excluded fault types (NETWORK_LATE_SETTLEMENT)
        // are neither scored nor silently dropped -- counted and surfaced so
        // the exclusion is visible in the report, not an absence a reader has
        // to notice on their own.
        int excludedGroundTruthCount = 0;
        for (NetworkFaultType excluded : FaultAccuracyMapping.excludedFaultTypes()) {
            excludedGroundTruthCount += groundTruth.getOrDefault(excluded, List.of()).size();
        }

        return new AccuracyReport(
                batchId, reconRunId, windowSeconds,
                Map.copyOf(perType),
                Set.copyOf(FaultAccuracyMapping.excludedFaultTypes()),
                excludedGroundTruthCount,
                FaultAccuracyMapping.UNREACHABLE_EXCEPTION_TYPES,
                currentExceptionSubjects.size());
    }

    /**
     * Ground truth for this batch: {@code faultlab.injected_faults} rows with
     * {@code source = 'NETWORK'}, keyed on {@code run_id = batchId} -- see
     * {@code SettlementBatchRepository.recordFault}, which stores the batch id
     * in {@code run_id} for network-side faults (the generator-side {@code
     * source = 'PROCESSOR'} rows use a real generator run id in that same
     * column, which is exactly why the {@code source} filter is not
     * optional).
     */
    private Map<NetworkFaultType, List<String>> loadGroundTruth(String batchId) {
        List<Map<String, Object>> rows = jdbc.queryForList(
                "SELECT fault_type, external_txn_id FROM faultlab.injected_faults "
                        + "WHERE run_id = :batchId AND source = 'NETWORK'",
                new MapSqlParameterSource("batchId", batchId));

        Map<NetworkFaultType, List<String>> byType = new EnumMap<>(NetworkFaultType.class);
        for (Map<String, Object> row : rows) {
            NetworkFaultType type = NetworkFaultType.valueOf((String) row.get("fault_type"));
            byType.computeIfAbsent(type, t -> new ArrayList<>()).add((String) row.get("external_txn_id"));
        }
        return byType;
    }

    /** Every subject_key currently (non-superseded) named by an exception in this run. */
    private Set<String> loadCurrentExceptionSubjects(long reconRunId) {
        return new HashSet<>(jdbc.queryForList(
                "SELECT subject_key FROM recon_exceptions "
                        + "WHERE recon_run_id = :reconRunId AND superseded_at IS NULL",
                new MapSqlParameterSource("reconRunId", reconRunId), String.class));
    }

    /**
     * external_txn_id of every current exception, grouped by the fault type
     * whose expected exception type matches. A subject can appear under more
     * than one fault type's bucket only if two fault types map to the same
     * exception type, which the mapping table does not do today -- each
     * exception type in {@link FaultAccuracyMapping} belongs to exactly one
     * fault type.
     */
    private Map<NetworkFaultType, Set<String>> loadExceptionHitsByType(long reconRunId) {
        Map<NetworkFaultType, Set<String>> result = new EnumMap<>(NetworkFaultType.class);
        for (NetworkFaultType faultType : FaultAccuracyMapping.scoredFaultTypes()) {
            Expectation expectation = FaultAccuracyMapping.expectationFor(faultType).orElseThrow();
            if (expectation.signal() != FaultAccuracyMapping.Signal.EXCEPTION) {
                continue;
            }
            List<String> subjects = jdbc.queryForList(
                    "SELECT external_txn_id FROM recon_exceptions "
                            + "WHERE recon_run_id = :reconRunId AND superseded_at IS NULL "
                            + "AND type = :type AND external_txn_id IS NOT NULL",
                    new MapSqlParameterSource()
                            .addValue("reconRunId", reconRunId)
                            .addValue("type", expectation.exceptionType().name()),
                    String.class);
            result.put(faultType, new HashSet<>(subjects));
        }
        return result;
    }

    /** matched_txn_id of every FUZZY_MATCHED line outcome in this run. */
    private Set<String> loadFuzzyMatchedTxnIds(long reconRunId) {
        return new HashSet<>(jdbc.queryForList(
                "SELECT matched_txn_id FROM recon_line_outcomes "
                        + "WHERE recon_run_id = :reconRunId AND outcome = 'FUZZY_MATCHED'",
                new MapSqlParameterSource("reconRunId", reconRunId), String.class));
    }

    /**
     * Guards the naming-convention scoping caveat: {@code external_txn_id
     * LIKE runId || '-%'} is a convention {@link CapturedLedgerView} relies
     * on, not a schema-enforced guarantee ({@code transaction_states} has no
     * {@code run_id} column or FK). Before any per-run number is trusted,
     * confirm the candidate pool {@link CapturedLedgerView#capturedPaymentsFor}
     * actually returns for this run's batch matches the batch's own recorded
     * payment count -- if a stray payment from another run's id prefix ever
     * leaked in (or the naming convention broke), this fails loudly here
     * rather than quietly inflating or deflating every downstream ratio.
     */
    private void assertCandidatePoolMatchesPaymentCount(String batchId, long reconRunId) {
        String runId = jdbc.queryForObject(
                "SELECT run_id FROM recon_batches WHERE batch_id = :batchId",
                new MapSqlParameterSource("batchId", batchId), String.class);

        Integer candidatePoolSize = jdbc.queryForObject(
                "SELECT count(*) FROM transaction_states WHERE external_txn_id LIKE :prefix",
                new MapSqlParameterSource("prefix", runId + "-%"), Integer.class);

        Integer actualPaymentCount = jdbc.queryForObject(
                "SELECT count(*) FROM faultlab.generator_runs g "
                        + "JOIN transaction_states ts ON ts.external_txn_id LIKE g.run_id || '-%' "
                        + "WHERE g.run_id = :runId",
                new MapSqlParameterSource("runId", runId), Integer.class);

        if (candidatePoolSize == null || actualPaymentCount == null
                || !candidatePoolSize.equals(actualPaymentCount)) {
            throw new IllegalStateException(
                    "candidate pool size (" + candidatePoolSize + ") for run " + runId
                            + " does not match its recorded payment count (" + actualPaymentCount
                            + ") -- the external_txn_id LIKE runId || '-%' scoping convention "
                            + "may have collided with another run's ids; refusing to trust this "
                            + "run's accuracy numbers");
        }
    }

    /**
     * Fails loudly if {@code runs} were not all produced by the same {@code
     * matcher_version} -- a window sweep is only interpretable if the window
     * was the only thing that varied. Call before comparing accuracy numbers
     * across a sweep.
     */
    public void assertSingleMatcherVersion(List<String> batchIds, List<Integer> windowSecondsValues) {
        Set<String> versions = new HashSet<>();
        for (String batchId : batchIds) {
            for (int windowSeconds : windowSecondsValues) {
                List<String> found = jdbc.queryForList(
                        "SELECT matcher_version FROM recon_runs "
                                + "WHERE batch_id = :batchId AND window_seconds = :windowSeconds",
                        new MapSqlParameterSource()
                                .addValue("batchId", batchId)
                                .addValue("windowSeconds", windowSeconds),
                        String.class);
                versions.addAll(found);
            }
        }
        if (versions.size() > 1) {
            throw new IllegalStateException(
                    "sweep spans more than one matcher_version: " + versions
                            + " -- results are not comparable; rerun after clearing stale recon_runs rows");
        }
    }

    /** TP/FP/FN for one fault type, at one run. */
    public record TypeScore(NetworkFaultType faultType, int truePositives, int falsePositives, int falseNegatives) {

        public double precision() {
            int denom = truePositives + falsePositives;
            return denom == 0 ? Double.NaN : (double) truePositives / denom;
        }

        public double recall() {
            int denom = truePositives + falseNegatives;
            return denom == 0 ? Double.NaN : (double) truePositives / denom;
        }
    }

    /**
     * @param excludedFaultTypes        fault types not scored (NETWORK_LATE_SETTLEMENT), on record
     * @param excludedGroundTruthCount  how many ground-truth rows those excluded types account for
     * @param excludedExceptionTypes    exception types structurally unmeasurable (STATE_CONFLICT)
     * @param totalCurrentExceptions    every current exception this run raised, of any type --
     *                                  a sanity figure, not itself scored
     */
    public record AccuracyReport(
            String batchId,
            long reconRunId,
            int windowSeconds,
            Map<NetworkFaultType, TypeScore> perType,
            Set<NetworkFaultType> excludedFaultTypes,
            int excludedGroundTruthCount,
            Set<ReconExceptionType> excludedExceptionTypes,
            int totalCurrentExceptions) {

        /** Overall precision/recall, pooling TP/FP/FN across every scored type. */
        public TypeScore overall() {
            int tp = 0;
            int fp = 0;
            int fn = 0;
            for (TypeScore score : perType.values()) {
                tp += score.truePositives();
                fp += score.falsePositives();
                fn += score.falseNegatives();
            }
            return new TypeScore(null, tp, fp, fn);
        }
    }
}
