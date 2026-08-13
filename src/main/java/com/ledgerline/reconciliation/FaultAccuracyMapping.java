package com.ledgerline.reconciliation;

import java.util.EnumMap;
import java.util.Map;
import java.util.Optional;

import com.ledgerline.settlement.NetworkFaultType;

/**
 * What a correctly-working engine should produce for each {@link
 * NetworkFaultType}, expressed as data rather than as control flow buried in
 * a scoring test.
 *
 * This table, not {@link ReconExceptionType}, is the vocabulary Day 4 scores
 * against: the two enums are not 1:1 (six fault types, five exception types,
 * and one detection -- {@code FUZZY_MATCHED} -- that is not an exception at
 * all), and the mismatch is exactly why this class exists instead of a
 * {@code valueOf} call.
 *
 * Two fault types are declared explicitly out of scope, with a reason, rather
 * than silently absent:
 *
 * <ul>
 *   <li>{@code STATE_CONFLICT}, the exception type, has no fault that reaches
 *       it end-to-end on generator-produced data (see {@code
 *       FaultReachabilityTest.KNOWN_UNREACHABLE_EXCEPTION_TYPES} and its
 *       Javadoc) -- structurally unmeasurable, not a matcher defect.</li>
 *   <li>{@link NetworkFaultType#NETWORK_LATE_SETTLEMENT} has no
 *       classification path today: a late-but-otherwise-correct row exact
 *       matches on id and lands {@code MATCHED} with no exception (see
 *       {@code LateSettlementReachabilityTest} and {@code
 *       FaultReachabilityTest.KNOWN_UNREACHABLE_FAULTS}). The Day 4 decision,
 *       made and recorded here rather than left ambiguous: this stays
 *       explicitly out of scope for scoring, not a sixth exception type and
 *       not a severity flag. {@code time_delta_seconds} is already populated
 *       on every exact match for exactly this situation -- so a future
 *       decision has evidence to work from -- but turning it into a
 *       classification would mean changing {@link ReconciliationService}'s
 *       matching logic and bumping {@code MATCHER_VERSION}, which is Day
 *       1-3-adjacent surgery this task does not have license to do
 *       unprompted.</li>
 * </ul>
 */
final class FaultAccuracyMapping {

    private FaultAccuracyMapping() {
    }

    /**
     * How a fault type is detected: either as a {@link ReconExceptionType}
     * raised against the payment's real {@code external_txn_id}, or as a
     * {@code FUZZY_MATCHED} line outcome recovering it (mangled-id is the
     * only fault fuzzy matching exists to recover -- see {@link
     * NetworkFaultType#NETWORK_MANGLED_TXN_ID}).
     */
    enum Signal {
        EXCEPTION, FUZZY_MATCH
    }

    record Expectation(NetworkFaultType faultType, Signal signal, ReconExceptionType exceptionType) {

        static Expectation exception(NetworkFaultType faultType, ReconExceptionType type) {
            return new Expectation(faultType, Signal.EXCEPTION, type);
        }

        static Expectation fuzzyMatch(NetworkFaultType faultType) {
            return new Expectation(faultType, Signal.FUZZY_MATCH, null);
        }
    }

    /**
     * Fault types this table can score. {@link NetworkFaultType#values()}
     * minus this set is exactly the documented non-mapping
     * (NETWORK_LATE_SETTLEMENT) -- see the class Javadoc.
     */
    private static final Map<NetworkFaultType, Expectation> EXPECTATIONS = buildExpectations();

    private static Map<NetworkFaultType, Expectation> buildExpectations() {
        Map<NetworkFaultType, Expectation> table = new EnumMap<>(NetworkFaultType.class);

        // A dropped row means the ledger captured a payment the settlement
        // file never mentions -- recordMissingInSettlement's sweep over
        // captured payments with no claiming line, keyed on the real txn id.
        table.put(NetworkFaultType.NETWORK_DROPPED_ROW,
                Expectation.exception(NetworkFaultType.NETWORK_DROPPED_ROW,
                        ReconExceptionType.MISSING_IN_SETTLEMENT));

        // A transposed/fat-fingered amount: exact id match, gross disagrees
        // with the ledger's captured amount.
        table.put(NetworkFaultType.NETWORK_AMOUNT_DRIFT,
                Expectation.exception(NetworkFaultType.NETWORK_AMOUNT_DRIFT,
                        ReconExceptionType.AMOUNT_MISMATCH));

        // The same external_txn_id appears on more than one settlement line.
        table.put(NetworkFaultType.NETWORK_DUPLICATE_ROW,
                Expectation.exception(NetworkFaultType.NETWORK_DUPLICATE_ROW,
                        ReconExceptionType.DUPLICATE_SETTLEMENT));

        // A row whose id resolves to no payment we have ever captured.
        // SettlementBatchRepository.recordFault stores the fabricated id
        // itself as external_txn_id -- it is not a real payment's id, but it
        // is exactly what the settlement line and the ground-truth row both
        // carry, which is what the scoring join needs.
        table.put(NetworkFaultType.NETWORK_UNKNOWN_TXN,
                Expectation.exception(NetworkFaultType.NETWORK_UNKNOWN_TXN,
                        ReconExceptionType.MISSING_IN_LEDGER));

        // The one fault fuzzy matching exists to recover: id corrupted, every
        // other attribute correct. A correctly-working engine reports
        // FUZZY_MATCHED, not an exception -- the payment was found, just not
        // by identifier. Ground truth's external_txn_id is the *original*
        // (real) id (see SettlementBatchRepository.NetworkFault.mangledTxnId),
        // which is also what a successful FUZZY_MATCHED's matched_txn_id
        // carries.
        table.put(NetworkFaultType.NETWORK_MANGLED_TXN_ID,
                Expectation.fuzzyMatch(NetworkFaultType.NETWORK_MANGLED_TXN_ID));

        // NETWORK_LATE_SETTLEMENT: deliberately absent. See class Javadoc.

        return Map.copyOf(table);
    }

    /** Fault types this table scores -- NETWORK_LATE_SETTLEMENT is not among them. */
    static java.util.Set<NetworkFaultType> scoredFaultTypes() {
        return EXPECTATIONS.keySet();
    }

    static Optional<Expectation> expectationFor(NetworkFaultType faultType) {
        return Optional.ofNullable(EXPECTATIONS.get(faultType));
    }

    /** Fault types explicitly excluded from scoring, with the reason on record. */
    static java.util.Set<NetworkFaultType> excludedFaultTypes() {
        return java.util.EnumSet.complementOf(java.util.EnumSet.copyOf(EXPECTATIONS.keySet()));
    }

    /**
     * Exception types with no NetworkFaultType that can produce them on
     * generator-produced data -- currently just STATE_CONFLICT. Named here,
     * not just in FaultReachabilityTest, so the accuracy report can print the
     * same exclusion the reachability test already enforces, rather than the
     * two drifting apart.
     */
    static final java.util.Set<ReconExceptionType> UNREACHABLE_EXCEPTION_TYPES =
            java.util.Set.of(ReconExceptionType.STATE_CONFLICT);
}
