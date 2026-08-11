package com.ledgerline.reconciliation;

/**
 * The bucket a settlement line lands in, evaluated in this fixed order and
 * stopping at the first match -- see {@link ReconciliationService} for the
 * per-condition reasoning. Declared as an ordered enum, rather than left as
 * the incidental order of a chain of {@code if} branches, so the precedence
 * is a fact a reader (and a future maintainer re-ordering branches) can see
 * and check against, not something only provable by re-reading the method.
 *
 * Matches recon_line_outcomes' outcome CHECK constraint exactly.
 * MISSING_IN_SETTLEMENT is deliberately not a member here: it is a property
 * of a ledger payment with no settlement line to attach a line-outcome row
 * to, not a bucket any settlement line can land in. It exists only as a
 * {@link ReconExceptionType}.
 */
enum ReconOutcome {

    /**
     * external_txn_id is null, or the id is absent from {@code
     * transaction_states} entirely -- this run never heard of it. This means
     * exactly one thing after the Day 2 fix-up: the payment cannot be
     * identified at all. A payment that exists but was never captured (or
     * was captured and then voided/refunded/expired) is identifiable, and is
     * STATE_CONFLICT below, not this -- testing presence in a
     * capture-derived view alone cannot distinguish "never heard of" from
     * "heard of, never captured," which was the original defect this
     * precedence closes.
     */
    MISSING_IN_LEDGER,

    /**
     * This external_txn_id appears on more than one line in the batch.
     * Evaluated above STATE_CONFLICT and AMOUNT_MISMATCH deliberately: a
     * duplicated row is a fact about the file's structure, not about any one
     * line's amount or the payment's state, and reporting it as an amount or
     * state problem would hide the actual defect -- fixing the amount on one
     * of two duplicate lines would leave the real problem (the file paid this
     * transaction twice) completely unaddressed.
     */
    DUPLICATE_SETTLEMENT,

    /**
     * The payment is identifiable but not in a state where settlement is
     * expected -- anything other than CAPTURED or SETTLED (NEW, AUTHORIZED,
     * VOIDED, REFUNDED, EXPIRED). A settlement for money that was given
     * back, never taken, or not yet moved is a conflict regardless of
     * whether the reported amount happens to agree with anything -- so this
     * is checked before the amount comparison, not after. AUTHORIZED is
     * included deliberately: money has not moved yet, so it is the same kind
     * of conflict as VOIDED, not a case requiring separate handling.
     */
    STATE_CONFLICT,

    /** gross_amount_minor differs from the ledger's captured amount. */
    AMOUNT_MISMATCH,

    /** None of the above. */
    MATCHED,

    /**
     * Recovered by pass 2: exact matching could not identify this line's
     * payment, but exactly one payment matched it on amount, merchant, and
     * capture time within the run's window.
     *
     * Deliberately a distinct value from MATCHED rather than folded into it.
     * "The unmatched set shrank versus pass 1" is only measurable if
     * fuzzy-recovered lines are countable separately, and a fuzzy match is a
     * weaker claim than an exact one -- it rests on three attributes
     * agreeing rather than on an identifier, so a consumer of these results
     * is entitled to treat it with less confidence.
     */
    FUZZY_MATCHED,

    /**
     * Pass 2 found two or more equally plausible payments and refused to
     * choose.
     *
     * Not an error path. A matcher that breaks ties -- by taking the first,
     * the nearest in time, or the lowest id -- does not fail loudly when it
     * is wrong; it produces a confident, plausible-looking wrong answer,
     * which is strictly worse than producing nothing, because nothing
     * downstream can tell the guess apart from a real match. The candidate
     * count is recorded on the outcome row so the refusal is inspectable
     * rather than merely negative.
     */
    AMBIGUOUS;

    /**
     * The exception type a line with this outcome raises, or empty when it
     * raises none.
     *
     * An explicit table rather than a chain of ifs, and not simply
     * {@code valueOf(name())} as it was before pass 2 existed: two outcomes
     * now break that identity. FUZZY_MATCHED raises nothing, like MATCHED --
     * the line found its payment. AMBIGUOUS raises MISSING_IN_LEDGER,
     * because a line the matcher refused to resolve is still, at the end of
     * the run, a line whose payment was not identified; what distinguishes
     * it from an ordinary miss is candidate_count on the outcome row, not a
     * sixth exception type. Day 4 reasons over this mapping, so it is data
     * it can read rather than control flow it would have to re-derive.
     */
    java.util.Optional<ReconExceptionType> exceptionType() {
        return switch (this) {
            case MATCHED, FUZZY_MATCHED -> java.util.Optional.empty();
            case AMBIGUOUS -> java.util.Optional.of(ReconExceptionType.MISSING_IN_LEDGER);
            case MISSING_IN_LEDGER -> java.util.Optional.of(ReconExceptionType.MISSING_IN_LEDGER);
            case DUPLICATE_SETTLEMENT -> java.util.Optional.of(ReconExceptionType.DUPLICATE_SETTLEMENT);
            case STATE_CONFLICT -> java.util.Optional.of(ReconExceptionType.STATE_CONFLICT);
            case AMOUNT_MISMATCH -> java.util.Optional.of(ReconExceptionType.AMOUNT_MISMATCH);
        };
    }

    /**
     * How this outcome was arrived at, for recon_line_outcomes.match_method.
     *
     * Only MATCHED and FUZZY_MATCHED are match methods, and only they carry a
     * matched_txn_id. That is narrower than "did this line find its payment"
     * -- AMOUNT_MISMATCH and STATE_CONFLICT did find theirs, by id -- and the
     * narrowness is deliberate, because matched_txn_id is what the partial
     * unique index arbitrates over, and that index means *exclusively
     * claimed*, not merely *identified*.
     *
     * DUPLICATE_SETTLEMENT is the case that forces the distinction: n lines
     * legitimately name one payment, so no single one of them exclusively
     * claims it, and stamping the id on all n would collide on the index --
     * turning a correctly detected duplicate into a write failure. Recording
     * them as NONE keeps the index meaning "one payment, one claiming line"
     * while leaving the duplicate's own identification visible where it
     * already lives: on the DUPLICATE_SETTLEMENT exception row, which lists
     * every line number involved and the external_txn_id they share.
     */
    MatchMethod matchMethod() {
        return switch (this) {
            case MATCHED -> MatchMethod.EXACT;
            case FUZZY_MATCHED -> MatchMethod.FUZZY;
            case AMOUNT_MISMATCH, DUPLICATE_SETTLEMENT, STATE_CONFLICT,
                 MISSING_IN_LEDGER, AMBIGUOUS -> MatchMethod.NONE;
        };
    }

    /** Matches recon_line_outcomes.match_method's CHECK constraint. */
    enum MatchMethod {
        EXACT, FUZZY, NONE
    }
}
