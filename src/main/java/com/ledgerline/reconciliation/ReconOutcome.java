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
    MATCHED;

    ReconExceptionType asExceptionType() {
        return ReconExceptionType.valueOf(name());
    }
}
