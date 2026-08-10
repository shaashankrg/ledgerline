package com.ledgerline.reconciliation;

/**
 * Matches recon_exceptions' type CHECK constraint exactly. A superset of
 * {@link ReconOutcome}: every settlement-line outcome names an exception
 * type of the same name, plus MISSING_IN_SETTLEMENT, which only ever labels
 * an exception -- a ledger payment with no settlement line at all has no
 * line to hold a {@code recon_line_outcomes} row.
 */
enum ReconExceptionType {
    AMOUNT_MISMATCH,
    MISSING_IN_LEDGER,
    MISSING_IN_SETTLEMENT,
    DUPLICATE_SETTLEMENT,
    STATE_CONFLICT
}
