package com.ledgerline.generator;

/**
 * A deliberate defect injected into a generated stream.
 *
 * Each is something that genuinely happens to real payment pipelines, which is
 * the point: a reconciliation engine graded against invented anomalies proves
 * nothing about the ones it will actually meet.
 */
public enum FaultType {

    /**
     * The same event published two to five times.
     *
     * What at-least-once delivery produces on every retry. The ledger should
     * absorb these without writing twice, so a detector finding "duplicate"
     * here is finding something the system already handled -- which is still
     * worth knowing, because a spike in duplicates means retries are climbing.
     */
    DUPLICATE_PUBLISH,

    /**
     * A capture published before its authorize.
     *
     * Not possible on one partition, entirely possible across three when a
     * producer retries the authorize and the capture goes out first.
     */
    OUT_OF_ORDER,

    /**
     * A capture whose authorize is never published at all.
     *
     * Distinct from out-of-order: nothing is coming. The capture waits
     * indefinitely, and a reconciliation engine should eventually notice a
     * transaction that has been half-arrived for too long.
     */
    ORPHAN_CAPTURE,

    /**
     * The settled amount differs from the captured amount.
     *
     * The fault reconciliation exists for. The ledger says one number, the
     * settlement file says another, and nothing in the pipeline notices
     * because both messages were individually valid.
     */
    AMOUNT_DRIFT,

    /**
     * A transaction that captures and then never settles.
     *
     * Money the ledger believes moved, that the scheme never confirmed. Looks
     * healthy from inside the system, which is exactly why it needs an outside
     * view to catch.
     */
    MISSING_SETTLEMENT
}
