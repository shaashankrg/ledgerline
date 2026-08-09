package com.ledgerline.settlement;

/**
 * A deliberate defect injected into the settlement file, from the network's
 * side of the comparison.
 *
 * Mirrors {@link com.ledgerline.generator.FaultType} in spirit -- each models
 * something that genuinely happens to a card network's settlement batch --
 * but is a separate enum because these faults are properties of the file, not
 * of the event stream that produced it.
 */
public enum NetworkFaultType {

    /**
     * Omit the row for a captured payment.
     *
     * The network never settles something we captured -- money we're owed
     * and haven't been paid.
     */
    NETWORK_DROPPED_ROW,

    /**
     * Alter the reported gross amount.
     *
     * A transposed digit or a partial settlement. Injected in both
     * directions -- over and under -- so a detector with an {@code abs()}
     * hidden in it cannot pass by only ever seeing one sign.
     */
    NETWORK_AMOUNT_DRIFT,

    /**
     * Emit the same payment's row twice in the batch.
     *
     * A batch re-sent after a failed transfer; without detection we'd be
     * paid twice for the same movement.
     */
    NETWORK_DUPLICATE_ROW,

    /**
     * Shift settled_at far outside the normal settlement window.
     *
     * Models a delayed settlement -- the money is real, but arrives long
     * after it should have.
     */
    NETWORK_LATE_SETTLEMENT,

    /**
     * Emit a row whose external_txn_id matches no payment we have.
     *
     * A network error, or a payment we lost on our own side. Deliberately
     * has no corresponding captured payment at all.
     */
    NETWORK_UNKNOWN_TXN
}
