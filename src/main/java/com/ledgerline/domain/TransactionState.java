package com.ledgerline.domain;

/**
 * Where a transaction sits in its lifecycle.
 *
 * Modelled on a card payment. The states are deliberately few: each one exists
 * because some event is legal in it that is illegal elsewhere, which is the
 * only thing that justifies a state in a machine like this.
 */
public enum TransactionState {

    /** Nothing has happened yet. The starting point for every transaction. */
    NEW,

    /** Funds are held on the issuing side. No money has moved. */
    AUTHORIZED,

    /**
     * The merchant has claimed the held funds.
     *
     * This is where the ledger entries are written. Capture is the first point
     * at which the movement is committed rather than merely reserved.
     */
    CAPTURED,

    /**
     * Funds have moved in the scheme's settlement batch.
     *
     * A bookkeeping milestone rather than a second movement: the entries were
     * already written at capture, so settling writes none. See EntryPolicy.
     */
    SETTLED,

    /** Money returned to the payer after capture. */
    REFUNDED,

    /** An authorization released before capture. No money ever moved. */
    VOIDED,

    /** An authorization that timed out before being captured. */
    EXPIRED;

    /**
     * True when no further event can be applied.
     *
     * SETTLED is deliberately absent. It is a resting state, not a terminal
     * one: a settled payment can still be refunded, and that is the ordinary
     * customer-return path rather than an exception. Treating it as terminal
     * would make the commonest refund in the system illegal.
     */
    public boolean isTerminal() {
        return this == REFUNDED || this == VOIDED || this == EXPIRED;
    }
}
