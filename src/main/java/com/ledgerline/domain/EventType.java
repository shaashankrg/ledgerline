package com.ledgerline.domain;

/**
 * What happened to a transaction.
 *
 * One per real-world action in a card lifecycle. Deliberately excludes
 * anything that is a state rather than an action -- there is no "pending"
 * event, because pending is not something that happens, it is where you are.
 */
public enum EventType {

    /** Place a hold on the payer's funds. */
    AUTHORIZE,

    /** Claim the held funds. Writes the ledger entries. */
    CAPTURE,

    /** Confirm the movement settled in the scheme batch. Writes nothing. */
    SETTLE,

    /** Return captured funds to the payer. Writes reversing entries. */
    REFUND,

    /** Release an authorization before capture. Writes nothing. */
    VOID,

    /** An authorization aged out before capture. Writes nothing. */
    EXPIRE
}
