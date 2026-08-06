package com.ledgerline.domain;

/**
 * Another writer advanced the transaction first.
 *
 * Raised when the conditional update matches no row, meaning the state was not
 * what the caller had read a moment earlier. Distinct from
 * {@link IllegalTransitionException}: that one says the event is wrong for this
 * transaction and will never apply, this one says the caller lost a race and
 * the event may well be legal against the state that actually won.
 *
 * That distinction decides what happens downstream. An illegal transition is a
 * dead letter; a conflict is worth retrying, because re-reading the new state
 * may show the event applies cleanly -- or that it was already applied, which
 * is the duplicate-delivery case and equally fine.
 */
public class TransitionConflictException extends RuntimeException {

    private final transient String externalTxnId;
    private final transient TransactionState expected;

    public TransitionConflictException(String externalTxnId, TransactionState expected) {
        super("transaction " + externalTxnId + " was no longer in state " + expected
                + " when the transition was applied");
        this.externalTxnId = externalTxnId;
        this.expected = expected;
    }

    public String externalTxnId() {
        return externalTxnId;
    }

    public TransactionState expected() {
        return expected;
    }
}
