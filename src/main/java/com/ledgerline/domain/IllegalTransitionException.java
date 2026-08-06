package com.ledgerline.domain;

/**
 * An event that cannot be applied in the transaction's current state.
 *
 * Always permanent: the transaction's state is what it is, and redelivering the
 * same event will hit the same wall. Downstream this maps to a dead letter
 * rather than a retry.
 */
public class IllegalTransitionException extends RuntimeException {

    private final transient TransactionState from;
    private final transient EventType event;

    public IllegalTransitionException(TransactionState from, EventType event) {
        super("cannot apply " + event + " to a transaction in state " + from);
        this.from = from;
        this.event = event;
    }

    public TransactionState from() {
        return from;
    }

    public EventType event() {
        return event;
    }
}
