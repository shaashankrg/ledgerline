package com.ledgerline.domain;

import java.util.EnumMap;
import java.util.EnumSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

/**
 * The legal lifecycle of a transaction, as a table.
 *
 * Transitions are data, not control flow. A chain of if/else or a switch buried
 * in a service answers "can I do this?" only by being executed; a table can be
 * read, counted, and asserted over exhaustively -- which is what makes the
 * completeness of the rules visible rather than assumed.
 *
 * The table is total by construction: every (state, event) pair either appears
 * here or is illegal, and there is no third possibility.
 */
public final class TransactionStateMachine {

    /**
     * Every legal transition, as from-state to (event to next-state).
     *
     * Read a row as: in this state, these events are legal, and each moves the
     * transaction to that state.
     */
    private static final Map<TransactionState, Map<EventType, TransactionState>> TRANSITIONS =
            buildTransitions();

    private static Map<TransactionState, Map<EventType, TransactionState>> buildTransitions() {
        Map<TransactionState, Map<EventType, TransactionState>> table =
                new EnumMap<>(TransactionState.class);

        // NEW: a transaction can only begin by being authorized. Capturing
        // without an authorization is the classic ordering violation this
        // machine exists to reject.
        table.put(TransactionState.NEW, transitions(
                entry(EventType.AUTHORIZE, TransactionState.AUTHORIZED)));

        // AUTHORIZED: funds are held. Three ways out -- claim them, release
        // them, or let them lapse.
        table.put(TransactionState.AUTHORIZED, transitions(
                entry(EventType.CAPTURE, TransactionState.CAPTURED),
                entry(EventType.VOID, TransactionState.VOIDED),
                entry(EventType.EXPIRE, TransactionState.EXPIRED)));

        // CAPTURED: the money has moved. It can settle in the scheme batch, or
        // be given back. VOID is deliberately absent: voiding is for releasing
        // a hold, and once funds have moved the correction is a refund, which
        // writes reversing entries rather than pretending nothing happened.
        table.put(TransactionState.CAPTURED, transitions(
                entry(EventType.SETTLE, TransactionState.SETTLED),
                entry(EventType.REFUND, TransactionState.REFUNDED)));

        // SETTLED: money has moved and cleared. Refunding after settlement is
        // the normal path for a customer return.
        table.put(TransactionState.SETTLED, transitions(
                entry(EventType.REFUND, TransactionState.REFUNDED)));

        // Terminal states accept nothing further.
        table.put(TransactionState.REFUNDED, transitions());
        table.put(TransactionState.VOIDED, transitions());
        table.put(TransactionState.EXPIRED, transitions());

        return table;
    }

    private TransactionStateMachine() {
    }

    /**
     * Applies an event, returning the state it leads to.
     *
     * @throws IllegalTransitionException when the event is not legal in this
     *                                    state
     */
    public static TransactionState next(TransactionState from, EventType event) {
        return peek(from, event)
                .orElseThrow(() -> new IllegalTransitionException(from, event));
    }

    /** Non-throwing form, for callers deciding whether to attempt something. */
    public static Optional<TransactionState> peek(TransactionState from, EventType event) {
        return Optional.ofNullable(TRANSITIONS.get(from)).map(row -> row.get(event));
    }

    public static boolean isLegal(TransactionState from, EventType event) {
        return peek(from, event).isPresent();
    }

    /**
     * Every event legal in this state. Empty for terminal states.
     *
     * No null check on the row: the table names every state, which
     * tableCoversEveryState asserts, so a missing row is not a reachable case.
     */
    public static Set<EventType> legalEventsFrom(TransactionState from) {
        Map<EventType, TransactionState> row = TRANSITIONS.get(from);
        return row.isEmpty()
                ? EnumSet.noneOf(EventType.class)
                : EnumSet.copyOf(row.keySet());
    }

    /**
     * The whole table, for tests that need to enumerate it.
     *
     * Returned as an unmodifiable view so a caller cannot quietly reshape the
     * rules by mutating what it was handed.
     */
    public static Map<TransactionState, Map<EventType, TransactionState>> table() {
        Map<TransactionState, Map<EventType, TransactionState>> copy =
                new EnumMap<>(TransactionState.class);
        TRANSITIONS.forEach((state, row) -> copy.put(state, Map.copyOf(row)));
        return Map.copyOf(copy);
    }

    /** Where a transaction starts before any event has been applied. */
    public static TransactionState initialState() {
        return TransactionState.NEW;
    }

    @SafeVarargs
    private static Map<EventType, TransactionState> transitions(
            Map.Entry<EventType, TransactionState>... entries) {
        Map<EventType, TransactionState> row = new EnumMap<>(EventType.class);
        for (Map.Entry<EventType, TransactionState> entry : entries) {
            row.put(entry.getKey(), entry.getValue());
        }
        return row;
    }

    private static Map.Entry<EventType, TransactionState> entry(EventType event, TransactionState to) {
        return Map.entry(event, to);
    }
}
