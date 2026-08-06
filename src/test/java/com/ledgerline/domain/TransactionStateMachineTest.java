package com.ledgerline.domain;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;

import java.util.ArrayList;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

/**
 * Exhaustive coverage of the transition table.
 *
 * Every (state, event) pair in the cross product is exercised: the legal ones
 * assert their destination, the illegal ones assert they throw. The pairs are
 * generated from the enums rather than listed by hand, so adding a state or an
 * event automatically produces new cases instead of silently going untested.
 */
class TransactionStateMachineTest {

    /** Every legal transition, stated independently of the implementation. */
    private static Stream<Arguments> legalTransitions() {
        return Stream.of(
                Arguments.of(TransactionState.NEW, EventType.AUTHORIZE, TransactionState.AUTHORIZED),
                Arguments.of(TransactionState.AUTHORIZED, EventType.CAPTURE, TransactionState.CAPTURED),
                Arguments.of(TransactionState.AUTHORIZED, EventType.VOID, TransactionState.VOIDED),
                Arguments.of(TransactionState.AUTHORIZED, EventType.EXPIRE, TransactionState.EXPIRED),
                Arguments.of(TransactionState.CAPTURED, EventType.SETTLE, TransactionState.SETTLED),
                Arguments.of(TransactionState.CAPTURED, EventType.REFUND, TransactionState.REFUNDED),
                Arguments.of(TransactionState.SETTLED, EventType.REFUND, TransactionState.REFUNDED));
    }

    /**
     * The complement: every pair not named above.
     *
     * Derived by subtraction rather than enumerated, so it cannot drift out of
     * step with the legal set.
     */
    private static Stream<Arguments> illegalTransitions() {
        List<Arguments> legal = legalTransitions().toList();
        List<Arguments> illegal = new ArrayList<>();

        for (TransactionState state : TransactionState.values()) {
            for (EventType event : EventType.values()) {
                boolean isLegal = legal.stream().anyMatch(argument ->
                        argument.get()[0] == state && argument.get()[1] == event);
                if (!isLegal) {
                    illegal.add(Arguments.of(state, event));
                }
            }
        }
        return illegal.stream();
    }

    @ParameterizedTest(name = "{0} + {1} -> {2}")
    @MethodSource("legalTransitions")
    void legalTransitionMovesToExpectedState(
            TransactionState from, EventType event, TransactionState expected) {

        assertThat(TransactionStateMachine.next(from, event)).isEqualTo(expected);
        assertThat(TransactionStateMachine.isLegal(from, event)).isTrue();
        assertThat(TransactionStateMachine.peek(from, event)).contains(expected);
    }

    @ParameterizedTest(name = "{0} + {1} is rejected")
    @MethodSource("illegalTransitions")
    void illegalTransitionThrows(TransactionState from, EventType event) {
        assertThatExceptionOfType(IllegalTransitionException.class)
                .isThrownBy(() -> TransactionStateMachine.next(from, event))
                .satisfies(thrown -> {
                    assertThat(thrown.from()).isEqualTo(from);
                    assertThat(thrown.event()).isEqualTo(event);
                    assertThat(thrown.getMessage()).contains(event.name(), from.name());
                });

        assertThat(TransactionStateMachine.isLegal(from, event)).isFalse();
        assertThat(TransactionStateMachine.peek(from, event)).isEmpty();
    }

    /**
     * The cross product is fully accounted for.
     *
     * Guards against the failure this suite exists to prevent: a pair that is
     * neither asserted legal nor asserted illegal, and so never tested at all.
     */
    @Test
    void everyStateEventPairIsClassified() {
        int states = TransactionState.values().length;
        int events = EventType.values().length;
        long legal = legalTransitions().count();
        long illegal = illegalTransitions().count();

        assertThat(legal + illegal)
                .as("every (state, event) pair must be either legal or illegal")
                .isEqualTo((long) states * events);

        System.out.println("=== TRANSITION TABLE ===");
        System.out.println("states               : " + states);
        System.out.println("events               : " + events);
        System.out.println("total pairs          : " + states * events);
        System.out.println("legal transitions    : " + legal);
        System.out.println("illegal transitions  : " + illegal);
        System.out.println("=== END ===");
    }

    @Test
    void tableMatchesTheLegalTransitionsAssertedHere() {
        Map<TransactionState, Map<EventType, TransactionState>> table =
                TransactionStateMachine.table();

        long entriesInTable = table.values().stream().mapToLong(Map::size).sum();
        assertThat(entriesInTable)
                .as("the table must hold exactly the legal transitions and no others")
                .isEqualTo(legalTransitions().count());

        legalTransitions().forEach(argument -> {
            TransactionState from = (TransactionState) argument.get()[0];
            EventType event = (EventType) argument.get()[1];
            TransactionState expected = (TransactionState) argument.get()[2];
            assertThat(table.get(from).get(event)).isEqualTo(expected);
        });
    }

    @Test
    void tableIsNotModifiableByCallers() {
        Map<TransactionState, Map<EventType, TransactionState>> table =
                TransactionStateMachine.table();

        assertThatExceptionOfType(UnsupportedOperationException.class)
                .isThrownBy(() -> table.put(TransactionState.NEW, Map.of()));
        assertThatExceptionOfType(UnsupportedOperationException.class)
                .isThrownBy(() -> table.get(TransactionState.NEW)
                        .put(EventType.REFUND, TransactionState.REFUNDED));
    }

    @Test
    void terminalStatesAcceptNothing() {
        for (TransactionState state : TransactionState.values()) {
            if (state.isTerminal()) {
                assertThat(TransactionStateMachine.legalEventsFrom(state))
                        .as("%s is terminal and must accept no events", state)
                        .isEmpty();
            } else {
                assertThat(TransactionStateMachine.legalEventsFrom(state))
                        .as("%s is not terminal and must accept at least one event", state)
                        .isNotEmpty();
            }
        }
    }

    @Test
    void legalEventsFromReportsTheRow() {
        assertThat(TransactionStateMachine.legalEventsFrom(TransactionState.NEW))
                .containsExactly(EventType.AUTHORIZE);
        assertThat(TransactionStateMachine.legalEventsFrom(TransactionState.AUTHORIZED))
                .containsExactlyInAnyOrder(EventType.CAPTURE, EventType.VOID, EventType.EXPIRE);
        assertThat(TransactionStateMachine.legalEventsFrom(TransactionState.CAPTURED))
                .containsExactlyInAnyOrder(EventType.SETTLE, EventType.REFUND);
        assertThat(TransactionStateMachine.legalEventsFrom(TransactionState.SETTLED))
                .containsExactly(EventType.REFUND);
    }

    /**
     * Every state has a row in the table.
     *
     * legalEventsFrom relies on this: it dereferences the row without a null
     * check, which is only safe because a state cannot be missing.
     */
    @Test
    void tableCoversEveryState() {
        Map<TransactionState, Map<EventType, TransactionState>> table =
                TransactionStateMachine.table();

        assertThat(table.keySet())
                .as("a state with no row would make legalEventsFrom throw")
                .containsExactlyInAnyOrder(TransactionState.values());
    }

    @Test
    void transactionsBeginInNew() {
        assertThat(TransactionStateMachine.initialState()).isEqualTo(TransactionState.NEW);
        assertThat(TransactionState.NEW.isTerminal()).isFalse();
    }

    /** Every state must be reachable, or it is dead weight in the model. */
    @Test
    void everyStateIsReachable() {
        EnumSet<TransactionState> reachable = EnumSet.of(TransactionStateMachine.initialState());
        boolean grew = true;

        while (grew) {
            grew = false;
            for (TransactionState state : EnumSet.copyOf(reachable)) {
                for (EventType event : TransactionStateMachine.legalEventsFrom(state)) {
                    if (reachable.add(TransactionStateMachine.next(state, event))) {
                        grew = true;
                    }
                }
            }
        }

        assertThat(reachable)
                .as("a state no sequence of events can reach does not belong in the model")
                .containsExactlyInAnyOrder(TransactionState.values());
    }

    /** The happy path, walked end to end. */
    @Test
    void authorizeCaptureSettleIsTheNormalLifecycle() {
        TransactionState state = TransactionStateMachine.initialState();
        state = TransactionStateMachine.next(state, EventType.AUTHORIZE);
        assertThat(state).isEqualTo(TransactionState.AUTHORIZED);

        state = TransactionStateMachine.next(state, EventType.CAPTURE);
        assertThat(state).isEqualTo(TransactionState.CAPTURED);

        state = TransactionStateMachine.next(state, EventType.SETTLE);
        assertThat(state).isEqualTo(TransactionState.SETTLED);

        // Settled is a resting state, not the end: a refund can still follow.
        assertThat(state.isTerminal()).isFalse();
        assertThat(TransactionStateMachine.next(state, EventType.REFUND))
                .isEqualTo(TransactionState.REFUNDED);
        assertThat(TransactionState.REFUNDED.isTerminal()).isTrue();
    }

    /** Capturing without authorizing is the ordering violation that matters. */
    @Test
    void captureBeforeAuthorizeIsRejected() {
        assertThatExceptionOfType(IllegalTransitionException.class)
                .isThrownBy(() -> TransactionStateMachine.next(
                        TransactionStateMachine.initialState(), EventType.CAPTURE));
    }

    /** A settled payment cannot be voided; it has to be refunded. */
    @Test
    void voidAfterCaptureIsRejected() {
        assertThatExceptionOfType(IllegalTransitionException.class)
                .isThrownBy(() -> TransactionStateMachine.next(
                        TransactionState.CAPTURED, EventType.VOID));
    }

    /** Refunding twice is rejected -- REFUNDED accepts nothing. */
    @Test
    void doubleRefundIsRejected() {
        assertThatExceptionOfType(IllegalTransitionException.class)
                .isThrownBy(() -> TransactionStateMachine.next(
                        TransactionState.REFUNDED, EventType.REFUND));
    }
}
