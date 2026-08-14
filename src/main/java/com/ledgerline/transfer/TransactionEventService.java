package com.ledgerline.transfer;

import java.util.List;
import java.util.Optional;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import com.ledgerline.domain.EntryGroup;
import com.ledgerline.domain.EntryPolicy;
import com.ledgerline.domain.EventType;
import com.ledgerline.domain.IdempotencyKeyReuseException;
import com.ledgerline.domain.IllegalTransitionException;
import com.ledgerline.domain.TransactionEvent;
import com.ledgerline.domain.TransactionState;
import com.ledgerline.domain.TransactionStateMachine;
import com.ledgerline.domain.TransitionConflictException;
import com.ledgerline.ledger.LedgerWriter;
import com.ledgerline.ledger.ParkedEventRepository;
import com.ledgerline.ledger.ParkedEventRepository.ParkedEvent;
import com.ledgerline.ledger.TransactionStateRepository;
import com.ledgerline.metrics.LedgerlineMetrics;

/**
 * Applies a lifecycle event to a transaction. The only writer to the ledger.
 *
 * Four things happen, in one database transaction: the event is validated, its
 * eventId is claimed for idempotency, the state machine gates the transition
 * and advances it by compare-and-swap, and any entries the event produces are
 * written.
 *
 * They share a transaction because splitting them would let a crash leave a
 * transaction whose recorded state disagrees with its ledger -- captured
 * according to the state row, with no entries behind it, or the reverse. Either
 * way the two would have to be reconciled by hand, and nothing in the system
 * could say which was right.
 *
 * There are two distinct guards here and they answer different questions.
 * Idempotency asks "have I already applied this exact message?" and is
 * per-event. The state machine asks "can this kind of event follow what has
 * happened so far?" and is per-transaction. A redelivered capture trips the
 * first; a capture arriving before its authorize trips the second.
 */
@Service
public class TransactionEventService {

    private static final Logger log = LoggerFactory.getLogger(TransactionEventService.class);

    private final EventValidator validator;
    private final TransactionStateRepository states;
    private final LedgerWriter ledgerWriter;
    private final ParkedEventRepository parkedEvents;
    private final LedgerlineMetrics metrics;

    TransactionEventService(EventValidator validator,
            TransactionStateRepository states,
            LedgerWriter ledgerWriter,
            ParkedEventRepository parkedEvents,
            LedgerlineMetrics metrics) {
        this.validator = validator;
        this.states = states;
        this.ledgerWriter = ledgerWriter;
        this.parkedEvents = parkedEvents;
        this.metrics = metrics;
    }

    /**
     * Applies several events for one record as a single unit.
     *
     * Exists for a message that names no lifecycle step and so is read as a
     * complete transfer -- an implicit AUTHORIZE immediately followed by its
     * CAPTURE. Applying them in one transaction means a failure partway leaves
     * neither applied, so a redelivery of the same record starts clean rather
     * than resuming a transaction stuck AUTHORIZED with nothing capturing it.
     *
     * @return the result of the last event applied
     */
    @Transactional
    public EventResult applyAll(List<TransactionEvent> events) {
        EventResult last = null;
        for (TransactionEvent event : events) {
            last = apply(event);
        }
        return last;
    }

    /**
     * Applies an event, parking it if it has merely arrived early.
     *
     * An event the state machine refuses splits two ways, and the split is the
     * point of the parking mechanism:
     *
     * EARLY -- the transaction has no state row yet, so nothing has authorized
     * it. A capture in that position is not wrong, it is ahead of its
     * authorize, which a partitioned topic with retries produces routinely.
     * These are parked and replayed when the authorize lands.
     *
     * WRONG -- the transaction has a state, and the event cannot follow it. A
     * capture against a REFUNDED transaction is not early; nothing later will
     * make it legal. These propagate as IllegalTransitionException and are dead
     * lettered by the consumer.
     *
     * @return the outcome, carrying the resulting state and whether this was a
     *         redelivery of an event already applied
     * @throws com.ledgerline.domain.IllegalTransitionException if the event can
     *         never follow the transaction's current state
     * @throws IdempotencyKeyReuseException if the eventId was already used for
     *         a materially different payload
     * @throws TransitionConflictException if another writer advanced the
     *         transaction first
     */
    @Transactional
    public EventResult apply(TransactionEvent event) {
        validator.validate(event);

        String payloadHash = EventPayloadHasher.hash(event);
        Optional<Long> claimedRowId = ledgerWriter.claimEvent(event.eventId(), payloadHash,
                event.type() + " " + event.externalTxnId(), event.merchantId());

        if (claimedRowId.isEmpty()) {
            // Someone already applied this eventId. Either it is a genuine
            // redelivery, in which case the state is already correct and there
            // is nothing to do, or the key was reused for a different payload,
            // which is an error.
            //
            // requireMatchingPayload throws on a genuine mismatch (counted at
            // its own throw site, LedgerWriter.requireMatchingPayload) rather
            // than returning here -- reaching this line means the payload
            // matched, so this is the harmless-redelivery counter, not the
            // mismatch one.
            ledgerWriter.requireMatchingPayload(event.eventId(), payloadHash);
            metrics.idempotentDuplicateRejected();
            return new EventResult(currentStateOf(event), true);
        }

        boolean transactionIsNew = states.createIfAbsent(
                event.externalTxnId(), TransactionStateMachine.initialState());

        TransactionState current = states.find(event.externalTxnId())
                .orElseThrow(() -> new IllegalStateException(
                        "state row vanished for " + event.externalTxnId()));

        if (!TransactionStateMachine.isLegal(current, event.type())) {
            return parkOrReject(event, current, transactionIsNew);
        }

        TransactionState next = TransactionStateMachine.next(current, event.type());

        // Compare-and-swap. Losing means another writer moved the transaction
        // between the read above and this write, which is a race rather than a
        // bad event.
        if (!states.compareAndAdvance(event.externalTxnId(), current, next)) {
            throw new TransitionConflictException(event.externalTxnId(), current);
        }

        writeEntries(event, claimedRowId.get());
        metrics.paymentProcessed(event.type().name());

        // An authorize is what parked events were waiting for. Draining here,
        // inside the same transaction, means the drain commits or rolls back
        // with the authorize that enabled it.
        if (event.type() == EventType.AUTHORIZE) {
            drainParkedEvents(event.externalTxnId());
        }

        return new EventResult(next, false);
    }

    /**
     * Decides whether a rejected event is early or simply wrong.
     *
     * Early means the transaction is still in its initial state -- nothing has
     * happened to it yet, so an event that is not an authorize has plainly
     * overtaken one. Anything else has a history the event contradicts, and no
     * amount of waiting reconciles it.
     *
     * The idempotency claim is released when parking, because the event has not
     * been applied: it will claim the key for real when it drains. Leaving the
     * claim in place would make the drained event look like a replay and
     * silently skip it.
     */
    private EventResult parkOrReject(
            TransactionEvent event, TransactionState current, boolean transactionIsNew) {

        boolean early = current == TransactionStateMachine.initialState();

        if (!early) {
            // Has a history this event contradicts. Permanent.
            metrics.stateTransitionRejected(current.name(), event.type().name());
            throw new IllegalTransitionException(current, event.type());
        }

        ledgerWriter.releaseClaim(event.eventId());
        parkedEvents.park(event);
        metrics.parkedEvent("early");

        if (transactionIsNew) {
            // This event created the state row and is not going to use it.
            // Leaving a NEW row behind would make a later genuine authorize
            // look like it were resuming something.
            states.deleteIfUntouched(event.externalTxnId());
        }

        log.debug("Parked {} for transaction {}: arrived before its authorize",
                event.type(), event.externalTxnId());

        return new EventResult(current, false, true);
    }

    /**
     * Replays a transaction's parked events, oldest first.
     *
     * Idempotent by construction: each row is claimed with a conditional update
     * before being applied, so a redelivered authorize that drains again finds
     * every row already claimed and applies nothing. That is what keeps a
     * duplicate authorize from double-applying the captures behind it.
     *
     * A parked event that is still illegal after draining is abandoned rather
     * than left pending. Leaving it pending would mean every subsequent
     * authorize for that transaction retries it forever, and it has already
     * demonstrated that the state it needs is not the state it got. Abandoned
     * rows stay in the table with a reason, so the event is inspectable rather
     * than lost -- which is the difference between giving up on applying it and
     * pretending it never arrived.
     */
    private void drainParkedEvents(String externalTxnId) {
        for (ParkedEvent parked : parkedEvents.pendingFor(externalTxnId)) {
            if (!parkedEvents.claimForDrain(parked.parkedId())) {
                // Another drain reserved it first.
                continue;
            }

            try {
                EventResult result = apply(parked.event());

                if (result.parked()) {
                    // Replaying it parked it again, which means the drain is
                    // not making progress on this row. Abandoning rather than
                    // leaving it pending is what stops an authorize-park-drain
                    // loop from repeating indefinitely.
                    parkedEvents.markAbandoned(parked.parkedId(),
                            "still not applicable when drained");
                } else {
                    parkedEvents.markApplied(parked.parkedId());
                    metrics.parkedEventDrained();
                }
            } catch (IllegalTransitionException e) {
                // Still illegal even now. Not retried again: see above.
                parkedEvents.markAbandoned(parked.parkedId(), e.getMessage());
                log.warn("Abandoned parked event {} for transaction {}: {}",
                        parked.event().eventId(), externalTxnId, e.getMessage());
            }
        }
    }

    private TransactionState currentStateOf(TransactionEvent event) {
        return states.find(event.externalTxnId())
                .orElse(TransactionStateMachine.initialState());
    }

    /**
     * Writes the entries the event produces, if any.
     *
     * Events that only advance state produce an empty group and write nothing,
     * which is why this is not simply a call to recordEntryGroup.
     */
    private void writeEntries(TransactionEvent event, long transactionRowId) {
        EntryGroup group = EntryPolicy.entriesFor(event);
        if (group.isEmpty()) {
            return;
        }

        // Written through LedgerWriter rather than by this class directly, so
        // there stays exactly one component that inserts into ledger_entries.
        ledgerWriter.recordEntryGroup(transactionRowId, group);
    }

    /** Current state of a transaction, for callers that need to inspect it. */
    public Optional<TransactionState> stateOf(String externalTxnId) {
        return states.find(externalTxnId);
    }

    /**
     * Outcome of applying an event.
     *
     * @param state    where the transaction sits afterwards
     * @param replayed true when this event had already been applied, so nothing
     *                 was written this time
     * @param parked   true when the event arrived before it could be applied
     *                 and is waiting for its authorize. Not a failure: the
     *                 consumer acknowledges these normally, because the event
     *                 is safely stored and redelivering it would achieve
     *                 nothing.
     */
    public record EventResult(TransactionState state, boolean replayed, boolean parked) {

        public EventResult(TransactionState state, boolean replayed) {
            this(state, replayed, false);
        }
    }
}
