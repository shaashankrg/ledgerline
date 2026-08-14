package com.ledgerline.metrics;

import java.time.Duration;
import java.util.concurrent.ConcurrentHashMap;

import org.springframework.stereotype.Component;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;

/**
 * The single place every counter and timer in the app is registered and
 * incremented from.
 *
 * Centralizing this is what makes the per-counter wiring tests possible: each
 * one has an exact name and label set here, and a test that triggers the
 * behavior the name claims can assert this exact meter moved, rather than
 * guessing at a name a call site constructed inline. It also enforces the
 * cardinality rule in one place -- every method here takes a bounded,
 * enum-shaped label, never a caller-supplied free string like an
 * external_txn_id.
 *
 * Counter/Timer instances are cached per label combination
 * ({@link MeterRegistry#counter} already does this internally via its own
 * registry lookup, but caching here avoids re-resolving the meter on every
 * single call on a hot path).
 */
@Component
public class LedgerlineMetrics {

    private final MeterRegistry registry;

    private final ConcurrentHashMap<String, Counter> paymentsProcessed = new ConcurrentHashMap<>();
    private final Counter idempotentDuplicatesRejected;
    private final Counter payloadHashMismatches;
    private final ConcurrentHashMap<String, Counter> dlqMessages = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, Counter> parkedEvents = new ConcurrentHashMap<>();
    private final Counter parkedEventsDrained;
    private final ConcurrentHashMap<String, Counter> stateTransitionsRejected = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, Counter> reconExceptions = new ConcurrentHashMap<>();
    private final Timer endToEndLatency;

    LedgerlineMetrics(MeterRegistry registry) {
        this.registry = registry;

        this.idempotentDuplicatesRejected = Counter.builder("idempotent_duplicates_rejected_total")
                .description("Events whose eventId was already claimed by a matching payload -- a harmless redelivery")
                .register(registry);

        this.payloadHashMismatches = Counter.builder("payload_hash_mismatches_total")
                .description("Events whose eventId was reused for a materially different payload")
                .register(registry);

        this.parkedEventsDrained = Counter.builder("parked_events_drained_total")
                .description("Parked events successfully replayed once their authorize arrived")
                .register(registry);

        // End-to-end latency: publish (Kafka header timestamp) to DB commit.
        // Single-host measurement -- see the header-stamping site in
        // TransactionProducer and the reading site in TransactionConsumer for
        // the clock-sync caveat this depends on.
        this.endToEndLatency = Timer.builder("ledgerline_end_to_end_latency")
                .description("Time from Kafka publish (producer-stamped header) to ledger DB commit. "
                        + "Single-host clock only -- see TransactionConsumer for the caveat.")
                .publishPercentileHistogram()
                .register(registry);
    }

    /** payments_processed_total{event_type}. Incremented once an event's entries have committed. */
    public void paymentProcessed(String eventType) {
        paymentsProcessed.computeIfAbsent(eventType, type -> Counter.builder("payments_processed_total")
                        .tag("event_type", type)
                        .description("Events whose entries were written to the ledger")
                        .register(registry))
                .increment();
    }

    /** idempotent_duplicates_rejected_total. A redelivery recognized as already-applied. */
    public void idempotentDuplicateRejected() {
        idempotentDuplicatesRejected.increment();
    }

    /** payload_hash_mismatches_total. An eventId reused for a different payload. */
    public void payloadHashMismatch() {
        payloadHashMismatches.increment();
    }

    /** dlq_messages_total{reason}. reason is the failing exception's simple class name -- bounded, not per-message. */
    public void dlqMessage(String reason) {
        dlqMessages.computeIfAbsent(reason, r -> Counter.builder("dlq_messages_total")
                        .tag("reason", r)
                        .description("Messages routed to the dead letter topic, by failure reason")
                        .register(registry))
                .increment();
    }

    /** parked_events_total{reason}. reason is bounded (e.g. "early") -- never a transaction id. */
    public void parkedEvent(String reason) {
        parkedEvents.computeIfAbsent(reason, r -> Counter.builder("parked_events_total")
                        .tag("reason", r)
                        .description("Events parked because they arrived before their authorize")
                        .register(registry))
                .increment();
    }

    /** parked_events_drained_total. A parked event successfully replayed. */
    public void parkedEventDrained() {
        parkedEventsDrained.increment();
    }

    /** state_transitions_rejected_total{from,to}. from/to are TransactionState/EventType names -- bounded enums. */
    public void stateTransitionRejected(String from, String event) {
        String key = from + "->" + event;
        stateTransitionsRejected.computeIfAbsent(key, k -> Counter.builder("state_transitions_rejected_total")
                        .tag("from", from)
                        .tag("to", event)
                        .description("Events that could not legally follow the transaction's current state")
                        .register(registry))
                .increment();
    }

    /** recon_exceptions_total{type}. type is ReconExceptionType.name() -- five bounded values. */
    public void reconException(String type) {
        reconExceptions.computeIfAbsent(type, t -> Counter.builder("recon_exceptions_total")
                        .tag("type", t)
                        .description("Reconciliation exceptions raised, by type")
                        .register(registry))
                .increment();
    }

    /** Records one message's publish-to-commit latency. */
    public void recordEndToEndLatency(Duration latency) {
        endToEndLatency.record(latency);
    }
}
