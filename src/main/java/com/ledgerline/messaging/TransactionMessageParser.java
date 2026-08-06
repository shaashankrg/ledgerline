package com.ledgerline.messaging;

import java.time.Instant;
import java.util.List;

import org.springframework.stereotype.Component;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.ledgerline.domain.EntryPolicy;
import com.ledgerline.domain.EventType;
import com.ledgerline.domain.TransactionEvent;

/**
 * Turns a raw message payload into one or more {@link TransactionEvent}s.
 *
 * Parsing lives here rather than in the listener so the listener stays a
 * handful of lines, and so every way a payload can be unusable surfaces as one
 * exception type the listener can route without inspecting it.
 *
 * This is not validation. Whether the accounts exist, the currencies agree, or
 * the amount fits the ledger is EventValidator's business, and duplicating any
 * of it here would mean two places to change when a rule moves.
 *
 * A message names one event when eventType is present. When it is absent, the
 * message predates the event model and describes a complete transfer, which
 * under the state machine is not one step but two: an authorization followed
 * immediately by its capture. Returning a list rather than silently emitting a
 * bare CAPTURE is what keeps such a message legal -- CAPTURE alone against a
 * transaction with no prior AUTHORIZE is exactly the ordering violation the
 * state machine exists to reject.
 */
@Component
class TransactionMessageParser {

    private final ObjectMapper objectMapper;

    TransactionMessageParser(ObjectMapper objectMapper) {
        this.objectMapper = objectMapper;
    }

    /**
     * @return the event or events this record represents, in the order they
     *         must be applied
     * @throws PermanentMessageException if the payload is not a well-formed
     *                                   message -- bad JSON, a missing field,
     *                                   or an amount that is not a number
     */
    List<TransactionEvent> parse(String payload) {
        TransactionMessage message;
        try {
            message = objectMapper.readValue(payload, TransactionMessage.class);
        } catch (Exception e) {
            throw new PermanentMessageException("message payload could not be parsed", e);
        }

        requirePresent(message.transactionId(), "transactionId");
        requirePresent(message.currency(), "currency");

        if (message.eventType() == null) {
            // Legacy shape: one message, one complete movement. AUTHORIZE is
            // synthesized so CAPTURE has something legal to follow.
            requireMovementFields(message);
            Instant now = Instant.now();
            return List.of(
                    movementEvent(message, EventType.AUTHORIZE, message.transactionId() + ":AUTHORIZE", now),
                    movementEvent(message, EventType.CAPTURE, message.transactionId() + ":CAPTURE", now));
        }

        EventType type = message.eventType();

        // Only movements need accounts and an amount. A settle or a void names
        // neither, so requiring them would reject perfectly good events.
        if (EntryPolicy.writesEntries(type)) {
            requireMovementFields(message);
        }

        return List.of(movementEvent(message, type, eventIdOf(message, type), Instant.now()));
    }

    private void requireMovementFields(TransactionMessage message) {
        requirePresent(message.fromAccountId(), "fromAccountId");
        requirePresent(message.toAccountId(), "toAccountId");
        requirePresent(message.amount(), "amount");
    }

    private static TransactionEvent movementEvent(
            TransactionMessage message, EventType type, String eventId, Instant occurredAt) {
        return new TransactionEvent(
                message.transactionId(),
                eventId,
                type,
                message.fromAccountId(),
                message.toAccountId(),
                message.amount(),
                message.currency(),
                occurredAt);
    }

    /**
     * The identity a redelivery must reproduce exactly.
     *
     * Taken from the message when it carries one. When it does not, it is
     * derived from the transaction id and the event type, which is stable
     * across redeliveries of the same record. Generating a fresh id here would
     * defeat deduplication entirely, since every delivery would look new.
     */
    private static String eventIdOf(TransactionMessage message, EventType type) {
        return message.eventId() == null
                ? message.transactionId() + ":" + type.name()
                : message.eventId();
    }

    private static void requirePresent(Object value, String field) {
        if (value == null) {
            throw new PermanentMessageException(field + " is missing from the message", null);
        }
    }
}
