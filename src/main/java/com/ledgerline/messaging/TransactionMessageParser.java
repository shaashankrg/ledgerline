package com.ledgerline.messaging;

import org.springframework.stereotype.Component;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.ledgerline.transfer.TransferRequest;

/**
 * Turns a raw message payload into a {@link TransferRequest}.
 *
 * Parsing lives here rather than in the listener so the listener stays a
 * handful of lines, and so every way a payload can be unusable surfaces as one
 * exception type the listener can route without inspecting it.
 *
 * This is not validation. Whether the accounts exist, the currencies agree, or
 * the amount fits the ledger is TransferService's business, and duplicating any
 * of it here would mean two places to change when a rule moves.
 */
@Component
class TransactionMessageParser {

    private final ObjectMapper objectMapper;

    TransactionMessageParser(ObjectMapper objectMapper) {
        this.objectMapper = objectMapper;
    }

    /**
     * @throws PermanentMessageException if the payload is not a well-formed
     *                                   message -- bad JSON, a missing field,
     *                                   or an amount that is not a number
     */
    TransferRequest parse(String payload) {
        TransactionMessage message;
        try {
            message = objectMapper.readValue(payload, TransactionMessage.class);
        } catch (Exception e) {
            throw new PermanentMessageException("message payload could not be parsed", e);
        }

        requirePresent(message.transactionId(), "transactionId");
        requirePresent(message.fromAccountId(), "fromAccountId");
        requirePresent(message.toAccountId(), "toAccountId");
        requirePresent(message.amount(), "amount");
        requirePresent(message.currency(), "currency");

        // The transaction id from the message is the idempotency key. It is
        // carried through unchanged so a redelivery of the same record claims
        // the same key and is recognized as a replay rather than written twice.
        return new TransferRequest(
                message.transactionId(),
                message.fromAccountId(),
                message.toAccountId(),
                message.amount(),
                message.currency());
    }

    private static void requirePresent(Object value, String field) {
        if (value == null) {
            throw new PermanentMessageException(field + " is missing from the message", null);
        }
    }
}
