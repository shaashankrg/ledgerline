package com.ledgerline.messaging;

/**
 * A message that will never succeed, however many times it is retried.
 *
 * Raised for payloads that cannot be parsed and for business rules the message
 * violates. Retrying either would block the partition forever, since the input
 * is what is wrong and nothing about redelivery changes it.
 */
class PermanentMessageException extends RuntimeException {

    PermanentMessageException(String message, Throwable cause) {
        super(message, cause);
    }
}
