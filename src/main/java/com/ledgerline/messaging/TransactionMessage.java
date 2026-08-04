package com.ledgerline.messaging;

import java.math.BigDecimal;

import com.fasterxml.jackson.annotation.JsonFormat;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;

/**
 * A transaction published to the {@code transactions} topic.
 *
 * @param transactionId the idempotency key, supplied by the producer. It is
 *                      what lets a consumer recognize a redelivered message as
 *                      the same transaction, so it must survive redelivery
 *                      unchanged and must never be generated downstream --
 *                      a consumer-side id would differ on every delivery and
 *                      defeat the deduplication it exists for.
 */
public record TransactionMessage(

        String transactionId,

        @JsonFormat(shape = JsonFormat.Shape.STRING)
        Long fromAccountId,

        @JsonFormat(shape = JsonFormat.Shape.STRING)
        Long toAccountId,

        /*
         * String, never a JSON number. A JSON number is parsed as a double by
         * most clients, and a double cannot represent every decimal exactly --
         * the value would be corrupted in transit with nothing to signal it.
         * The same reasoning as the HTTP layer's amount handling.
         */
        @JsonSerialize(using = PlainDecimalMessageSerializer.class)
        BigDecimal amount,

        String currency) {
}
