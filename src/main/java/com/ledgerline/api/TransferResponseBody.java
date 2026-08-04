package com.ledgerline.api;

import java.math.BigDecimal;

import com.fasterxml.jackson.annotation.JsonFormat;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;

/**
 * The POST /api/v1/transfers response body.
 *
 * Identical in shape and content for an original and its replay -- the only
 * difference between the two responses is the Idempotent-Replay header, so a
 * client never has to branch on whether its retry was the one that won.
 */
public record TransferResponseBody(

        /*
         * Serialized as a string for the same reason as the amount: a JSON
         * number would be parsed into a double by a JS client, and ids beyond
         * 2^53 would silently lose their low bits.
         */
        @JsonFormat(shape = JsonFormat.Shape.STRING)
        long transactionId,

        Long fromAccountId,

        Long toAccountId,

        /** Pinned to the ledger's scale, plain notation, never scientific. */
        @JsonSerialize(using = PlainDecimalSerializer.class)
        BigDecimal amount,

        String currency) {
}
