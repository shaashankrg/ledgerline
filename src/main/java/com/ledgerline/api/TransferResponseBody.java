package com.ledgerline.api;

import java.math.BigDecimal;

import com.fasterxml.jackson.databind.annotation.JsonSerialize;

/**
 * The POST /api/v1/transfers response body.
 *
 * Acknowledges that the transfer was accepted for processing, not that it has
 * been recorded. The ledger row does not exist yet when this is returned, so
 * there is no database-generated id to report -- transactionId is the
 * client-supplied Idempotency-Key, which is what the consumer will use to write
 * the entries and what the client can look the transfer up by afterwards.
 *
 * That makes the id a String here rather than the numeric ledger id the
 * synchronous path used to return.
 */
public record TransferResponseBody(

        String transactionId,

        Long fromAccountId,

        Long toAccountId,

        /** Pinned to the ledger's scale, plain notation, never scientific. */
        @JsonSerialize(using = PlainDecimalSerializer.class)
        BigDecimal amount,

        String currency) {
}
