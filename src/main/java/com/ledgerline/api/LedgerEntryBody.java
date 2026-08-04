package com.ledgerline.api;

import java.math.BigDecimal;
import java.time.Instant;

import com.fasterxml.jackson.annotation.JsonFormat;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;

/**
 * One ledger entry as seen from a particular account.
 *
 * The sign of the amount is from that account's point of view: negative for the
 * debit side, positive for the credit side.
 */
record LedgerEntryBody(

        @JsonFormat(shape = JsonFormat.Shape.STRING)
        long entryId,

        @JsonFormat(shape = JsonFormat.Shape.STRING)
        long transactionId,

        @JsonSerialize(using = PlainDecimalSerializer.class)
        BigDecimal amount,

        String currency,

        /** ISO-8601 UTC, so clients need no format negotiation. */
        Instant createdAt,

        /**
         * The other account in the same transaction.
         *
         * Null when the transaction has no single counterparty -- nothing today
         * writes such a transaction, but the read path does not assume the pair
         * shape it was not responsible for creating.
         */
        @JsonFormat(shape = JsonFormat.Shape.STRING)
        Long counterpartyAccountId) {
}
