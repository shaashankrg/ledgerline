package com.ledgerline.api;

import java.math.BigDecimal;

import com.fasterxml.jackson.annotation.JsonFormat;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;

/**
 * A derived account balance.
 *
 * The balance is a live SUM over ledger_entries, not a stored column. Nothing
 * here is cached: a stored balance is a second source of truth that can drift
 * from the entries, and the entries are what the invariant is defined over.
 *
 * entryCount is what the sum was computed over, which makes an unexpected
 * balance diagnosable -- a wrong total with a plausible count is a different
 * bug from a wrong total with no entries at all.
 */
record AccountBalanceBody(

        @JsonFormat(shape = JsonFormat.Shape.STRING)
        long accountId,

        String currency,

        /** String, at the ledger's scale, same as the write path. */
        @JsonSerialize(using = PlainDecimalSerializer.class)
        BigDecimal balance,

        long entryCount) {
}
