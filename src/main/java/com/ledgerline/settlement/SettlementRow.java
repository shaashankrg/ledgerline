package com.ledgerline.settlement;

import java.time.Instant;

/**
 * One row of the network's settlement file.
 *
 * Gross and fee are reported separately, never netted -- see the CSV writer's
 * class comment for why. Both are {@code long} minor units; no field here is
 * ever a floating point type, on the same reasoning the ledger itself is built
 * on: exact arithmetic is required, and a type that cannot guarantee it is
 * disqualified from the money path.
 *
 * @param externalTxnId nullable -- NETWORK_UNKNOWN_TXN produces a row with no
 *                      matching payment on our side.
 */
public record SettlementRow(
        String externalTxnId,
        String merchantId,
        long grossAmountMinor,
        long feeMinor,
        String currency,
        Instant settledAt) {

    public SettlementRow withGrossAmountMinor(long newGross) {
        return new SettlementRow(externalTxnId, merchantId, newGross, feeMinor, currency, settledAt);
    }

    public SettlementRow withFeeMinor(long newFee) {
        return new SettlementRow(externalTxnId, merchantId, grossAmountMinor, newFee, currency, settledAt);
    }

    public SettlementRow withSettledAt(Instant newSettledAt) {
        return new SettlementRow(externalTxnId, merchantId, grossAmountMinor, feeMinor, currency, newSettledAt);
    }
}
