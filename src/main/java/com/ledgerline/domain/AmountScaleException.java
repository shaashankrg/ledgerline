package com.ledgerline.domain;

import java.math.BigDecimal;

/**
 * The amount carries more decimal places than the ledger can store.
 *
 * Rejected rather than rounded: silently dropping a fraction of a cent is the
 * kind of loss a ledger exists to make impossible.
 */
public class AmountScaleException extends TransferException {

    public AmountScaleException(BigDecimal amount, int maxScale) {
        super("amount " + amount.toPlainString() + " has scale " + amount.scale()
                + ", which exceeds the ledger scale of " + maxScale);
    }
}
