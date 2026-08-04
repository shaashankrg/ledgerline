package com.ledgerline.transfer;

/** Source and destination are the same account. */
public class SameAccountTransferException extends TransferException {

    public SameAccountTransferException(long accountId) {
        super("fromAccountId and toAccountId must differ, both were " + accountId);
    }
}
