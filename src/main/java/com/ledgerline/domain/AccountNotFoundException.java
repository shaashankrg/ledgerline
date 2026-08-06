package com.ledgerline.domain;

/** A referenced account does not exist. */
public class AccountNotFoundException extends TransferException {

    public AccountNotFoundException(long accountId) {
        super("account " + accountId + " does not exist");
    }
}
