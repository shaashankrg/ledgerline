package com.ledgerline.domain;

/** An account's currency does not match the currency declared on the request. */
public class CurrencyMismatchException extends TransferException {

    public CurrencyMismatchException(long accountId, String accountCurrency, String requestCurrency) {
        super("account " + accountId + " is denominated in " + accountCurrency
                + " but the request declared " + requestCurrency);
    }
}
