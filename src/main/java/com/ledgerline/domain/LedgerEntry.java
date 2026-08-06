package com.ledgerline.domain;

import java.math.BigDecimal;
import java.util.Objects;

/**
 * A single signed movement against one account.
 *
 * Negative debits the account, positive credits it. Sign carries the direction
 * rather than a separate field, so an entry cannot claim to be a debit while
 * holding a positive amount.
 *
 * This is the in-memory shape produced by applying an event. Persisting it is
 * LedgerWriter's job, and this record deliberately knows nothing about that.
 */
public record LedgerEntry(long accountId, BigDecimal amount, String currency) {

    public LedgerEntry {
        Objects.requireNonNull(amount, "amount");
        Objects.requireNonNull(currency, "currency");

        if (amount.signum() == 0) {
            // A zero entry is meaningless and always signals a bug upstream,
            // matching the CHECK constraint the ledger table carries.
            throw new IllegalArgumentException("a ledger entry cannot be zero");
        }
    }

    public boolean isDebit() {
        return amount.signum() < 0;
    }
}
