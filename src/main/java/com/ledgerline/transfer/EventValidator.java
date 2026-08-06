package com.ledgerline.transfer;

import java.util.Locale;

import org.springframework.dao.EmptyResultDataAccessException;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.stereotype.Component;

import com.ledgerline.domain.AccountNotFoundException;
import com.ledgerline.domain.AmountScaleException;
import com.ledgerline.domain.CurrencyMismatchException;
import com.ledgerline.domain.SameAccountTransferException;
import com.ledgerline.domain.TransactionEvent;

/**
 * Business validation for an incoming event.
 *
 * These rules moved here from TransferService when the transfer path was
 * retired. They are unchanged in substance -- the same five checks throwing the
 * same five exception types -- because they were never about transfers
 * specifically. They are about whether a money movement makes sense against the
 * accounts it names, which is equally true of a capture or a refund.
 *
 * Only events that actually move money are checked. An authorize or a settle
 * names no accounts and carries no amount, so there is nothing here to verify;
 * the state machine is what governs those.
 */
@Component
public class EventValidator {

    /** Matches ledger_entries.amount NUMERIC(19,4). */
    static final int LEDGER_SCALE = 4;

    private final NamedParameterJdbcTemplate jdbc;

    EventValidator(NamedParameterJdbcTemplate jdbc) {
        this.jdbc = jdbc;
    }

    /**
     * @throws SameAccountTransferException if both sides are the same account
     * @throws AmountScaleException if the amount carries more precision than
     *         the ledger stores
     * @throws AccountNotFoundException if either account does not exist
     * @throws CurrencyMismatchException if an account's currency differs from
     *         the event's
     */
    public void validate(TransactionEvent event) {
        if (!event.movesMoney()) {
            // Nothing to check: state-only events name no accounts.
            return;
        }

        if (event.fromAccountId().equals(event.toAccountId())) {
            throw new SameAccountTransferException(event.fromAccountId());
        }

        // Rejected rather than rounded, so a sub-cent fraction can never be
        // silently dropped on the way into the ledger.
        if (event.amount().scale() > LEDGER_SCALE) {
            throw new AmountScaleException(event.amount(), LEDGER_SCALE);
        }

        String currency = event.currency().toUpperCase(Locale.ROOT);
        requireAccountInCurrency(event.fromAccountId(), currency);
        requireAccountInCurrency(event.toAccountId(), currency);
    }

    private void requireAccountInCurrency(long accountId, String requestCurrency) {
        String accountCurrency;
        try {
            accountCurrency = jdbc.queryForObject(
                    "SELECT currency FROM accounts WHERE id = :id",
                    new MapSqlParameterSource("id", accountId),
                    String.class);
        } catch (EmptyResultDataAccessException e) {
            throw new AccountNotFoundException(accountId);
        }

        // CHAR(3) comes back space-padded when the value is shorter.
        accountCurrency = accountCurrency.trim();

        if (!accountCurrency.equalsIgnoreCase(requestCurrency)) {
            throw new CurrencyMismatchException(accountId, accountCurrency, requestCurrency);
        }
    }
}
