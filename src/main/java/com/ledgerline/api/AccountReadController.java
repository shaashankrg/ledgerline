package com.ledgerline.api;

import java.util.List;
import java.util.Optional;

import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import com.ledgerline.ledger.LedgerQueries;
import com.ledgerline.ledger.LedgerQueries.AccountBalance;
import com.ledgerline.ledger.LedgerQueries.LedgerEntryRow;

/**
 * Read endpoints for inspecting ledger state.
 *
 * A debugging surface, deliberately plain. No transaction wrapper: each handler
 * issues a single query, and a read-only transaction around one statement buys
 * nothing.
 */
@RestController
@RequestMapping("/api/v1/accounts")
class AccountReadController {

    static final int DEFAULT_LIMIT = 50;
    static final int MAX_LIMIT = 200;

    private final LedgerQueries ledgerQueries;

    AccountReadController(LedgerQueries ledgerQueries) {
        this.ledgerQueries = ledgerQueries;
    }

    /** Current balance, summed live over the account's entries. */
    @GetMapping("/{accountId}/balance")
    AccountBalanceBody balance(@PathVariable long accountId) {
        String currency = ledgerQueries.findAccountCurrency(accountId)
                .orElseThrow(() -> new AccountNotFoundInReadException(accountId));

        AccountBalance balance = ledgerQueries.balanceOf(accountId);

        return new AccountBalanceBody(accountId, currency, balance.balance(), balance.entryCount());
    }

    /**
     * A page of the account's entries, newest first.
     *
     * The limit is clamped rather than rejected: a client asking for more than
     * the maximum wants as much as it can get, and failing the request would
     * only make it guess.
     */
    @GetMapping("/{accountId}/entries")
    EntryPageBody entries(
            @PathVariable long accountId,
            @RequestParam(required = false) Integer limit,
            @RequestParam(required = false) String before) {

        String currency = ledgerQueries.findAccountCurrency(accountId)
                .orElseThrow(() -> new AccountNotFoundInReadException(accountId));

        int effectiveLimit = clampLimit(limit);
        Optional<EntryCursor> cursor = Optional.ofNullable(before).map(EntryCursor::decode);

        // One extra row answers "is there another page" without a second query.
        List<LedgerEntryRow> rows = ledgerQueries.entriesBefore(
                accountId,
                cursor.map(EntryCursor::createdAt).orElse(null),
                cursor.map(EntryCursor::id).orElse(null),
                effectiveLimit + 1);

        boolean hasMore = rows.size() > effectiveLimit;
        List<LedgerEntryRow> page = hasMore ? rows.subList(0, effectiveLimit) : rows;

        List<LedgerEntryBody> items = page.stream()
                .map(row -> new LedgerEntryBody(
                        row.id(),
                        row.transactionId(),
                        row.amount(),
                        currency,
                        row.createdAt(),
                        row.counterpartyAccountId()))
                .toList();

        String nextCursor = null;
        if (hasMore) {
            LedgerEntryRow last = page.get(page.size() - 1);
            nextCursor = new EntryCursor(last.createdAt(), last.id()).encode();
        }

        return new EntryPageBody(items, nextCursor);
    }

    private static int clampLimit(Integer requested) {
        if (requested == null) {
            return DEFAULT_LIMIT;
        }
        return Math.max(1, Math.min(requested, MAX_LIMIT));
    }

    /**
     * Signals an unknown account on a read.
     *
     * Distinct from the transfer layer's AccountNotFoundException, which maps to
     * 422 because a transfer naming a missing account is an unprocessable
     * request. Here the account is the resource being addressed, so its absence
     * is a 404.
     */
    static class AccountNotFoundInReadException extends RuntimeException {
        AccountNotFoundInReadException(long accountId) {
            super("account " + accountId + " does not exist");
        }
    }
}
