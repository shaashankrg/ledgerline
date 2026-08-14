package com.ledgerline.ledger;

import java.math.BigDecimal;
import java.sql.ResultSet;
import java.time.Instant;
import java.util.List;
import java.util.Optional;

import org.springframework.dao.EmptyResultDataAccessException;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.stereotype.Component;

/**
 * Read queries over the ledger.
 *
 * Balances are derived on every call rather than stored. A balance column would
 * be a second source of truth that can drift from the entries, and the entries
 * are what the sum-to-zero invariant is defined over. The account_id index from
 * V2 is what keeps this cheap; EXPLAIN shows a bitmap index scan, not a
 * sequential scan.
 */
@Component
public class LedgerQueries {

    private final NamedParameterJdbcTemplate jdbc;

    LedgerQueries(NamedParameterJdbcTemplate jdbc) {
        this.jdbc = jdbc;
    }

    /**
     * The system-wide invariant delta: the sum of every entry ever written.
     *
     * Zero exactly when every transaction's entries balance -- each balanced
     * pair sums to zero by construction (EntryGroup will not let an unbalanced
     * one be built), so the only way this can be nonzero is a write that
     * bypassed that guarantee, such as a single-sided row inserted directly
     * via SQL. COALESCE keeps an empty table at zero rather than null.
     *
     * "Minor" in the gauge name this backs ({@code ledger_invariant_delta_minor})
     * refers to the finest unit the ledger itself tracks -- {@code
     * ledger_entries.amount} is {@code NUMERIC(19,4)}, i.e. already at its own
     * smallest denomination -- not a conversion to cents. Unlike the
     * reconciliation module's {@code gross_amount_minor} columns, there is no
     * x100 step here: rescaling would either lose the 4th decimal place ledger
     * writes actually use, or invent a unit this table does not have.
     */
    public BigDecimal invariantDeltaMinor() {
        return jdbc.queryForObject(
                "SELECT COALESCE(SUM(amount), 0) FROM ledger_entries",
                new MapSqlParameterSource(), BigDecimal.class);
    }

    /**
     * Every account implicated by a currently-unbalanced transaction, with
     * that account's net contribution to the imbalance.
     *
     * The global delta above can only say "something is wrong somewhere" --
     * two offsetting errors in different accounts sum to zero and vanish from
     * it entirely, the same blind spot that has bitten this project at the
     * application layer more than once. This is the per-item check: it names
     * which transaction is unbalanced and which account holds the orphaned
     * entry, not just that the total moved.
     */
    public List<UnbalancedAccount> unbalancedAccounts() {
        return jdbc.query(
                """
                SELECT e.account_id AS account_id,
                       e.transaction_id AS transaction_id,
                       SUM(e.amount) AS net_amount
                FROM ledger_entries e
                WHERE e.transaction_id IN (
                    SELECT transaction_id FROM ledger_entries
                    GROUP BY transaction_id HAVING SUM(amount) <> 0
                )
                GROUP BY e.account_id, e.transaction_id
                ORDER BY e.transaction_id, e.account_id
                """,
                new MapSqlParameterSource(),
                (rs, rowNum) -> new UnbalancedAccount(
                        rs.getLong("account_id"),
                        rs.getLong("transaction_id"),
                        rs.getBigDecimal("net_amount")));
    }

    /** An account's contribution to one unbalanced transaction. */
    public record UnbalancedAccount(long accountId, long transactionId, BigDecimal netAmount) {
    }

    /** An account's currency, or empty when no such account exists. */
    public Optional<String> findAccountCurrency(long accountId) {
        try {
            String currency = jdbc.queryForObject(
                    "SELECT currency FROM accounts WHERE id = :id",
                    new MapSqlParameterSource("id", accountId),
                    String.class);
            // CHAR(3) comes back space-padded when the value is shorter.
            return Optional.of(currency.trim());
        } catch (EmptyResultDataAccessException e) {
            return Optional.empty();
        }
    }

    /**
     * Live SUM over the account's entries.
     *
     * COALESCE keeps an account with no entries at zero rather than null, so
     * "no activity" and "net zero" render the same way to a client that only
     * wants a number.
     */
    public AccountBalance balanceOf(long accountId) {
        return jdbc.queryForObject(
                "SELECT COALESCE(SUM(amount), 0) AS balance, COUNT(*) AS entry_count "
                        + "FROM ledger_entries WHERE account_id = :accountId",
                new MapSqlParameterSource("accountId", accountId),
                (rs, rowNum) -> new AccountBalance(
                        rs.getBigDecimal("balance"),
                        rs.getLong("entry_count")));
    }

    /**
     * One page of an account's entries, newest first.
     *
     * Keyset pagination on (created_at, id): the WHERE clause names the last row
     * of the previous page and asks for everything strictly older. Unlike
     * OFFSET, rows inserted between requests do not shift the boundary, so a
     * page cannot repeat or skip rows just because the table grew.
     *
     * The id tiebreak is required, not cosmetic -- both entries of a transfer
     * share a created_at, so a timestamp-only comparison could not separate
     * them.
     *
     * @param limit how many rows to return; callers clamp this
     * @return up to {@code limit} rows
     */
    public List<LedgerEntryRow> entriesBefore(long accountId, Instant beforeCreatedAt, Long beforeId, int limit) {
        // The counterparty is the other entry of the same transaction. LEFT JOIN
        // so an entry whose transaction has no second side still lists.
        String sql = """
                SELECT e.id,
                       e.transaction_id,
                       e.amount,
                       e.created_at,
                       other.account_id AS counterparty_account_id
                FROM ledger_entries e
                LEFT JOIN ledger_entries other
                       ON other.transaction_id = e.transaction_id
                      AND other.id <> e.id
                WHERE e.account_id = :accountId
                """;

        MapSqlParameterSource params = new MapSqlParameterSource()
                .addValue("accountId", accountId)
                .addValue("limit", limit);

        if (beforeCreatedAt != null) {
            // Row-value comparison: strictly before the cursor position in the
            // same (created_at DESC, id DESC) ordering the query returns.
            sql += "  AND (e.created_at, e.id) < (:beforeCreatedAt, :beforeId)\n";
            params.addValue("beforeCreatedAt", java.sql.Timestamp.from(beforeCreatedAt));
            params.addValue("beforeId", beforeId);
        }

        sql += "ORDER BY e.created_at DESC, e.id DESC\nLIMIT :limit";

        return jdbc.query(sql, params, ENTRY_ROW_MAPPER);
    }

    private static final RowMapper<LedgerEntryRow> ENTRY_ROW_MAPPER = (ResultSet rs, int rowNum) -> {
        long counterparty = rs.getLong("counterparty_account_id");
        return new LedgerEntryRow(
                rs.getLong("id"),
                rs.getLong("transaction_id"),
                rs.getBigDecimal("amount"),
                rs.getTimestamp("created_at").toInstant(),
                // getLong yields 0 for SQL NULL, so the null has to be asked for.
                rs.wasNull() ? null : counterparty);
    };

    /** A derived balance and the number of entries behind it. */
    public record AccountBalance(BigDecimal balance, long entryCount) {
    }

    /** One entry, as stored, before it is shaped for HTTP. */
    public record LedgerEntryRow(
            long id,
            long transactionId,
            BigDecimal amount,
            Instant createdAt,
            Long counterpartyAccountId) {
    }
}
