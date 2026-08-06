package com.ledgerline.ledger;

import java.util.Optional;

import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.stereotype.Component;

import com.ledgerline.domain.TransactionState;

/**
 * Reads and advances transaction lifecycle state.
 *
 * Every write here is a compare-and-swap. Reading the state and then writing
 * the new one as two statements would let two consumers observe the same prior
 * state and both proceed -- the read-then-write race this codebase has avoided
 * since the idempotency claim was written as ON CONFLICT rather than
 * SELECT-then-INSERT. Kafka's partition-by-key makes concurrent processing of
 * one transaction rare, but not impossible: during a rebalance two consumers
 * can briefly hold the same partition, which is exactly when a record is
 * redelivered.
 */
@Component
public class TransactionStateRepository {

    private final NamedParameterJdbcTemplate jdbc;

    TransactionStateRepository(NamedParameterJdbcTemplate jdbc) {
        this.jdbc = jdbc;
    }

    /** Current state, or empty when the transaction has no events yet. */
    public Optional<TransactionState> find(String externalTxnId) {
        return jdbc.query(
                "SELECT state FROM transaction_states WHERE external_txn_id = :id",
                new MapSqlParameterSource("id", externalTxnId),
                rs -> rs.next()
                        ? Optional.of(TransactionState.valueOf(rs.getString("state")))
                        : Optional.<TransactionState>empty());
    }

    /**
     * Creates the state row for a transaction seen for the first time.
     *
     * ON CONFLICT DO NOTHING so two consumers racing the first event of one
     * transaction cannot both insert. The loser gets false and re-reads,
     * finding whatever the winner wrote.
     *
     * @return true if this call created the row
     */
    public boolean createIfAbsent(String externalTxnId, TransactionState initial) {
        int inserted = jdbc.update(
                "INSERT INTO transaction_states (external_txn_id, state) "
                        + "VALUES (:id, :state) "
                        + "ON CONFLICT (external_txn_id) DO NOTHING",
                new MapSqlParameterSource()
                        .addValue("id", externalTxnId)
                        .addValue("state", initial.name()));
        return inserted == 1;
    }

    /**
     * Advances the transaction, but only if it is still where the caller
     * believes it is.
     *
     * The WHERE clause carries the expected state, so the check and the write
     * are one statement and cannot be interleaved. A caller that read
     * AUTHORIZED and was beaten to CAPTURED by another consumer matches no row
     * here and is told so, rather than silently overwriting the winner's work.
     *
     * @return true when this call performed the transition
     */
    public boolean compareAndAdvance(
            String externalTxnId, TransactionState expected, TransactionState next) {

        int updated = jdbc.update(
                "UPDATE transaction_states "
                        + "SET state = :next, version = version + 1, updated_at = now() "
                        + "WHERE external_txn_id = :id AND state = :expected",
                new MapSqlParameterSource()
                        .addValue("id", externalTxnId)
                        .addValue("expected", expected.name())
                        .addValue("next", next.name()));

        return updated == 1;
    }

    /**
     * Removes a state row that no event has actually advanced.
     *
     * Used when an event created the row and was then parked rather than
     * applied. The guard on version = 0 is what makes this safe: a row any
     * event has moved has a non-zero version and is left alone, so this can
     * never delete the state of a live transaction.
     */
    public void deleteIfUntouched(String externalTxnId) {
        jdbc.update(
                "DELETE FROM transaction_states "
                        + "WHERE external_txn_id = :id AND version = 0",
                new MapSqlParameterSource("id", externalTxnId));
    }

    /** How many transitions have been applied. Exposed for diagnostics. */
    public Optional<Long> versionOf(String externalTxnId) {
        return jdbc.query(
                "SELECT version FROM transaction_states WHERE external_txn_id = :id",
                new MapSqlParameterSource("id", externalTxnId),
                rs -> rs.next() ? Optional.of(rs.getLong("version")) : Optional.<Long>empty());
    }
}
