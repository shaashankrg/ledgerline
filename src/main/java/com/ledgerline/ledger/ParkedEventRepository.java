package com.ledgerline.ledger;

import java.sql.ResultSet;
import java.time.Instant;
import java.util.List;

import org.springframework.jdbc.core.RowMapper;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.stereotype.Component;

import com.ledgerline.domain.EventType;
import com.ledgerline.domain.TransactionEvent;

/**
 * Holds events that arrived before they could legally be applied.
 *
 * A parked event is not a failure, it is a wait. The distinction from the dead
 * letter topic is the whole reason this exists: the DLT holds things that will
 * never work, this holds things that do not work yet.
 */
@Component
public class ParkedEventRepository {

    private final NamedParameterJdbcTemplate jdbc;

    ParkedEventRepository(NamedParameterJdbcTemplate jdbc) {
        this.jdbc = jdbc;
    }

    /**
     * Parks an event for later.
     *
     * ON CONFLICT DO NOTHING on the event id: a redelivery of an already-parked
     * event must not create a second row, or draining would apply it twice.
     * The idempotency claim downstream would catch that, but relying on a later
     * guard to clean up a duplicate introduced here would be careless.
     *
     * @return true if this call parked it, false if it was already parked
     */
    public boolean park(TransactionEvent event) {
        int inserted = jdbc.update(
                "INSERT INTO parked_events "
                        + "(external_txn_id, event_id, event_type, from_account_id, "
                        + " to_account_id, amount, currency, occurred_at) "
                        + "VALUES (:txnId, :eventId, :type, :from, :to, :amount, :currency, :occurredAt) "
                        + "ON CONFLICT (event_id) DO NOTHING",
                new MapSqlParameterSource()
                        .addValue("txnId", event.externalTxnId())
                        .addValue("eventId", event.eventId())
                        .addValue("type", event.type().name())
                        .addValue("from", event.fromAccountId())
                        .addValue("to", event.toAccountId())
                        .addValue("amount", event.amount())
                        .addValue("currency", event.currency())
                        .addValue("occurredAt", java.sql.Timestamp.from(event.occurredAt())));

        return inserted == 1;
    }

    /**
     * Everything still waiting for this transaction, oldest first.
     *
     * Ordered by occurred_at then id: events replay in the order they happened
     * at their source, not the order they arrived here. Arrival order is
     * exactly the thing that was wrong with them.
     */
    public List<ParkedEvent> pendingFor(String externalTxnId) {
        return jdbc.query(
                "SELECT id, external_txn_id, event_id, event_type, from_account_id, "
                        + "       to_account_id, amount, currency, occurred_at "
                        + "FROM parked_events "
                        + "WHERE external_txn_id = :txnId "
                        + "  AND drain_claimed_at IS NULL "
                        + "  AND applied_at IS NULL AND abandoned_at IS NULL "
                        + "ORDER BY occurred_at, id",
                new MapSqlParameterSource("txnId", externalTxnId),
                PARKED_ROW_MAPPER);
    }

    /**
     * Reserves a parked row for one drainer, without yet saying what became of
     * it.
     *
     * Claiming and recording the outcome are separate steps on purpose. A
     * single update that both reserved the row and marked it applied would have
     * to be undone when the replay turned out to fail, and an abandonment
     * written afterwards would find the row no longer pending and silently do
     * nothing -- which is exactly the bug this shape avoids.
     *
     * The WHERE clause is a compare-and-swap: two concurrent drains cannot both
     * reserve the same row, so the second skips it rather than replaying it a
     * second time.
     *
     * @return true if this call reserved the row
     */
    public boolean claimForDrain(long parkedId) {
        int updated = jdbc.update(
                "UPDATE parked_events SET drain_claimed_at = now() "
                        + "WHERE id = :id "
                        + "  AND drain_claimed_at IS NULL "
                        + "  AND applied_at IS NULL AND abandoned_at IS NULL",
                new MapSqlParameterSource("id", parkedId));
        return updated == 1;
    }

    /** Records that a claimed row was successfully replayed. */
    public void markApplied(long parkedId) {
        jdbc.update(
                "UPDATE parked_events SET applied_at = now() WHERE id = :id",
                new MapSqlParameterSource("id", parkedId));
    }

    /**
     * Records that a claimed row will never apply, with the reason.
     *
     * The row is kept rather than deleted: why an event could not be replayed
     * is worth being able to answer later, and a deleted row answers nothing.
     */
    public void markAbandoned(long parkedId, String reason) {
        jdbc.update(
                "UPDATE parked_events SET abandoned_at = now(), abandon_reason = :reason "
                        + "WHERE id = :id",
                new MapSqlParameterSource()
                        .addValue("id", parkedId)
                        .addValue("reason", reason));
    }

    public long pendingCount() {
        return jdbc.queryForObject(
                "SELECT count(*) FROM parked_events "
                        + "WHERE applied_at IS NULL AND abandoned_at IS NULL",
                new MapSqlParameterSource(), Long.class);
    }

    private static final RowMapper<ParkedEvent> PARKED_ROW_MAPPER = (ResultSet rs, int rowNum) -> {
        long from = rs.getLong("from_account_id");
        Long fromAccount = rs.wasNull() ? null : from;
        long to = rs.getLong("to_account_id");
        Long toAccount = rs.wasNull() ? null : to;

        return new ParkedEvent(
                rs.getLong("id"),
                new TransactionEvent(
                        rs.getString("external_txn_id"),
                        rs.getString("event_id"),
                        EventType.valueOf(rs.getString("event_type")),
                        fromAccount,
                        toAccount,
                        rs.getBigDecimal("amount"),
                        rs.getString("currency").trim(),
                        rs.getTimestamp("occurred_at").toInstant()));
    };

    /** A parked row and the event it holds. */
    public record ParkedEvent(long parkedId, TransactionEvent event) {

        public Instant occurredAt() {
            return event.occurredAt();
        }
    }
}
