-- Events that arrived before they could legally be applied.
--
-- A capture whose authorize has not been processed yet is not poison. The
-- payload is well formed, the accounts are real, the amount is fine -- the
-- only thing wrong with it is that it is early. Sending it to the dead letter
-- topic would discard a perfectly good transfer because of message ordering,
-- which under a partitioned topic with retries is not an anomaly but an
-- expected occurrence.
--
-- So ordering rejections are parked here and replayed when their authorize
-- lands. Malformed payloads and permanent business rejections still go to the
-- DLT: those will never become applicable, however long they wait.

CREATE TABLE parked_events (
    id              BIGSERIAL PRIMARY KEY,

    -- The transaction whose authorize this event is waiting for. Draining is
    -- always "everything parked for this transaction", so this is the lookup.
    external_txn_id VARCHAR(100) NOT NULL,

    -- The event's own identity, carried through unchanged. Parking must not
    -- disturb idempotency: when this is eventually applied, it claims the same
    -- key it would have claimed on first arrival, so a redelivery that was
    -- already applied from the parked table is still recognized as a replay.
    event_id        VARCHAR(200) NOT NULL,

    event_type      VARCHAR(20)  NOT NULL,
    from_account_id BIGINT,
    to_account_id   BIGINT,
    amount          NUMERIC(19, 4),
    currency        CHAR(3)      NOT NULL,

    -- When the event happened at its source, not when it was parked. Draining
    -- orders by this: events are replayed in the order they occurred, which is
    -- the order the state machine expects, rather than the order they happened
    -- to arrive in.
    occurred_at     TIMESTAMPTZ  NOT NULL,

    parked_at       TIMESTAMPTZ  NOT NULL DEFAULT now(),

    -- Set when a drainer reserves this row, before it knows the outcome.
    -- Separate from applied_at so a replay that turns out to fail can still be
    -- recorded as abandoned: a single flag meaning both "reserved" and
    -- "applied" would leave no state in which to write the failure.
    drain_claimed_at TIMESTAMPTZ,

    -- Set when the event is successfully applied during a drain. Rows are kept
    -- rather than deleted so a transaction's history stays inspectable -- why
    -- something was late is a question worth being able to answer.
    applied_at      TIMESTAMPTZ,

    -- Set when a drain found the event still illegal. Such an event is not
    -- retried again: see the drain logic for why looping is worse than
    -- stopping.
    abandoned_at    TIMESTAMPTZ,
    abandon_reason  TEXT,

    -- One parked row per event. A redelivery of an event that is already
    -- parked must not create a second copy, or draining would apply it twice
    -- -- and while the idempotency claim would catch that, relying on a
    -- downstream guard to paper over a duplicate here would be sloppy.
    CONSTRAINT parked_events_event_id_key UNIQUE (event_id)
);

-- The drain query: everything still waiting for this transaction, oldest
-- first. Partial index because applied and abandoned rows are history and are
-- never drained again.
CREATE INDEX idx_parked_events_pending
    ON parked_events (external_txn_id, occurred_at, id)
    WHERE drain_claimed_at IS NULL AND applied_at IS NULL AND abandoned_at IS NULL;
