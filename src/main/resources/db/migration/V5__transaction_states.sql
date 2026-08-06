-- Lifecycle state per transaction.
--
-- A transaction is now a sequence of events (authorize, capture, settle,
-- refund, ...) sharing one external_txn_id, rather than a single complete
-- transfer. This table holds where each one currently sits, so the state
-- machine has something to gate incoming events against.
--
-- Separate from `transactions`, which keeps its existing job: that table is the
-- idempotency ledger for individual messages, one row per accepted event. This
-- one is the current state of a whole transaction, one row per payment. Folding
-- them together would mean a single row that is both "this message was seen"
-- and "this payment is captured", and those change at different rates.

CREATE TABLE transaction_states (
    -- The transaction's identity across its whole lifecycle. Primary key rather
    -- than a surrogate id: there is exactly one state row per transaction, and
    -- every lookup is by this value.
    external_txn_id VARCHAR(100) PRIMARY KEY,

    -- Stored as text rather than an enum type. A Postgres enum would need a
    -- migration to add a state, and the authority on the state set is the Java
    -- TransactionState -- two sources of truth for the same list would drift.
    state           VARCHAR(20)  NOT NULL,

    -- Advanced on every accepted transition. Not used for locking (the
    -- conditional update compares on state itself), but it makes the number of
    -- transitions applied visible when diagnosing a transaction's history.
    version         BIGINT       NOT NULL DEFAULT 0,

    created_at      TIMESTAMPTZ  NOT NULL DEFAULT now(),
    updated_at      TIMESTAMPTZ  NOT NULL DEFAULT now()
);

-- Transitions are applied as a conditional update:
--
--   UPDATE transaction_states SET state = :new
--    WHERE external_txn_id = :id AND state = :expected
--
-- Zero rows affected means another consumer moved the transaction first. That
-- is a compare-and-swap: the check and the write are one statement, so two
-- events for the same transaction cannot both observe the same prior state and
-- both proceed. A SELECT-then-UPDATE would let exactly that happen, and the
-- window is real -- during a rebalance two consumers can briefly hold the same
-- partition, which is precisely when duplicate delivery occurs.

-- Nothing writes ledger entries without also advancing state in the same
-- database transaction, so this index supports the only access pattern there
-- is: find a transaction's state by its external id. The primary key already
-- provides it; no further index is needed and none is added.
