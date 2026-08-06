-- Ground truth for fault injection: the answer key.
--
-- Every fault the generator deliberately injects is recorded here, so a
-- reconciliation engine's findings can later be graded against what was
-- actually done to the stream.
--
-- THE RECONCILIATION ENGINE MUST NOT READ THIS TABLE.
--
-- That is not a comment-level wish. The table lives in its own schema,
-- `faultlab`, rather than in `public` alongside the ledger. Two things follow:
--
--   1. Nothing reaches it by accident. A query written against the ledger sees
--      `public` on its search_path and simply cannot resolve `injected_faults`
--      without naming `faultlab.` explicitly -- which is a deliberate act, not
--      a slip.
--
--   2. It is revocable. A reconciliation role can be granted USAGE on `public`
--      and denied it on `faultlab`, making the boundary a permission the
--      database enforces rather than a rule people remember. The grant below
--      sets that up for a role that does not exist yet; creating it is a
--      deployment concern, and the schema separation is what makes it possible
--      at all.
--
-- Grading an engine against an answer key it can read proves nothing, so the
-- separation is the whole point of storing this apart from the ledger.

CREATE SCHEMA IF NOT EXISTS faultlab;

CREATE TABLE faultlab.injected_faults (
    id              BIGSERIAL PRIMARY KEY,

    -- Which run produced this fault. Together with the seed recorded on the
    -- run, this is what makes an injection reproducible: same seed, same run
    -- parameters, same faults in the same order.
    run_id          VARCHAR(100) NOT NULL,

    -- DUPLICATE_PUBLISH, OUT_OF_ORDER, ORPHAN_CAPTURE, AMOUNT_DRIFT,
    -- MISSING_SETTLEMENT. Text rather than an enum type for the same reason
    -- transaction_states.state is text: the authority on the list is the Java
    -- FaultType, and a second copy in the database would drift from it.
    fault_type      VARCHAR(40)  NOT NULL,

    -- The transaction the fault was applied to. Not unique: one transaction
    -- can carry more than one fault, and a duplicate publish is naturally
    -- several records against one id.
    external_txn_id VARCHAR(100) NOT NULL,

    -- The event within that transaction, where the fault is specific to one.
    -- Null for faults that are properties of the whole transaction, such as a
    -- settlement that never arrives.
    event_id        VARCHAR(200),

    -- What the fault did, in enough detail to grade a finding against it.
    -- For AMOUNT_DRIFT: the captured amount and the settled amount, so a
    -- detector claiming "drift of X" can be checked for the right magnitude
    -- rather than merely the right transaction.
    expected_amount NUMERIC(19, 4),
    actual_amount   NUMERIC(19, 4),

    -- For DUPLICATE_PUBLISH: how many copies were sent, so a detector is
    -- graded on finding the right number of duplicates rather than just one.
    occurrences     INT,

    -- Free-form context for whatever a fault type needs that the columns above
    -- do not cover. Deliberately not the primary record of anything -- a
    -- detector graded against a JSON blob is graded against a moving target.
    detail          TEXT,

    injected_at     TIMESTAMPTZ  NOT NULL DEFAULT now()
);

-- Grading walks a whole run, and diagnosis looks up one transaction.
CREATE INDEX idx_injected_faults_run ON faultlab.injected_faults (run_id);
CREATE INDEX idx_injected_faults_txn ON faultlab.injected_faults (external_txn_id);

-- The run itself, so a stream can be reproduced exactly.
CREATE TABLE faultlab.generator_runs (
    run_id            VARCHAR(100) PRIMARY KEY,

    -- The RNG seed. Everything the generator decides -- which transactions get
    -- faults, which accounts, what amounts -- derives from this, so the same
    -- seed and parameters replay the same stream. Week 3's accuracy numbers
    -- are meaningless without it: an engine scoring 90% on an unreproducible
    -- stream cannot be compared against anything.
    seed              BIGINT       NOT NULL,

    transaction_count INT          NOT NULL,
    rate_per_second   INT          NOT NULL,

    -- The fault rates this run was configured with, as JSON. Stored so a run
    -- can be replayed without also having to find the config that produced it.
    fault_rates       TEXT         NOT NULL,

    started_at        TIMESTAMPTZ  NOT NULL DEFAULT now(),
    finished_at       TIMESTAMPTZ
);
