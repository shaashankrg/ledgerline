-- Settlement file simulator: the network's independent view of payments.
--
-- The answer key now holds faults from both sides of the comparison -- the
-- generator's own faults (source = PROCESSOR, the default for every row
-- written before this migration) and the settlement simulator's faults
-- (source = NETWORK), so a later reconciliation engine's findings can be
-- graded against the full picture, not just one side of it.
--
-- The default on `source` is kept, deliberately not dropped: FaultLedger
-- (Week 1-2 code, off limits -- see the Day 1 task spec's "do not modify"
-- list) inserts into this table without naming every column, and its own
-- test suite is equally off limits to edit. Dropping the default, as an
-- earlier draft of this migration did, breaks every existing generator
-- insert with a NOT NULL violation. Keeping the default is what lets an old
-- writer go on meaning "processor" implicitly while a new writer names its
-- source explicitly.
ALTER TABLE faultlab.injected_faults
    ADD COLUMN source TEXT NOT NULL DEFAULT 'PROCESSOR';
ALTER TABLE faultlab.injected_faults
    ADD CONSTRAINT injected_faults_source_chk
    CHECK (source IN ('PROCESSOR', 'NETWORK'));

-- One row per simulator run. Mirrors faultlab.generator_runs: the seed and
-- base instant are what make a batch reproducible byte-for-byte, and the file
-- hash gives byte-equality checking without re-reading the file.
CREATE TABLE recon_batches (
    batch_id      TEXT PRIMARY KEY,
    run_id        TEXT NOT NULL,
    seed          BIGINT NOT NULL,
    base_instant  TIMESTAMPTZ NOT NULL,
    row_count     INT NOT NULL,
    file_sha256   TEXT NOT NULL,
    generated_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- The network's settlement file, loaded row by row.
--
-- external_txn_id is nullable and not unique. Nullable because
-- NETWORK_UNKNOWN_TXN produces a settlement row for a payment that does not
-- exist on our side. Not unique because NETWORK_DUPLICATE_ROW produces the
-- same transaction settled twice in one batch. Uniqueness is on
-- (batch_id, line_number) instead -- the physical position in the file, which
-- genuinely is unique.
--
-- Deliberately no foreign key to transactions: the network's view is allowed
-- to reference things we don't have, and enforcing referential integrity here
-- would make NETWORK_UNKNOWN_TXN unrepresentable -- exactly the fault it
-- exists to model.
CREATE TABLE settlement_records (
    id                  BIGSERIAL PRIMARY KEY,
    batch_id            TEXT NOT NULL REFERENCES recon_batches(batch_id),
    line_number         INT NOT NULL,
    external_txn_id     TEXT,
    merchant_id         TEXT NOT NULL,
    gross_amount_minor  BIGINT NOT NULL,
    fee_minor           BIGINT NOT NULL DEFAULT 0,
    currency            CHAR(3) NOT NULL,
    settled_at          TIMESTAMPTZ NOT NULL,
    -- The original text of the line, verbatim, before any parsing. If the
    -- parser is wrong, the parsed columns are plausibly wrong with no way to
    -- detect it; this is the audit trail underneath them.
    raw_line            TEXT NOT NULL,
    loaded_at           TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (batch_id, line_number)
);

CREATE INDEX ON settlement_records (batch_id);
CREATE INDEX ON settlement_records (external_txn_id);
-- Supports a later fuzzy match on (amount, merchant, time window).
CREATE INDEX ON settlement_records (merchant_id, gross_amount_minor, settled_at);

-- A role for a future reconciliation engine, so the permission boundary
-- V6 established for faultlab can be asserted for the settlement side too.
-- Mirrors V6's reasoning exactly: a query written against settlement_records
-- or the ledger sees `public` on its search_path and cannot resolve
-- `faultlab.injected_faults` without naming the schema explicitly, and this
-- role is denied that schema outright so the boundary is enforced by Postgres,
-- not by application discipline.
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'recon_role') THEN
        CREATE ROLE recon_role LOGIN PASSWORD 'recon_role';
    END IF;
END
$$;

GRANT USAGE ON SCHEMA public TO recon_role;
GRANT SELECT ON settlement_records, recon_batches TO recon_role;
GRANT SELECT ON accounts, transactions, ledger_entries, transaction_states TO recon_role;
REVOKE ALL ON SCHEMA faultlab FROM recon_role;
REVOKE ALL ON ALL TABLES IN SCHEMA faultlab FROM recon_role;
