-- Matcher pass 1: exact matching and exception classification.
--
-- Two tables. Both are written by the reconciliation engine, which runs
-- connected as recon_role (V8) specifically so the "never reads the answer
-- key" rule in the task spec is enforced by Postgres, not by application
-- discipline -- the same reasoning V6 and V8 already applied to reads is
-- applied here to writes: recon_role gets exactly the grants this engine
-- needs and nothing on faultlab, so a future change to this service cannot
-- accidentally regain that access without a migration author noticing.

-- One row per finding. subject_key, not (batch_id, external_txn_id, type), is
-- the dedup key: external_txn_id is nullable on settlement_records (V8), and
-- two SQL NULLs never conflict under a UNIQUE constraint, so keying on it
-- directly would let a rerun insert a fresh duplicate exception for every
-- line whose id didn't parse -- silently breaking idempotency on exactly the
-- rows the fault injector produces. subject_key is computed in
-- ReconciliationService, not as a generated column, as:
--   - settlement-sourced exception, external_txn_id present -> external_txn_id
--   - settlement-sourced exception, external_txn_id null    -> 'line:' || line_number
--   - MISSING_IN_SETTLEMENT (ledger-sourced)                -> the payment's external_txn_id
-- ON DELETE CASCADE on both FKs below: a batch's exceptions and line outcomes
-- are meaningless without the batch, and every existing settlement test
-- (SettlementSimulatorTest, SettlementPublishFailureTest) cleans up between
-- tests with an unqualified DELETE FROM recon_batches. Those tests predate
-- this migration and are off limits to edit; RESTRICT (the default) would
-- make their cleanup fail the moment any recon_exceptions row exists for a
-- batch still in the table, which is exactly the state this suite leaves
-- behind. Cascading is also the semantically correct behavior on its own
-- merits, independent of that test-ordering concern.
CREATE TABLE recon_exceptions (
    id                       BIGSERIAL PRIMARY KEY,
    batch_id                 TEXT NOT NULL REFERENCES recon_batches(batch_id) ON DELETE CASCADE,
    subject_key              TEXT NOT NULL,
    type                     TEXT NOT NULL,
    external_txn_id          TEXT,
    settlement_line_numbers  INT[] NOT NULL DEFAULT '{}',
    settlement_amount_minor  BIGINT,
    ledger_amount_minor      BIGINT,
    delta_minor              BIGINT,
    payment_state            TEXT,
    detected_at              TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT recon_exceptions_type_chk CHECK (type IN (
        'AMOUNT_MISMATCH', 'MISSING_IN_LEDGER', 'MISSING_IN_SETTLEMENT',
        'DUPLICATE_SETTLEMENT', 'STATE_CONFLICT')),
    UNIQUE (batch_id, subject_key, type)
);

CREATE INDEX ON recon_exceptions (batch_id);

-- One row per settlement line, recording exactly which bucket it landed in.
--
-- Exists because "exception count plus matched rows equals row_count" is not
-- a valid completeness check: DUPLICATE_SETTLEMENT is one exception covering
-- several lines, and MISSING_IN_SETTLEMENT corresponds to no settlement line
-- at all (hence its absence from the outcome CHECK below -- it is a property
-- of a ledger payment, not of a settlement line). Completeness has to be
-- proven per line, and the composite primary key is what makes "exactly one
-- bucket per line" a database guarantee rather than something the service
-- merely intends.
CREATE TABLE recon_line_outcomes (
    batch_id      TEXT NOT NULL,
    line_number   INT  NOT NULL,
    outcome       TEXT NOT NULL,
    exception_id  BIGINT REFERENCES recon_exceptions(id) ON DELETE CASCADE,
    PRIMARY KEY (batch_id, line_number),
    FOREIGN KEY (batch_id, line_number)
        REFERENCES settlement_records (batch_id, line_number) ON DELETE CASCADE,
    CONSTRAINT recon_line_outcomes_outcome_chk CHECK (outcome IN (
        'MATCHED', 'AMOUNT_MISMATCH', 'MISSING_IN_LEDGER',
        'DUPLICATE_SETTLEMENT', 'STATE_CONFLICT'))
);

-- recon_role (V8) is currently read-only. The reconciliation engine writes
-- its findings under this same role rather than the application's default
-- datasource role, so "never reads faultlab" is a permission the engine
-- structurally cannot violate, not a promise about which queries it happens
-- to issue. INSERT is all it needs -- ON CONFLICT DO NOTHING requires the
-- privilege on the target of the INSERT, not UPDATE, since a suppressed
-- conflict never touches the existing row.
GRANT SELECT, INSERT ON recon_exceptions, recon_line_outcomes TO recon_role;
-- BIGSERIAL's backing sequences are not covered by a table-level GRANT.
GRANT USAGE ON recon_exceptions_id_seq TO recon_role;

-- Re-assert the boundary. Migrations silently regrant on occasion (a
-- schema-wide GRANT added elsewhere would otherwise reopen this), and the
-- isolation test in this task suite runs on every build specifically to
-- catch that regression -- this line is what keeps the assertion true after
-- this migration runs, not just after V8's.
REVOKE ALL ON SCHEMA faultlab FROM recon_role;
REVOKE ALL ON ALL TABLES IN SCHEMA faultlab FROM recon_role;
