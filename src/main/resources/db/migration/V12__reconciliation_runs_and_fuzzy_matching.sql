-- Matcher pass 2: fuzzy matching, and the run identity that makes a batch
-- reconcilable more than once.
--
-- V11 keyed recon_exceptions and recon_line_outcomes on batch_id, with
-- UNIQUE (batch_id, subject_key, type) and PRIMARY KEY (batch_id, line_number).
-- That made a rerun of the same batch a clean no-op, which was the point --
-- but it also makes a *parameterised* rerun impossible: reconciling one batch
-- at two different fuzzy windows collides on those same constraints, because
-- nothing in the key distinguishes "the same work repeated" from "the same
-- batch evaluated under different settings". A window sweep needs both
-- results to coexist.
--
-- recon_runs introduces the missing entity. A run is one (batch, window,
-- matcher version) triple; exceptions and outcomes belong to a run, not to a
-- batch directly. Idempotency is preserved and sharpened: same parameters
-- resolve to the same recon_run_id via the unique constraint below, so the
-- existing ON CONFLICT DO NOTHING writes still make a rerun a no-op, while
-- different parameters get their own run and cannot overwrite each other.

CREATE TABLE recon_runs (
    recon_run_id     BIGSERIAL PRIMARY KEY,
    batch_id         TEXT NOT NULL REFERENCES recon_batches(batch_id) ON DELETE CASCADE,
    window_seconds   INT NOT NULL,
    -- Bumped by hand in ReconciliationService when matching logic changes, so
    -- a sweep against an old matcher cannot be silently compared against a
    -- new one. Not derived from a build number: what matters is whether the
    -- *matching decisions* changed, which only the author knows.
    matcher_version  TEXT NOT NULL,
    started_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (batch_id, window_seconds, matcher_version)
);

CREATE INDEX ON recon_runs (batch_id);

-- Existing rows are dropped rather than backfilled into a synthetic run.
--
-- Both tables hold only *derived* data: every row in them is recomputable by
-- calling ReconciliationService.run again over settlement_records and the
-- ledger, both of which are untouched here. A backfill would have to invent a
-- window_seconds for runs that predate the concept of a window, and that
-- invented value would then be indistinguishable from a real measured one
-- during Day 4's sweep -- a fabricated data point in an accuracy result is
-- worse than an absent one. Dropping is safe precisely because nothing here
-- is a source of truth; the settlement file and the ledger are.
DROP TABLE recon_line_outcomes;
DROP TABLE recon_exceptions;

CREATE TABLE recon_exceptions (
    id                       BIGSERIAL PRIMARY KEY,
    recon_run_id             BIGINT NOT NULL REFERENCES recon_runs(recon_run_id) ON DELETE CASCADE,
    subject_key              TEXT NOT NULL,
    type                     TEXT NOT NULL,
    external_txn_id          TEXT,
    settlement_line_numbers  INT[] NOT NULL DEFAULT '{}',
    settlement_amount_minor  BIGINT,
    ledger_amount_minor      BIGINT,
    delta_minor              BIGINT,
    payment_state            TEXT,
    detected_at              TIMESTAMPTZ NOT NULL DEFAULT now(),
    -- Unchanged from V11, and deliberately NOT extended for the two new
    -- outcomes. AMBIGUOUS is a line outcome, not an exception type: a line
    -- that ends ambiguous is still unmatched, so it raises the same
    -- MISSING_IN_LEDGER exception any other unidentified line raises. What
    -- separates "found nothing" from "found too much" is candidate_count on
    -- the outcome row, not a sixth exception type. FUZZY_MATCHED raises no
    -- exception at all, for the same reason MATCHED doesn't.
    CONSTRAINT recon_exceptions_type_chk CHECK (type IN (
        'AMOUNT_MISMATCH', 'MISSING_IN_LEDGER', 'MISSING_IN_SETTLEMENT',
        'DUPLICATE_SETTLEMENT', 'STATE_CONFLICT')),
    UNIQUE (recon_run_id, subject_key, type)
);

CREATE INDEX ON recon_exceptions (recon_run_id);

CREATE TABLE recon_line_outcomes (
    recon_run_id        BIGINT NOT NULL REFERENCES recon_runs(recon_run_id) ON DELETE CASCADE,
    batch_id            TEXT NOT NULL,
    line_number         INT  NOT NULL,
    outcome             TEXT NOT NULL,
    exception_id        BIGINT REFERENCES recon_exceptions(id) ON DELETE CASCADE,

    -- EXACT when pass 1 matched on external_txn_id, FUZZY when pass 2
    -- recovered it on (amount, merchant, time), NONE when nothing matched.
    match_method        TEXT NOT NULL DEFAULT 'NONE',
    -- The payment this line was matched to. Null whenever the line is
    -- unmatched -- including AMBIGUOUS, where candidates existed but the
    -- matcher refused to choose between them.
    matched_txn_id      TEXT,
    -- How many payments pass 2 considered plausible. Null for exact matches
    -- and for lines pass 2 never examined; 0/1/n for lines it did.
    candidate_count     INT,
    -- Signed, settled_at minus capture time: positive means the network
    -- settled after we captured, negative means it settled before. Direction
    -- points at different causes, exactly as delta_minor's sign does, so it
    -- is stored signed rather than as a magnitude.
    time_delta_seconds  BIGINT,

    PRIMARY KEY (recon_run_id, line_number),
    FOREIGN KEY (batch_id, line_number)
        REFERENCES settlement_records (batch_id, line_number) ON DELETE CASCADE,
    CONSTRAINT recon_line_outcomes_outcome_chk CHECK (outcome IN (
        'MATCHED', 'FUZZY_MATCHED', 'AMBIGUOUS', 'AMOUNT_MISMATCH',
        'MISSING_IN_LEDGER', 'DUPLICATE_SETTLEMENT', 'STATE_CONFLICT')),
    CONSTRAINT recon_line_outcomes_match_method_chk CHECK (match_method IN (
        'EXACT', 'FUZZY', 'NONE')),
    -- An unmatched line must not carry a matched payment, and a matched one
    -- must carry the payment it matched. Without this, a bug that recorded
    -- AMBIGUOUS while still stamping matched_txn_id would look like a refusal
    -- in the outcome column and a match in the id column at the same time.
    CONSTRAINT recon_line_outcomes_match_consistency_chk CHECK (
        (match_method = 'NONE' AND matched_txn_id IS NULL)
        OR (match_method <> 'NONE' AND matched_txn_id IS NOT NULL))
);

-- One payment is claimed by at most one settlement line within a run.
--
-- The other direction -- one line matching at most one payment -- is already
-- given by the primary key: a line has exactly one outcome row and therefore
-- at most one matched_txn_id. This index is the half the schema could not
-- otherwise express. Partial, because unmatched lines all carry NULL and
-- NULLs must not conflict with each other.
--
-- Enforced here rather than by trusting the matcher's candidate-exclusion
-- logic to be correct: this is the fifth instance in this project of letting
-- the database arbitrate rather than application code (after the idempotency
-- key insert, the state compare-and-swap, the parked-event claim, and V11's
-- exception dedup key). The sabotage pass for Day 3 drops this index
-- specifically to find out whether the application logic alone would have
-- held.
CREATE UNIQUE INDEX recon_line_outcomes_one_payment_per_run
    ON recon_line_outcomes (recon_run_id, matched_txn_id)
    WHERE matched_txn_id IS NOT NULL;

GRANT SELECT, INSERT ON recon_exceptions, recon_line_outcomes TO recon_role;
-- Pass 2 revises what pass 1 wrote rather than writing a second row: a line
-- has exactly one outcome (the primary key says so), so upgrading
-- MISSING_IN_LEDGER to FUZZY_MATCHED or AMBIGUOUS is necessarily an UPDATE.
-- Narrower than it looks -- the UPDATE is guarded on the row still being
-- MISSING_IN_LEDGER, so it can only move a line out of the unmatched state,
-- never rewrite a settled classification.
GRANT UPDATE ON recon_line_outcomes TO recon_role;
-- And a fuzzy match retires the MISSING_IN_LEDGER exception pass 1 raised for
-- it, which no longer describes anything once the line is identified.
GRANT DELETE ON recon_exceptions TO recon_role;
-- The matcher resolves-or-creates its own run row, so it needs to write here
-- too. SELECT alone would force run creation onto the application's datasource
-- role and split one logical operation across two connections.
GRANT SELECT, INSERT ON recon_runs TO recon_role;
GRANT USAGE ON recon_exceptions_id_seq, recon_runs_recon_run_id_seq TO recon_role;

-- Re-assert the boundary. Same reasoning as V11: migrations silently regrant,
-- and the isolation test runs on every build to catch exactly that.
REVOKE ALL ON SCHEMA faultlab FROM recon_role;
REVOKE ALL ON ALL TABLES IN SCHEMA faultlab FROM recon_role;
