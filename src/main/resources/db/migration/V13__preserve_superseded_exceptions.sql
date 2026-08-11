-- Pass 2 previously DELETEd the MISSING_IN_LEDGER exception it superseded
-- when a line was recovered fuzzily. Every other write in this project is
-- append-only and evidence-preserving -- raw_line kept verbatim even when
-- the parsed columns are trusted more, ledger entries never updated, only
-- reversed by a new entry. The delete broke that pattern for no reason that
-- survives scrutiny: "pass 1 could not identify this row, pass 2 recovered
-- it" is itself a fact worth keeping, and it currently survives only
-- implicitly, reconstructable (if at all) from recon_line_outcomes rather
-- than stated directly.
--
-- Replaces the delete with a nullable superseded_at. A superseded exception
-- is not currently active -- every "what's still open" query must filter on
-- it being null, the same discipline transaction_states' compare-and-swap
-- already requires of its callers -- but it is never destroyed.
ALTER TABLE recon_exceptions
    ADD COLUMN superseded_at TIMESTAMPTZ;

-- UNIQUE (recon_run_id, subject_key, type) is replaced by a partial unique
-- index over the *active* rows only. Without this, a superseded row would
-- permanently occupy its (run, subject_key, type) slot and block a genuine
-- future exception of the same shape for the same subject from ever being
-- inserted -- which cannot happen today (a line pass 2 has resolved is never
-- reclassified within the same run), but would silently reintroduce the
-- exact defect this migration fixes the moment that stopped being true.
-- Scoping the uniqueness to "active" rows is what keeps supersession and
-- idempotency from fighting each other.
ALTER TABLE recon_exceptions
    DROP CONSTRAINT recon_exceptions_recon_run_id_subject_key_type_key;

CREATE UNIQUE INDEX recon_exceptions_active_unique
    ON recon_exceptions (recon_run_id, subject_key, type)
    WHERE superseded_at IS NULL;

GRANT UPDATE (superseded_at) ON recon_exceptions TO recon_role;

REVOKE ALL ON SCHEMA faultlab FROM recon_role;
REVOKE ALL ON ALL TABLES IN SCHEMA faultlab FROM recon_role;
