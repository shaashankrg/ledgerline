-- Core double-entry schema.
--
-- Shape of the design: a transaction row says "this event happened" and carries
-- no amount. All money movement lives in ledger_entries, so there is no path
-- that records a total while skipping the debit/credit pair.

CREATE TABLE accounts (
    id           BIGSERIAL PRIMARY KEY,
    name         VARCHAR(100) NOT NULL,
    -- asset / liability / equity / ... Which direction a debit moves an account
    -- depends on this, so the column exists now even before that nuance is used.
    account_type VARCHAR(20)  NOT NULL,
    created_at   TIMESTAMPTZ  NOT NULL DEFAULT now()
);

CREATE TABLE transactions (
    id              BIGSERIAL PRIMARY KEY,
    -- Client-supplied key identifying one attempt to create this transaction.
    -- If a caller retries after a timeout, the UNIQUE constraint makes Postgres
    -- reject the duplicate rather than relying on application code to check.
    idempotency_key VARCHAR(100) NOT NULL UNIQUE,
    description     VARCHAR(255),
    created_at      TIMESTAMPTZ  NOT NULL DEFAULT now()
);

CREATE TABLE ledger_entries (
    id             BIGSERIAL PRIMARY KEY,
    transaction_id BIGINT         NOT NULL REFERENCES transactions (id),
    account_id     BIGINT         NOT NULL REFERENCES accounts (id),
    -- NUMERIC, never FLOAT/DOUBLE: exact decimal arithmetic is required for the
    -- "entries sum to zero" invariant to hold. 19 total digits, 4 after the point.
    amount         NUMERIC(19, 4) NOT NULL,
    created_at     TIMESTAMPTZ    NOT NULL DEFAULT now(),
    CONSTRAINT ledger_entries_amount_nonzero CHECK (amount <> 0)
);

-- Reconciliation groups by transaction_id; balance/history lookups filter by
-- account_id. Both would be sequential scans without these.
CREATE INDEX idx_ledger_entries_transaction ON ledger_entries (transaction_id);
CREATE INDEX idx_ledger_entries_account ON ledger_entries (account_id);
