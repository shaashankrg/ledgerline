-- Idempotency support for the transfer service.
--
-- payload_hash records what a given idempotency_key was originally used for.
-- A retry carrying the same key must also carry the same payload; if the hash
-- differs, the caller has reused a key for a different transfer and the
-- service rejects it rather than silently returning the wrong transaction.

ALTER TABLE transactions
    ADD COLUMN payload_hash CHAR(64);

-- Nullable rather than NOT NULL: rows written before this migration (and by
-- LedgerWriter, which stays a dumb primitive and knows nothing about hashing)
-- have no payload to hash. A NOT NULL column would force a backfill value that
-- could collide with a real hash and make an unrelated request look like a
-- replay.

-- accounts.currency exists so the service can reject a transfer whose declared
-- currency does not match the accounts involved. Mixing currencies inside one
-- balanced entry pair would make the amounts sum to zero numerically while
-- being meaningless as money.
ALTER TABLE accounts
    ADD COLUMN currency CHAR(3) NOT NULL DEFAULT 'USD';

-- The seed accounts predate this column and are all USD, so the default
-- backfills them correctly. New accounts must state their currency explicitly.
ALTER TABLE accounts
    ALTER COLUMN currency DROP DEFAULT;

-- transactions.idempotency_key already carries a UNIQUE constraint from V2
-- (transactions_idempotency_key_key). That index is what makes concurrent
-- duplicate submissions safe, so it is asserted here rather than assumed:
-- this fails loudly if the constraint were ever dropped.
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1
        FROM pg_constraint c
        JOIN pg_class t ON t.oid = c.conrelid
        WHERE t.relname = 'transactions'
          AND c.contype = 'u'
          AND (SELECT array_agg(a.attname::text ORDER BY a.attname)
               FROM pg_attribute a
               WHERE a.attrelid = t.oid AND a.attnum = ANY (c.conkey)
              ) = ARRAY['idempotency_key']::text[]
    ) THEN
        ALTER TABLE transactions
            ADD CONSTRAINT transactions_idempotency_key_key UNIQUE (idempotency_key);
    END IF;
END $$;
