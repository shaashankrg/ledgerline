-- Purely additive: gives the ledger side of the fuzzy-match triple
-- (amount, merchant, time window) something to actually observe.
--
-- Before this migration, the settlement simulator derived a merchant
-- identity from (seed, externalTxnId) inside SettlementSimulator only -- an
-- attribute of the settlement row, invisible to the ledger. A reconciliation
-- engine reads the ledger and must never know the seed, so it could never
-- have derived the same value; the merchant field existed on one side of
-- the comparison only, which made it worthless for matching. This column is
-- what lets the same merchant identity be written once, by the payment's own
-- producer, and read independently by both the settlement file and the
-- ledger.
--
-- Nullable and untouched by every write path that doesn't know a merchant:
-- EmitTransactionCommand, every pre-existing test that constructs a
-- TransactionMessage or TransactionEvent positionally, and any producer that
-- predates this column all continue to insert NULL here, which is exactly
-- what "merchant unknown" should mean.
ALTER TABLE transactions
    ADD COLUMN merchant_id TEXT;
