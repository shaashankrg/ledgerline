-- Makes a settlement batch self-describing about its own generator run's
-- transmission health, without needing the logs from the machine that
-- produced it.
--
-- Fail-soft producer behavior (see TransactionGenerator.generate) means a
-- publish failure never aborts a run -- correct, because the settlement file
-- must still include a payment the world saw even if our own pipeline failed
-- to transmit it. The cost of fail-soft is a measurement hazard: a flaky
-- broker during a later accuracy sweep produces a settlement row with no
-- ledger entry and no ground-truth label (nobody injected the loss on
-- purpose), which the engine correctly flags as MISSING_IN_LEDGER and the
-- harness would then score as a false positive -- with nothing in the
-- numbers to say the cause was infrastructure trouble rather than a real
-- detection miss. Recording the count here is what lets a later reader tell
-- the difference.
ALTER TABLE recon_batches
    ADD COLUMN publish_failures INT NOT NULL DEFAULT 0;
