# Known limitations

Deliberate, accepted gaps in the current implementation, recorded so they're
decisions on record rather than things a future reader has to rediscover by
tripping over them.

---

## `injected_faults.source` defaults to `PROCESSOR`

A writer that omits the `source` column is silently labelled `PROCESSOR`
rather than rejected -- in the one table whose entire job is to be a
trustworthy grader for a future reconciliation engine.

**Kept because:** `FaultLedger` (generator-side, off-limits to modify) inserts
into `faultlab.injected_faults` without naming every column, including
`source`. Dropping the default breaks every existing generator insert with a
`NOT NULL` violation.

**Mitigated by:** `SettlementSimulatorTest.everyFaultFromASettlementRunIsLabelledNetwork`
asserts every row a settlement run writes carries `source = 'NETWORK'`,
scoped by `run_id`. This doesn't remove the silent default, but it means a
settlement-side writer that forgets to name `source` fails a test immediately
rather than being discovered later as a mislabelled row in the answer key.

**Closing mechanism, if ever prioritized:** name `source` explicitly in
`FaultLedger`, then `ALTER TABLE faultlab.injected_faults ALTER COLUMN source
DROP DEFAULT`. Both changes touch off-limits Week 1-2 code, which is why this
wasn't done as part of the settlement simulator work.

---

## `merchant_id` is synthetic, not sourced from a real merchant entity

The ledger's own notion of "who got paid" is the credited account, and only
six of those are seeded in the whole schema -- not enough cardinality for the
later fuzzy-match triple `(amount, merchant, time window)` to actually
discriminate. `SyntheticMerchants` layers a deterministic, skewed-distribution
merchant identity on top, independent of the ledger account.

The identity is assigned once, in `TransactionGenerator.planTransaction`, keyed
on `(seed, index)` -- the payment's positional index within its run, not its
externalTxnId or any other run-specific string. It is then carried on
`TransactionMessage.merchantId()` into both `transactions.merchant_id` (via
the consumer's write path) and the settlement file (via
`SettlementSimulator` reading it back off the same published record it
already sources from), so it is a genuine value one side writes and the other
reads -- not a value derived independently on both sides that would agree
with itself by construction and be worthless for matching.

Keying on `index` rather than `externalTxnId` matters for a specific reason:
`externalTxnId` embeds the run id (`runId + "-txn-" + index`), so two runs
sharing a seed but not a run id -- exactly what
`TransactionGeneratorTest.sameSeedProducesAnIdenticalStream` checks -- would
otherwise get different merchants for "the payment at position N" and fail
that test. `index` is assigned in `planTransaction` before any conditional RNG
consumption, so it is a stable enumeration position, not a position in a draw
sequence -- it does not carry the same hazard as keying off a value that
shifts when config changes.

**Mitigated by:** `SettlementSimulatorTest.generatedBatchHasSufficientMerchantCardinality`
pins a floor (>= 10 distinct merchants in a 200-transaction batch); 
`FuzzyMatchFieldsTest.fuzzyMatchFieldsExistOnBothSides` proves the join
actually works end to end, through the database only, with no access to the
seed; `SettlementSimulatorTest.faultRowsAreNotIdentifiableByShapeAlone`
proves no merchant is exclusive to fault-affected rows.

**Mild residual tell:** `NETWORK_UNKNOWN_TXN` rows use a fabricated index far
outside the run's real transaction count (`1_000_000 + ...`), so their ids,
while correctly shaped, sit in a gap a careful reader could notice by
eyeballing the file (real ids are small, unknown-txn ids are not). Detecting
them still requires consulting the ledger to confirm no such payment exists,
which is legitimate detection, so this wasn't judged worth fixing -- interleaving
fabricated indices into gaps in the real sequence would remove even this
residual tell if it's ever prioritized.

---

## Settlement fee is a fixed 2% of gross

`SettlementSimulator.feeFor` computes fee as exactly `round(gross * 0.02)`,
floored at 1 cent, for every row including drifted ones (fee is recomputed
from the drifted gross, not left stale from before the drift). Every row in
the file satisfies `fee = 0.02 * gross` exactly.

**Consequence:** the fee column currently carries zero information
independent of the gross amount -- it's a pure function of a field already in
the row. A `FEE_MISMATCH` exception type is not just out of scope for Day 1,
it's impossible by construction: there's no way for a detector to find a fee
discrepancy when fee never varies independently of gross. A flat 2% is also a
simplification of real settlement economics, where interchange varies by card
type and merchant category.

**Accepted because:** the fix that closed the earlier fee/gross leak (a stale
fee computed from the pre-drift gross, which was a checkable arithmetic
signature identifying drifted rows without touching the ledger) was worth
doing immediately; giving every row independent, non-degenerate fee variation
is a separate piece of work, not needed for anything Day 1 tests or measures.

**Precondition for detecting fee discrepancies, if ever prioritized:** fee
has to become an independently varying claim -- not a fixed rate, and not
recomputed identically from gross on both the honest and faulted paths --
before a `FEE_MISMATCH` fault type or detector could mean anything.

---

## The invariant-gauge detection-latency number is measured at a shortened interval, not the production one

`RogueLedgerEntrySabotageTest` (Day 5) prints and asserts a real,
wall-clock-measured detection latency for `ledger_invariant_delta_minor` --
e.g. "corruption visible within ~1.5-2s." That number is measured with
`ledgerline.metrics.invariant-check-interval` overridden to 2s via
`@SpringBootTest(properties=...)`, not the 15s production default
(`application.properties`).

**Why the override exists:** so the test measures a real number quickly
rather than waiting out a full 15s recompute cycle on every run. The
mechanism this proves -- the gauge reliably moves within one scheduled
recompute of corruption landing, with nothing computed synchronously on the
scrape path to widen that further -- is unaffected by which interval is
configured; the specific millisecond figure is not.

**What this means for anyone quoting the number:** under the actual 15s
production default, the honest worst-case detection bound is close to 15s
(time until the next `@Scheduled` firing), not the 2s the test measures
under. The test's own printed output and class Javadoc both state this
explicitly now, specifically so the number isn't lifted into a README or
said aloud without its configuration attached.

**Closing mechanism, if ever prioritized:** a second assertion in the same
test class, run under the real 15s default (accepting the slower test), to
have a directly-measured production-interval number on record rather than
one derived by inference from "the interval is the bound."
