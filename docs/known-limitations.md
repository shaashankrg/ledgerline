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

---

## 2026-08-16 -- Torn ledger write during Day 10 leader-kill testing: root cause not confirmed after genuine reproduction attempts

**This entry blocks Day 11** (repeatability reruns and CI automation) until
its outcome is explicitly accepted. See "Decision point" at the end.

### What actually happened -- symptom level

During Day 10's leader-kill chaos testing, one run of `LeaderKillDurabilityTest`
(`acks=all`, runId `leaderkill-1786843219682`, a 3000-transaction / 100 tx/s
run against the real 3-broker cluster with the partition leader killed 8s
in) ended with its own final assertion failing:

```
SELECT SUM(amount) FROM ledger_entries  ->  -247.2750, not 0
```

This was first noticed by that test's own end-of-run invariant assertion,
not by a continuous poller -- `LeaderKillDurabilityTest` checks the
invariant once, after its settle-wait, not throughout the run.
`ChaosInvariantTest`'s continuous poller (with its settle-recheck fix, Day
9) was **not** running at the time -- confirmed by querying for any
`chaos`-prefixed transaction in the incident's time window
(`2026-08-16 01:15-01:30`), which returned zero rows. That fix could not
have masked this occurrence because it was not active.

The specific affected record, identified via:

```sql
SELECT e.transaction_id, count(*), SUM(e.amount), t.idempotency_key
FROM ledger_entries e JOIN transactions t ON t.id = e.transaction_id
GROUP BY e.transaction_id, t.idempotency_key
HAVING count(*) <> 2 OR SUM(e.amount) <> 0;
```

was transaction id 28038 (`leaderkill-1786843219682-txn-2157:CAPTURE`),
ledger_entries row id 18663: a single entry, `account_id=2, amount=-247.2750`,
where a CAPTURE must always produce a balanced 2-entry pair (debit + credit
summing to zero). The transaction's full AUTHORIZE/CAPTURE/SETTLE sequence
(ids 28037, 28038, 28040) each claimed a distinct idempotency key exactly
once -- not a retried or duplicated event.

One additional fact established during this investigation, not previously
recorded: the incident write landed at `01:23:16.412975Z`, **173 seconds**
after this run's generation phase began (`01:20:23.004811Z`) -- deep into
an abnormally slow post-kill catch-up tail (this run's full span was
199 seconds for a generation phase that should take ~30s), not near the
kill itself. The consumer's processing cadence immediately before and
through this transaction was steady (~35-50ms between events, no visible
gap), meaning the write happened during ordinary, undisturbed catch-up
traffic, not adjacent to any visible disruption. The exact wall-clock
offset between the leader kill and this write could not be independently
re-confirmed from durable logs -- the console output of that specific test
invocation was not redirected to a saved file (a gap in this session's own
tooling, not evidence of anything about the bug), and Kubernetes' own event
retention window had rolled past the incident by the time this follow-up
investigation began. This is stated plainly as an evidence gap, not papered
over.

### What the repair did

The missing credit entry was reconstructed from the **original Kafka
message itself**, not inferred or guessed -- the `transactions` topic still
retained the record at investigation time:

```
kafka-console-consumer --bootstrap-server localhost:29092 --topic transactions \
  --from-beginning --timeout-ms 15000 | grep 'leaderkill-1786843219682-txn-2157'
```

returned the original CAPTURE event: `fromAccountId=2, toAccountId=1,
amount=247.2750` -- exactly matching the surviving debit (`account_id=2,
amount=-247.2750`). The missing credit (`account_id=1, amount=+247.2750`)
was inserted directly:

```sql
INSERT INTO ledger_entries (transaction_id, account_id, amount)
VALUES (28038, 1, 247.2750);
```

**Confidence check:** the repair was verified two ways, both re-confirmed
as part of this follow-up investigation, not assumed to still hold:
`SELECT count(*), SUM(amount) FROM ledger_entries WHERE transaction_id =
28038` now returns `2, 0.0000`, and `SELECT SUM(amount) FROM ledger_entries`
(the whole-table invariant) returns `0.0000`. More importantly, the repair
matches the **recovered original event**, not just "whatever balances the
row" -- had the debit itself been the erroneous half (rather than the
credit being missing), balancing the row alone would have produced a
plausible-looking but wrong repair. Cross-checking against the source
event is what makes this a correction, not a guess dressed up as one. This
is the one and only manual write against `ledger_entries` in this
project's history.

### Timing and correlation -- checked directly, not reasoned about abstractly

- **Pod kill (this run's own):** the incident write happened during
  ordinary catch-up traffic ~173s after generation start, not adjacent to
  any visible gap in consumer processing. The exact kill-to-write interval
  is not independently recoverable (see evidence-gap note above).
- **Day 9's auto-commit flip:** ruled out. That measurement ran on the
  single-broker kind cluster that existed *before* Day 10's 3-broker scale-
  up, which required a full cluster recreation (`kind delete cluster` /
  `kind create cluster`) to add the extra broker port mappings. That
  cluster instance no longer existed by the time this incident's cluster
  (the 3-broker one) was even created -- there is no cluster-instance
  continuity for a correlation to run through.
- **The 2-of-3-brokers-down sabotage test:** ruled out on ordering alone --
  that test was written and run well after this incident, against a
  cluster state the incident predates.
- **Day 9's settle-recheck fix masking this:** ruled out directly (see
  above) -- `ChaosInvariantTest` was not running at the time.
- **The exec-probe-timeout / stale-build bugs from Day 7-8:** no supporting
  evidence found. Those were Kafka-broker-liveness-probe and Docker-image-
  staleness issues respectively, unrelated to the Postgres write path, and
  the consumer's processing cadence around the incident shows no gap
  consistent with an unexpected pod restart at that moment. Not proven
  impossible, but nothing points toward it either -- ruled out on the
  balance of available evidence, not with certainty.
- **Postgres/JDBC-level timeout configuration:** ruled out with certainty --
  grepped the entire repository for `statement_timeout`,
  `idle_in_transaction`, `lock_timeout`, `socketTimeout`, `loginTimeout`;
  none exist anywhere in the project. No configured timeout could have
  silently truncated the transaction.
- **The reconciliation engine:** ruled out with certainty, at the database
  grant level, not just by code inspection -- `recon_role` (the Postgres
  role the recon pathway's `DataSource` connects as) has no `GRANT` of any
  kind on `ledger_entries` in any migration (`V11`-`V13`, the only
  migrations that grant privileges to `recon_role`, grant only on
  `recon_exceptions`, `recon_line_outcomes`, `recon_runs`). The database
  itself would reject a write to `ledger_entries` through that role even if
  a code path attempted one.

### Root cause -- reproduction attempted, not achieved

Two lines of investigation were run, both documented in
`docs/day10-torn-write-investigation.md` and summarized here:

**Connection-swap theory (session-of-origin):** suspecting a HikariCP
connection swap mid-transaction (motivated by real `PSQLException:
Connection reset` errors observed elsewhere in that session's chaos runs),
temporary diagnostic logging was added to `LedgerWriter.recordEntryGroup`
logging the JDBC connection's identity hash at transaction start and after
every insert. Re-running the same leader-kill scenario with this logging
active produced 1982 `recordEntryGroup` calls, all `entries=2`, exactly
3964 "after insert" lines (a clean 1982 x 2), and **zero**
`sameAsStart=false` occurrences. This directly disproves a connection swap
as the mechanism, at least for the runs where it was checked. No torn
write recurred in that attempt either.

**Direct reproduction attempts (this follow-up investigation):** a
dedicated test, `TornWriteReproductionTest`, was written to mirror the
*exact* original incident conditions -- 100 tx/s, leader killed 8s in
(not the faster 500ms/300tx/s timing `LeaderKillDurabilityTest` was later
retuned to) -- with a continuous scanner checking for any unbalanced
transaction throughout generation and a 4-minute post-generation tail
(matching the ~173s window in which the original write landed). Run twice
against a healthy, freshly-verified 3-broker cluster:

- Attempt 1: 3000 transactions, leader killed at +8s, scanned for 4
  additional minutes after generation completed. **Not reproduced** --
  every transaction correctly balanced; the scanner never logged even a
  transient hit.
- Attempt 2: identical conditions, cluster reconfirmed healthy beforehand.
  **Not reproduced** -- same result.

**Total genuine reproduction effort: 2 attempts, 6000 transactions, ~10.5
minutes of dedicated real-cluster generation plus continuous scanning,
under conditions matching the original incident as closely as evidence
allows. Zero reproductions.**

**Root cause remains unconfirmed.** This is stated plainly, not as a
placeholder for a guess: every application-level code path, fault-
injection mechanism, redelivery path, database-bypass path, and (directly,
with instrumentation) the connection-swap theory have been ruled out with
evidence. Two dedicated reproduction attempts at the original conditions
did not reproduce it. The original incident, one occurrence in roughly
30,000+ ledger entries written across that session's chaos runs, was rare
enough that a clean pair of follow-up attempts not reproducing it is
consistent with either "the mechanism requires a specific, uncommon
alignment this follow-up didn't hit" or "the mechanism is gone" -- these
attempts cannot distinguish between the two.

**Is this Ledgerline's own code, or a test-environment artifact?** Neither
can be confirmed. Every application-level explanation that *would* make
this a Ledgerline code bug has been directly ruled out (`EntryPolicy`,
`EntryGroup`, `LedgerWriter`, the idempotency path, the recon path, no
timeout config, no connection swap). Nothing in the sabotage/chaos-test
harness itself writes to `ledger_entries` outside the same
`LedgerWriter.recordEntryGroup` path the application always uses, so there
is no basis for calling it a test-artifact either -- the harness does not
have a separate write mechanism that could produce this independently of
the real write path. This is not a case where the answer is "obviously the
test rig, not production" -- if this mechanism exists at all, it lives
somewhere below the code this investigation can directly instrument (the
JDBC driver, HikariCP's physical connection handling, or the container/
network layer of the local `kind` cluster), and it was not caught in the
act.

### Recurrence risk

Honest assessment: **low-frequency but not ruled out.** One confirmed
occurrence in this project's entire history, across many thousands of
chaos-test writes under real broker/pod kills; two dedicated attempts at
reproduction, in similar but not identical conditions to the original,
found nothing. The repair fixed the one instance of bad data; it does
**not** close any underlying cause, because no underlying cause was
identified to close. If the mechanism is real and specific to the kind of
severe, multi-minute network disruption a broker leader election under
sustained load can cause, it could recur under similarly severe real-world
conditions (a real multi-broker cluster losing a leader under load is not
a kind-cluster-only scenario). If the mechanism was in fact something
specific to this local `kind` cluster's container/network stack, it may
not recur in a different deployment environment at all. This investigation
cannot distinguish between those two possibilities.

### Decision point

This is **outcome (C)**: root cause not confirmed despite genuine
reproduction attempts (2 attempts, 6000 transactions, conditions matching
the original incident as closely as available evidence allows).

Per the investigation's own governing instructions, **Day 11 does not
proceed automatically under this outcome.** This is flagged back explicitly
as a decision point requiring an explicit go-ahead, not treated as low-risk
by default. The honest position: the one known bad record has been found,
understood well enough to repair correctly against source data, and every
application-code explanation has been ruled out -- but the mechanism that
produced it is not understood, and a system whose Day 11 scope is three
more full chaos reruns plus nightly CI automation around this exact
pipeline is a bet that this class of event won't recur or won't matter if
it does. That bet has not been justified by this investigation; it can
only be made explicitly, by whoever is accountable for it, with this
report in hand.
