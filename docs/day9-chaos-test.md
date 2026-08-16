# Day 9: ChaosInvariantTest -- results and deferred-gap closures

## The clean run

`ChaosInvariantTest.chaosRunHoldsTheInvariantContinuouslyAndLosesNothing`,
run against the live Helm-deployed `kind` cluster (3 processor replicas,
single-broker Kafka, real Postgres), 2026-08-15 18:22-18:36 CDT:

- 12,000 transactions generated (36,000 messages: AUTHORIZE, CAPTURE, SETTLE
  per transaction), 20 tx/s sustained, **0 publish failures**.
- **13 processor pods killed** (`kubectl delete pod`, one random replica
  every 30-60s) across the ~10-minute generation window. Every kill was
  absorbed by the Deployment recreating the pod on its own -- none were
  manually recreated.
- `ledger_invariant_delta_minor` polled every 5s throughout: **zero
  violations that persisted past a short settle recheck** (see "A false
  positive, caught and fixed" below for what that recheck exists to rule
  out).
- **Per-payment assertion (primary): 12,000/12,000 transactions correct** --
  every transaction has exactly the ledger entries its own published event
  sequence implies, derived from `GeneratorResult.messages()`, not assumed.
- **Pair-count backup check (secondary): 12,000 expected == 12,000 actual**,
  36,000 distinct idempotency keys (3 per transaction) -- reported for
  visibility, never the primary signal.
- Resting-state confirmation after the run: `invariantDeltaMinor() == 0`,
  `unbalancedAccounts()` empty.

Total run time: 848.5s (~14 minutes: ~10 min generation + drain settle +
assertion time).

## A false positive, caught and fixed

The first real run of this test (before the fix below) failed with several
observed "violations" -- `SUM(amount)` readings like -19.17, -252.36,
-457.51 -- each landing within seconds of a pod kill. Investigated rather
than assumed: a direct post-run query
(`SELECT transaction_id, count(*), SUM(amount) FROM ledger_entries GROUP BY
transaction_id HAVING count(*) <> 2 OR SUM(amount) <> 0`) returned **zero
rows** -- every transaction ever written had exactly 2 balanced entries.
Nothing was actually broken.

The mechanism: `LedgerQueries.invariantDeltaMinor()` is one unparameterized,
unsnapshotted `SELECT SUM(amount)` against a table 3 replicas are actively
committing balanced pairs into several times a second. Each individual pair
commits atomically (`LedgerWriter.recordEntryGroup`'s single
`@Transactional` boundary makes a torn pair commit impossible) -- but the
poll is not coordinated with commit timing at all, so a sample landing
between two concurrent, unrelated commits sees "N pairs fully committed, M
more mid-flight," which sums to something other than zero at that exact
instant even though nothing is wrong. That is expected noise under
concurrency, not corruption.

**Fix:** a nonzero sample now triggers an immediate, tight-interval recheck
(up to 5 attempts, 200ms apart -- comfortably longer than any single
transaction's commit latency, comfortably shorter than the 5s poll
interval). Only a sample that is *still* nonzero after that short settle
window is recorded as a real violation. This preserves the original intent
(catch a violation that recovers before the next scheduled 5s poll) while no
longer conflating "the table was mid-write when I looked" with "something is
actually broken."

A second, related bug was found and fixed the same way: the pre-flight
`confirmThreeReplicasAreGenuinelySpreadAcrossPartitions` check and the
pair-count backup check both had the identical shape of problem --
`kafka-consumer-groups --describe` can catch the group mid-rebalance (every
`CONSUMER-ID` reads `-` for one snapshot), and the backup check's
`balancedPairCount()` was originally unscoped, comparing this run's expected
pairs against *every* pair ever written to the shared, long-lived test
cluster (leftover data from earlier `emit`/`generate` runs and prior test
attempts). Both were fixed the same way: retry the pre-flight check for up
to 30s instead of trusting one snapshot, and scope both `balancedPairCount`
and the drain-wait's own count query by this run's `runId` prefix, exactly
as the per-payment check already did.

## Deferred gap: multi-consumer concurrency -- closed

Every earlier test in this project ran a single process -- one JVM, one
consumer, trivially "not stepping on itself." This is the first time three
independent, real OS processes hold different partitions of the same topic
and write to the same ledger concurrently, under load, with one of them
liable to be killed at any moment. A single-process test cannot prove this
by construction; there is nothing to interleave with.

`confirmThreeReplicasAreGenuinelySpreadAcrossPartitions` confirmed -- via
`kafka-consumer-groups --describe`, not assumed -- exactly 3 distinct
`CONSUMER-ID`s holding the topic's 3 partitions before the run began. The
10 minutes of concurrent writes that followed, settling into a ledger with
zero unbalanced transactions and zero missing/duplicated entries, is the
actual proof that concurrent consumers do not interfere with each other or
with the ledger's correctness.

## Regression re-run after the AckMode fix

`KafkaConsumerConfig`'s container factory changed (see
`docs/day9-auto-commit-loss.md`) to switch `AckMode` based on the
auto-commit flag -- a change to shared production code, not test-only. Ran
the full 10-minute chaos test again afterward, with the flag back at its
normal `false`/`MANUAL_IMMEDIATE` setting, to confirm no regression:
2026-08-15 19:11-19:26 CDT, 12 pod kills, 36,000/36,000 published, 0 publish
failures, 12,000/12,000 per-payment correctness, 12,000/12,000 backup check,
resting-state `SUM(amount) = 0`. Some transient nonzero readings appeared in
the app's own 15s-interval `LedgerInvariantGauges` metric (the same benign
concurrent-commit timing artifact documented above, in a different
component with its own scheduler) -- `ChaosInvariantTest`'s own poller,
which is the actual pass/fail authority and has the settle-recheck fix,
recorded zero real violations. Clean pass, 921.1s.

## Deferred gap: auto-commit loss -- closed

Measured separately against the live Deployment with
`processor.consumer.autoCommitEnabled=true` applied via a real `helm
upgrade` (not simulated): **2,500 of 3,000 payments (83%) lost** under one
pod kill mid-batch. Auto-commit was reverted afterward and the reversion
confirmed from the running pods' own logs. Full writeup, including a real
Spring Kafka `AckMode`/auto-commit incompatibility bug found and fixed along
the way, in `docs/day9-auto-commit-loss.md`.
