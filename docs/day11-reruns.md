# Day 11: chaos reruns -- results, one real bug found and fixed

## Instrumentation added before reruns began

Per Day 11's explicit requirement, two things were added and deployed
*before* any rerun started:

- **Extended-catch-up-window trace** (`TransactionConsumer`, DEBUG-level):
  logs offset/latency/externalTxnIds whenever a message's publish-to-commit
  latency exceeds 5s. Exists specifically because the Day 10 torn-write
  incident's exact kill-to-write timing couldn't be reconstructed after the
  fact -- this closes that evidence gap going forward. Wired to DEBUG via
  `LOGGING_LEVEL_COM_LEDGERLINE_MESSAGING_TRANSACTIONCONSUMER` (default
  INFO, set DEBUG only for these reruns).
- **Torn-write-signature check** (`ChaosInvariantTest.assertNoTornWriteSignature`):
  a new, separately-logged, separately-asserted check for the exact Day 10
  incident shape (a transaction whose entries don't form a clean 2-entry
  balanced pair), scoped to each run's own `runId`. Produces its own
  explicit `CHAOS TEST RESULT [torn-write-signature]` line, distinct from
  `[invariant-gauge]` and `[per-payment]`, so a recurrence cannot blend
  into a generic pass.

Both changes are in Day 5-6/Day 9 test and production code; both were
flagged and approved before being made.

## Rerun 1 -- clean

2026-08-16, ~11:10-11:26 CDT. 13 pod kills, 36,000/36,000 published, 0
publish failures, 974.5s.

```
CHAOS TEST RESULT [invariant-gauge]: PASS (0 real violations)
CHAOS TEST RESULT [per-payment]: PASS
CHAOS TEST RESULT [torn-write-signature]: PASS (no recurrence)
```

Zero `Extended catch-up window write` lines in any of the 3 processor
pods' logs -- consistent with a run where nothing took unusually long to
land.

## Rerun 2 -- clean

2026-08-16, ~11:38-11:44 CDT. 13 pod kills, 36,000/36,000 published, 0
publish failures, 991.3s.

```
CHAOS TEST RESULT [invariant-gauge]: PASS (0 real violations)
CHAOS TEST RESULT [per-payment]: PASS
CHAOS TEST RESULT [torn-write-signature]: PASS (no recurrence)
```

Zero extended-catch-up hits again.

## Rerun 3, first attempt -- failed, NOT a data-integrity recurrence

2026-08-16, ~11:46-11:59 CDT. Failed before reaching the per-payment or
torn-write-signature checks:

```
[every message must have actually reached Kafka -- a publish failure would
make the ground truth this test grades against wrong, not the system
under test]
expected: 0
 but was: 12
```

**This did not trip the Day 11 hard-stop condition.** The torn-write-
signature check never ran this attempt (the earlier publish-failures
assertion failed first), so there is no torn-write recurrence to report --
this is a different, new problem, investigated and documented per Day 11's
explicit instruction to write down symptom/cause/fix before patching
silently.

### Symptom

12 publish failures out of 36,000 messages (35,988 published), all 12
clustered in the first ~60 seconds of the run, all against the first 4-5
transactions generated (`txn-0` through `txn-4`). Every other transaction
in the 12,000-transaction run published successfully. The run also killed
18 pods instead of the usual 13 -- a consequence of the run legitimately
taking longer wall-clock time than normal (the killer thread runs on its
own schedule independent of generation progress), not a separate problem.

### Root cause

```
org.apache.kafka.common.errors.TimeoutException: Topic transactions not
present in metadata after 5000 ms.
```

-- exactly matching `KafkaProducerConfig`'s own `MAX_BLOCK_MS_CONFIG =
5_000`. Timeline:

- `11:46:30.739` -- Postgres port-forward starts.
- `11:46:40.117`-`11:46:42.699` -- the test's pre-flight **consumer**
  readiness check (`confirmThreeReplicasAreGenuinelySpreadAcrossPartitions`)
  confirms the consumer group stable.
- `11:46:42.845` -- the **producer** (a separate Kafka client, with its
  own separate metadata-fetch cycle) is constructed as part of the Spring
  context and immediately starts sending real generator traffic.
- `11:46:47.920` -- first publish failure: the producer's own metadata
  fetch for the `transactions` topic had not completed yet.

`ChaosInvariantTest`'s only pre-flight network check,
`awaitPortOpen`, confirms the Kafka EXTERNAL listener's TCP port accepts
connections -- it says nothing about whether *this specific test's*
freshly-constructed producer has finished resolving topic metadata over
that connection. The consumer-readiness check is a different client
entirely and being ready proves nothing about the producer's state. This
is a gap specific to this test's pattern (spin up a fresh Spring context,
immediately hammer a brand-new producer with real traffic) -- the real
processor Deployment never hits this, because its producer has been alive
and warm since pod startup, long before any request arrives.

**Not a chaos-induced failure**: the first failure landed at `11:46:47`,
before the first pod kill of this run (`11:47:15`). Kafka broker pods show
0 restarts spanning back well before this run started, ruling out a broker
disruption. This is a cold-start producer bootstrap race in the test
harness itself.

### Fix

Added `ChaosInvariantTest.awaitProducerMetadataReady()`, called
immediately after the existing consumer-readiness pre-flight check and
before real generation starts: retries `KafkaTemplate.partitionsFor(topic)`
(the same metadata fetch a real `send()` triggers, without publishing
anything -- no throwaway transaction pollutes the ledger this run is about
to grade) for up to 30s, succeeding as soon as the producer reports a
non-empty partition list for the topic.

This is a test-only change (`ChaosInvariantTest.java`); no production code
was touched, since the race does not exist in the real Deployment's own
producer lifecycle.

## Rerun 3, second attempt (with the producer warm-up fix) -- clean

2026-08-16, ~12:19-12:38 CDT. The new pre-flight check fired as designed:

```
CHAOS TEST: producer metadata ready for topic transactions
```

12 pod kills, 36,000/36,000 published, **0 publish failures** (the fix
closed the race), 1133s.

```
CHAOS TEST RESULT [invariant-gauge]: PASS (0 real violations)
CHAOS TEST RESULT [per-payment]: PASS
CHAOS TEST RESULT [torn-write-signature]: PASS (no recurrence)
```

Zero extended-catch-up hits in any of the 3 processor pods' logs.

## Summary: 3 for 3, no data-integrity recurrence

- Rerun 1: clean on first attempt.
- Rerun 2: clean on first attempt.
- Rerun 3: failed on first attempt (cold-start producer-metadata race,
  root-caused and fixed above, test-only change), clean on second attempt.

Across all three completed clean runs: 3 torn-write-signature checks run,
3 passes, 0 recurrences of the Day 10 data-integrity signature. The
extended-catch-up-window instrumentation was live and fired zero times
across all three clean runs -- consistent with the Day 10 incident being
a rare event under conditions these reruns did not happen to reproduce,
same conclusion as the dedicated reproduction attempts in
`docs/known-limitations.md`.

Per Day 11's exit criterion, this satisfies "3 consecutive clean chaos
reruns... with no recurrence of the data-integrity signature" -- counting
rerun 3's fixed, clean second attempt as the third clean result, with the
first (failed) attempt documented above rather than silently discarded.
