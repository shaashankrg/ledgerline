# Day 11: reruns, CI automation, make targets, fresh-clone verification

> **Carried-forward context, not resolved by this work**: Day 10 found one
> real, unexplained data-integrity incident (a single torn ledger write).
> Root cause remains unconfirmed after genuine reproduction attempts on
> both Day 10 and Day 11 (6,000+ dedicated repro transactions, plus three
> full chaos reruns below, zero recurrences across all of it). Full
> writeup: `docs/known-limitations.md`'s 2026-08-16 entry. That entry is
> the authoritative record -- this file covers Day 11's reruns, CI, and
> tooling work, all of which found **zero recurrences** of that signature.

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

## CI automation

Three GitHub Actions workflows, `.github/workflows/`:

- **`fast-tests.yml`** -- every push and PR. Plain `mvn test`: every test
  class runs except the six gated behind `ledgerline.chaostest` /
  `ledgerline.crashtest` / `ledgerline.chaossmoke` (`@EnabledIfSystemProperty`,
  false by default), which a bare `mvn test` silently skips rather than
  needing its own exclude list. Testcontainers-backed classes run fine on
  a standard `ubuntu-latest` runner's Docker daemon; nothing here touches a
  Kubernetes cluster.
- **`nightly.yml`** -- cron `0 9 * * *` plus manual dispatch. Two jobs:
  `crash-test` (the ~90s `CrashRecoveryTest`, Testcontainers, no cluster
  needed) and `chaos-smoke-test` (stands up a real kind cluster via
  `helm/kind-action`, installs the chart, runs the new 2-minute
  `ChaosInvariantTest#chaosSmokeRunHoldsTheInvariantContinuouslyAndLosesNothing`
  -- see below). Both dump pod state/logs on failure so a red nightly run
  is diagnosable from the Actions log alone.
- **`full-chaos.yml`** -- `workflow_dispatch` only, deliberately not on a
  schedule or push (running the real 10-minute test nightly alongside the
  smoke test would double nightly CI time for a signal the smoke test
  already gives most of). Same cluster-standup sequence, runs the full
  `chaosRunHoldsTheInvariantContinuouslyAndLosesNothing`.

### The 2-minute chaos smoke test

New: `ChaosInvariantTest.chaosSmokeRunHoldsTheInvariantContinuouslyAndLosesNothing`,
gated behind `ledgerline.chaossmoke=true` (in addition to the existing
`ledgerline.chaostest=true`) so it doesn't run alongside the full test by
default. Not a separate, weaker test -- the 10-minute and 2-minute
variants both call a shared `runChaosScenario(Duration)`, so the
mechanism, assertions, and the three explicit result lines
(invariant-gauge / per-payment / torn-write-signature) are identical;
only the duration differs. Real pod kills happen in a 2-minute window too
(kill interval is 30-60s, so 2-4 kills land), so this is a genuine,
smaller version of the same chaos exposure, not a simulation of one.

## `make` targets

The Makefile was rewritten (it previously targeted the Day 1-6
docker-compose stack, stale since the Day 7-8 migration to kind+Helm) to
front the real, current system:

- **`make demo`** -- creates the kind cluster if it doesn't exist, builds
  and loads the processor image, `helm install`s the chart, waits for the
  processor rollout, prints the Grafana/Prometheus/processor URLs.
- **`make chaos`** -- the full 10-minute `ChaosInvariantTest` against
  whatever cluster `make demo` stood up.
- **`make recon`** -- triggers the real `ledgerline-recon` CronJob
  immediately (`kubectl create job --from=cronjob/...`), not a separate
  local process against port-forwarded services -- this is the actual
  mechanism a reader would see in production.
- **`make bench`** -- runs `LoadRampCommand` (Day 6's load ramp) against
  the live cluster's real processor Deployment, so a Grafana dashboard
  watched during the run shows the real system degrading, not a simulated
  one. `RATES`/`STEP` overridable (`make bench RATES=100,200 STEP=60`).
- **`make sabotage`** -- `MinInsyncReplicasSabotageTest` against the live
  cluster: kills 2 of 3 Kafka brokers, asserts a real publish fails
  loudly. Does not restore the killed brokers afterward (documented in
  the target's own comment and the test class's Javadoc) -- `make down`
  and `make demo` again for a clean cluster afterward.
- **`make down`** -- tears down the kind cluster entirely.

A real bug was found and fixed while verifying these:
`$(or $(RATES),50,100,200,400,800)` in the original `bench` target was
parsed by GNU Make as `or(RATES, "50", "100", "200", "400", "800")` --
each comma-separated value became a *separate* argument to `$(or)`, not
part of one default string, so the default silently truncated to just
`50`. Caught by dry-running every target (`make -n <target>`) before
trusting any of them, not by assumption. Fixed with plain `?=` conditional
variable assignment (`RATES ?= 50,100,200,400,800`) instead.

## Fresh-clone verification

Per Day 11's explicit instruction not to approximate this: the existing
kind cluster was torn down, the cached `ledgerline:dev` Docker image was
removed, all of this session's work was committed locally (not pushed --
`git clone` only copies committed history, and the point was verifying
what's actually in this working tree, not the stale pre-Day-9 state on
the remote), and the repo was cloned into a genuinely new directory
(outside the working directory this session developed in).

`make demo` run from that fresh clone, on a machine with zero pre-existing
kind cluster and zero cached image, completed successfully end to end on
the first attempt: kind cluster created, image built from scratch (~2 min,
dominated by `mvnw dependency:go-offline`), loaded, chart installed,
processor rollout completed, all without any manual intervention or
retry. Verified genuinely healthy afterward (not just "the command
exited 0"):

```
NAME                                     READY   STATUS      RESTARTS   AGE
ledgerline-generator-nkkdv               0/1     Completed   0          76s
ledgerline-grafana-...                   1/1     Running     0          77s
ledgerline-kafka-0                       1/1     Running     0          3m
ledgerline-kafka-1                       1/1     Running     0          3m
ledgerline-kafka-2                       1/1     Running     0          3m
ledgerline-postgres-0                    1/1     Running     0          3m
ledgerline-processor-...  (x3)           1/1     Running     0          ~76s
ledgerline-prometheus-...                1/1     Running     0          77s
```

-- 3-broker Kafka, 3 processor replicas, all Running with zero restarts,
and the processor logs showing real `Applied event ... transaction now
SETTLED` lines from the install-time generator traffic, confirmed by
tailing them directly, not inferred from pod status alone. `make recon`
was also verified from the same fresh clone (`kubectl get jobs` showed
`manual-recon-run-... Complete 1/1`), and `make down` cleanly tore the
cluster back down afterward.
