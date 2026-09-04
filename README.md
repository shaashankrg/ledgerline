# Ledgerline

A double-entry payment ledger that ingests payment events over Kafka and
reconciles them against bank settlement files.

Built around one question: not "does this work," but "does this survive the
ways real payment systems actually break?" Every number below comes from a
real, repeatable run against a live Kubernetes cluster — not a unit test with
everything mocked out.

**Stack:** Java 21 · Spring Boot 3.5 · Apache Kafka · PostgreSQL · Flyway ·
Kubernetes (kind) · Helm · Prometheus/Grafana · Testcontainers · JUnit 5 ·
GitHub Actions

**Scale:** ~18,600 lines of Java across 71 production classes and 37 test
classes, 13 Flyway migrations, a 5-target Makefile, and 3 CI workflows.

## Why build this

When a payment system loses a transaction, nobody gets a stack trace. They
get a bank statement that doesn't match the ledger, weeks later, and someone
reconciles the difference by hand.

That failure mode is invisible to the way most projects are tested. A
consumer killed mid-write, a Kafka broker dying during a leader election, a
settlement file whose amounts drift from what the ledger recorded — none of
these show up on the happy path, and all of them are routine in production.

So this project was built the other way around: the chaos and
failure-injection tests came first, and the architecture is whatever it took
to pass them. Where a design decision could have gone either way, the number
that settled it is on record in `docs/` — including the ones that came out
inconvenient.

## Architecture

```
  Payment events                           Bank settlement files
  (authorize/capture/settle/refund)        (CSV, arrives on a schedule)
          |                                          |
          v                                          |
  +-----------------------------+                    |
  |  Kafka                      |  3 brokers         |
  |  acks=all                   |  min.insync=2      |
  |  manual offset commit       |  (commit only      |
  |                             |   after DB write)  |
  +--------------+--------------+                    |
                 v                                   |
  +-----------------------------+                    |
  |  Processor                  |  3 replicas        |
  |  idempotent consumer        |  DLQ for poison    |
  |  lifecycle state machine    |  rejects illegal   |
  |                             |  transitions       |
  +--------------+--------------+                    |
                 v                                   |
  +-----------------------------+                    |
  |  Postgres                   |  every txn =       |
  |  double-entry ledger        |  exactly 2         |
  |                             |  balanced entries  |
  |                             |  invariant gauge   |
  +--------------+--------------+                    |
                 |                                   |
                 v                                   v
        +-------------------------------------------------+
        |  Reconciliation engine                          |
        |  fuzzy match on (amount, merchant, time window)  |
        |  -> matched / unmatched / exception              |
        +-----------------------+-------------------------+
                                v
                Read-only API   ·   Prometheus / Grafana

  Graded by: a fault-injecting generator that writes the correct answers to
  a separate table the matcher never reads — so it can't score itself.
```

**Processor** — consumes payment lifecycle events off Kafka and writes a
double-entry ledger to Postgres. Every transaction is exactly two balanced
entries, enforced at the type level rather than by convention.

**Reconciliation engine** — matches ledger entries against settlement files
by amount, merchant, and time window, because real settlement data drifts
from the ledger in ways exact matching misses.

**Fault-injecting generator** — produces synthetic transaction streams with
deliberate faults (duplicates, out-of-order delivery, orphaned captures,
amount drift) and records what each fault actually was in a ground-truth
table the matcher never queries. Precision and recall are measured against
that answer key rather than assumed.

**Chaos suite** — kills real processor pods and real Kafka brokers under
sustained load via `kubectl delete pod`, against a live Helm-deployed
cluster. Not Testcontainers: the things under test are Kubernetes's own
reconciliation loop and Kafka's own consumer rebalancing, and an ephemeral
container can't exercise either.

## What's been proven, with numbers

- **Stopped 83% of payments from disappearing during processor crashes.**
  With Kafka's `enable.auto.commit` on, killing a consumer pod mid-batch lost
  2,500 of 3,000 payments — directly counted, not estimated. The processor
  commits offsets manually, only after the Postgres write succeeds. The
  measurement came first; the config followed.
  (`docs/day9-auto-commit-loss.md`)

- **12,000 payments through 13 real pod kills, zero lost or corrupted.**
  A 10-minute chaos run against 3 live replicas under sustained load
  (36,000 messages), graded per-payment against what each transaction's own
  published events imply the ledger should contain — not a generic
  pass/fail. Repeated across three full reruns plus nightly CI smoke runs.
  (`docs/day9-chaos-test.md`, `docs/day11-reruns.md`)

- **Durability holds when 1 of 3 Kafka brokers dies; breaks loudly when 2
  do.** With `acks=all` and `min.insync.replicas=2`, killing a live broker
  loses nothing. Killing a second — violating the guarantee — makes the
  producer fail rather than silently accept a write it can't make durable.
  Proven by killing brokers, not by reading docs.
  (`docs/day10-multi-broker.md`)

- **A tampered ledger entry is caught within one 15-second check cycle,
  instead of at end-of-day reconciliation.** The failure being detected is a
  single unmatched entry written straight to `ledger_entries` — an entry
  that satisfies every schema constraint (real account, real transaction,
  nonzero amount) and is wrong only in that nothing balances it. That is the
  shape a fraudulent or tampered write takes in a double-entry system, and
  no database constraint can reject it. A balance invariant recomputes on a
  timer and moves `ledger_invariant_delta_minor` within one cycle, with
  nothing computed on the scrape path to widen that. The sabotage test
  measures this at a shortened 2s interval for speed; under the 15s
  production default the honest worst-case bound is ~15s, stated that way in
  the test itself and in `docs/known-limitations.md` rather than quoting the
  faster number.

- **Reconciliation precision doesn't degrade as the match window widens.**
  Established by an actual parameter sweep across window sizes, per fault
  type, which is how the 24-hour production window was chosen rather than
  guessed at.

- **Every detection test has been sabotaged to confirm it fails.**
  Guarantees were deliberately broken one at a time — suppressed fault
  labelling, double-claimed settlements, rogue ledger entries,
  `min.insync.replicas` violations — with the outcome predicted in writing
  beforehand, then compared against what actually happened. All reverted;
  all recorded in `docs/sabotage-log.md`.

## Engineering decisions worth reading

Each of these is a place where the obvious choice was rejected for a measured
reason, documented in `docs/` rather than left implicit in the code.

| Decision | Why |
| --- | --- |
| Manual Kafka offset commits, not auto-commit | Auto-commit measurably loses 83% of in-flight payments on a mid-batch pod kill. The number came first; the config followed. |
| `acks=all` with `min.insync.replicas=2` | A write the cluster can't make durable should fail loudly. Proven by killing brokers, not by reading docs. |
| Chaos tests against real kind + Helm, not Testcontainers | Kubernetes's reconciliation loop and Kafka's consumer rebalancing are the things under test. An ephemeral container can't exercise either. |
| Real Postgres and real Kafka in unit tests (Testcontainers) | The invariant being tested is "Postgres plus our constraints produce a balanced ledger." A mocked datasource verifies nothing about that. |
| Fuzzy matching on `(amount, merchant, time window)` | Real settlement data drifts from the ledger. Exact matching produces false exceptions a human then has to clear by hand. |
| Ground truth stored where the matcher can't read it | A grader that shares state with the thing it grades proves nothing. The generator writes injected faults to a separate table; the matcher never queries it. |
| JaCoCo coverage scoped to `domain/` only | That package is pure logic with no I/O, so 100% is both achievable and meaningful. A blanket repo-wide target would be a number that means nothing. |
| Synthetic merchant identity layered over ledger accounts | Only six accounts are seeded — not enough cardinality for fuzzy matching to actually discriminate. Documented in `docs/known-limitations.md`. |

## Repo map

```
src/main/java/com/ledgerline/
  domain/          double-entry types, state machine, transition rules
  ledger/          ledger writes, queries, parked-event handling
  messaging/       Kafka producer/consumer, DLQ, consumer-group health
  reconciliation/  matching engine, fuzzy scorer, exception classification
  settlement/      settlement file simulation, CSV loading, network faults
  generator/       fault-injecting synthetic transaction generator
  api/             read-only HTTP API (balances, paginated entries)
  metrics/         Micrometer gauges, ledger invariant instrumentation
  cli/             migrate / generate / emit / recon / load-ramp entry points

helm/ledgerline/   the deployed system: Kafka, Postgres, processor, observability
k8s/               kind cluster config and raw manifests
docs/              investigations, incident write-ups, known limitations
```

## Quick start

```
make demo
```

Builds the processor image, stands up a local Kubernetes cluster (`kind`),
and installs the full system via Helm — 3-broker Kafka, Postgres, the
processor, Prometheus/Grafana. Verified from a genuinely fresh clone with no
cached state. Dashboard at `http://localhost:3000` once it's up.

Other targets: `make chaos` (the real 10-minute chaos test), `make recon`
(trigger reconciliation), `make bench` (load ramp), `make sabotage` (kills 2
of 3 Kafka brokers to prove writes fail loudly), `make down` (tear down the
cluster).

**Prerequisites:** Docker, JDK 21, `kind`, `kubectl`, and `helm`. Nothing
else — no undocumented step a reader would have to already know about.

## API

A read-only HTTP surface over the ledger, served by the processor:

| Endpoint | Returns |
| --- | --- |
| `GET /api/v1/accounts/{id}/balance` | Current balance, summed live over the account's entries |
| `GET /api/v1/accounts/{id}/entries?limit=&before=` | Cursor-paginated entries, newest first (limit clamped, not rejected) |
| `GET /actuator/health` | Liveness/readiness, used by the Kubernetes probes |
| `GET /actuator/prometheus` | Metrics scrape endpoint |

## Testing strategy

Tests are split by what they need, so the fast ones stay fast:

- **Fast suite** (every push): domain logic, ledger invariants, reconciliation
  accuracy, fuzzy matching, idempotency, out-of-order event handling. Several
  are Testcontainers-backed against real Postgres and real Kafka.
- **Crash recovery** (nightly, ~90s): spawns a second real JVM and kills it
  mid-consume — a genuine process crash, not an in-process thread interrupt.
- **Chaos smoke** (nightly, ~2 min): a live kind cluster, real pod kills.
- **Full chaos** (on demand): the 10-minute run, 3 replicas under sustained
  load with a pod killed every 30–60 seconds.
- **Sabotage tests**: deliberately break the system's own guarantees —
  double-claimed settlements, rogue ledger entries, `min.insync.replicas`
  violations — to confirm the detection actually fires.

Slow and cluster-dependent tests are gated behind system properties
(`ledgerline.chaostest`, `ledgerline.crashtest`), so a plain `./mvnw test`
runs exactly the fast suite with no include/exclude list to maintain.

## CI

Three GitHub Actions workflows:

- `fast-tests.yml` — every push and PR, full fast suite, surefire reports archived.
- `nightly.yml` — crash recovery test plus a 2-minute chaos smoke test against a
  real kind cluster, so the gated tests can't quietly rot between local runs.
- `full-chaos.yml` — the 10-minute chaos run, `workflow_dispatch` only, with
  automatic cluster-state and pod-log dumps on failure.

## Known limitations

`docs/known-limitations.md` records gaps and open questions deliberately —
including one unresolved data-integrity finding (a single torn ledger write)
that was investigated across 6,000+ dedicated reproduction transactions and
three full chaos reruns with zero recurrences, then documented honestly
rather than buried. It also records one intentionally reported non-result: an
`acks=1` vs `acks=all` comparison that didn't demonstrate what it set out to
on this specific setup, written up as such instead of massaged into a cleaner
story.

`docs/deferred-gaps.md` records work deliberately *not* done and why, so the
absences are decisions on record rather than oversights. `docs/incidents/`
holds real incident write-ups from building this.
