# Ledgerline

A payment ledger and reconciliation engine built around one question: not
"does this work," but "does this survive the ways real payment systems
actually break?" Every claim below is backed by a real, repeatable test
against a real Kubernetes cluster — not a unit test with everything mocked
out.

**Stack:** Java 21 · Spring Boot 3.5 · Apache Kafka · PostgreSQL · Flyway ·
Kubernetes (kind) · Helm · Prometheus/Grafana · Testcontainers · JUnit 5 ·
GitHub Actions

**Scale:** ~18,600 lines of Java across 71 production classes and 37 test
classes, 13 Flyway migrations, a 5-target Makefile, and 3 CI workflows.

## The problem this solves

Payment systems fail in specific, unglamorous ways: a consumer process gets
killed mid-write, a Kafka broker dies during a leader election, a settlement
file arrives with amounts that don't quite match what the ledger recorded.
Most demo projects test the happy path and call it done. This one is built
the other way around — the chaos and failure-injection tests came first,
and the architecture is shaped by what it takes to pass them.

## Architecture

```
Transaction events (authorize/capture/settle/refund)
        |
        v
    Kafka (3-broker, min.insync.replicas=2)
        |
        v
  Processor (Spring Boot, 3 replicas)
        |
        v
  Postgres (double-entry ledger)
        |
        v
  Reconciliation engine <---- Settlement files (fuzzy-matched)
```

- **Processor**: consumes payment lifecycle events off Kafka and writes a
  double-entry ledger to Postgres — every transaction is exactly two
  balanced entries, enforced at the type level, not just by convention.
- **Reconciliation engine**: matches the ledger against settlement files
  using fuzzy matching (amount, merchant, time window), because real
  settlement data drifts from the ledger in ways exact matching misses.
- **Fault-injecting generator**: produces synthetic transaction streams with
  deliberate faults (duplicates, out-of-order delivery, orphaned captures,
  amount drift) so the reconciliation engine's precision and recall are
  measured against a known-correct answer key, not assumed.
- **Chaos test suite**: kills real processor pods and real Kafka brokers
  under sustained load via `kubectl delete pod`, against a live Helm-deployed
  cluster — not Testcontainers, because the point is proving Kubernetes's
  own reconciliation loop and Kafka's own consumer rebalancing actually
  work, which an ephemeral test environment can't demonstrate.

## What's actually been proven, with numbers

- **12,000 transactions, 13 real pod kills, 0 lost or corrupted payments** —
  a 10-minute chaos run against 3 live processor replicas, graded against
  what each transaction's own published events imply it should look like in
  the ledger, not a generic pass/fail.
- **2,500 of 3,000 payments (83%) measurably lost** when Kafka's
  `enable.auto.commit` is turned on and a consumer pod is killed mid-batch —
  a real, directly-counted number, used to demonstrate why the processor
  runs with manual offset commits instead.
- **A killed Kafka broker with `acks=all` loses nothing; a killed broker
  with `min.insync.replicas` violated (2 of 3 brokers down) makes the
  producer fail loudly** rather than silently accept a write it can't make
  durable — proving the durability guarantee is real, not just configured.
- **Reconciliation precision never degrades as the match window widens** —
  established by an actual parameter sweep across window sizes, per fault
  type, which is how the 24-hour production window was chosen rather than
  guessed at.

## Engineering decisions worth reading

Each of these is a place where the obvious choice was rejected for a
measured reason, documented in `docs/` rather than left implicit in the code.

| Decision | Why |
| --- | --- |
| Manual Kafka offset commits, not auto-commit | Auto-commit measurably loses 83% of in-flight payments on a mid-batch pod kill. The number came first; the config followed. |
| `acks=all` with `min.insync.replicas=2` | A write the cluster can't make durable should fail loudly. Proven by killing brokers, not by reading docs. |
| Chaos tests against real kind + Helm, not Testcontainers | Kubernetes's reconciliation loop and Kafka's consumer rebalancing are the things under test. An ephemeral container can't exercise either. |
| Real Postgres and real Kafka in unit tests (Testcontainers) | The invariant being tested is "Postgres plus our constraints produce a balanced ledger." A mocked datasource verifies nothing about that. |
| Fuzzy matching on `(amount, merchant, time window)` | Real settlement data drifts from the ledger. Exact matching produces false exceptions that a human then has to clear by hand. |
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

See `docs/known-limitations.md` for gaps and open questions recorded
deliberately — including one unresolved data-integrity finding that was
investigated thoroughly, repaired, and documented honestly rather than
buried, and one intentionally reported non-result (an acks=1 vs acks=all
comparison that didn't demonstrate what it set out to on this specific
setup, written up as such instead of massaged into a cleaner story).

`docs/deferred-gaps.md` records work deliberately *not* done and why, so the
absences are decisions on record rather than oversights. `docs/incidents/`
holds real incident write-ups from building this.
