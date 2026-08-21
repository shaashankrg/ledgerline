# Ledgerline

A payment ledger and reconciliation engine built around one question: not
"does this work," but "does this survive the ways real payment systems
actually break?" Every claim below is backed by a real, repeatable test
against a real Kubernetes cluster — not a unit test with everything mocked
out.

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

## Known limitations

See `docs/known-limitations.md` for gaps and open questions recorded
deliberately — including one unresolved data-integrity finding that was
investigated thoroughly, repaired, and documented honestly rather than
buried, and one intentionally reported non-result (an acks=1 vs acks=all
comparison that didn't demonstrate what it set out to on this specific
setup, written up as such instead of massaged into a cleaner story).
