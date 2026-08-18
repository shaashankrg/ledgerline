# Ledgerline

A payment ledger and reconciliation engine, built to prove — not just claim —
that it survives real failures: killed pods, killed Kafka brokers, lost
network connections, all under real load against a real Kubernetes cluster.

## What's here

- A Spring Boot processor that consumes payment lifecycle events (authorize,
  capture, settle, refund) off Kafka and writes a double-entry ledger to
  Postgres.
- A reconciliation engine that matches the ledger against settlement files,
  including fuzzy matching for the kind of drift real settlement data has.
- A synthetic transaction generator with deliberate fault injection, used to
  measure the reconciliation engine's actual precision and recall rather than
  assume it.
- A chaos test suite that kills real Kubernetes pods and Kafka brokers under
  sustained load and asserts the ledger never loses or corrupts a payment.
- A Helm chart deploying the whole system (3-broker Kafka, Postgres,
  Prometheus/Grafana, the processor, reconciliation CronJob) to a local `kind`
  cluster with one command.

## Quick start

```
make demo
```

Builds the processor image, stands up a local Kubernetes cluster, and
installs everything. Grafana dashboard at `http://localhost:3000` once it's
up.

Other targets: `make chaos` (the real chaos test), `make recon` (trigger
reconciliation), `make bench` (load ramp), `make sabotage` (kills 2 of 3
Kafka brokers to prove writes fail loudly rather than silently), `make down`
(tear down the cluster).

## Known limitations

See `docs/known-limitations.md` for gaps and open questions recorded
deliberately, including one unresolved data-integrity finding worth reading
before relying on this in anything real.
