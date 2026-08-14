# Deferred gaps

Work deliberately not done, recorded so the decision is on record rather than
looking like an oversight to a future reader.

---

## Distributed tracing

Not added in Days 5-6, and not planned for a later day either -- this is a
deliberate exclusion, not a deferral under time pressure.

**Why:** this project already has a sibling project, the LLM Gateway, with
three dashboards, tracing, and a published load-test methodology. A single
happy-path trace here (publish -> consume -> ledger write) would be strictly
weaker evidence than the Gateway's trace of a request retrying through a live
provider failover -- it would demonstrate that spans can be created and
correlated, which the Gateway already demonstrates under a harder, more
interesting failure mode. Adding it here would be redundant work chosen
because Micrometer makes it easy, not because it teaches anything new.

**What would change this:** a scenario specific to Ledgerline that tracing is
uniquely suited to explain and that the existing counters/gauges cannot --
for example, correlating a specific parked-then-drained event's full
timeline across the park and drain, which today is reconstructable from logs
and the `parked_events` table but not from one view. No such need has come up
yet.

---

## Load ramp's upper ceiling, once consumer concurrency is no longer the limit

Day 6's ramp (`docs/load-test-day6.md`) found and causally verified one
bottleneck -- a single-threaded consumer serializing three partitions -- and
stopped there, per the Day 6 prompt's explicit "one ramp run, not two."

**What's left unmeasured:** with concurrency raised to match the partition
count, where the *next* ceiling sits (producer throughput, which the same
ramp data suggests caps near 500 msg/s; DB write contention at higher
concurrency; broker fsync limits on this single-node development Kafka) is
not determined. The causal check confirmed the named cause moved the number
in the right direction, not that it is the only cause remaining at every
concurrency level.

**Why not now:** answering it needs a second ramp run at higher concurrency,
which is exactly the repeated-rigor the Day 6 prompt calls out as not worth
buying again here (a connection-pool ablation methodology already exists,
published, in the Gateway project).

---

## `LoadRampCommand` is a throwaway harness, not a permanent load-testing tool

It has no assertions, no pass/fail criteria, and prints its own results to
the log rather than to a structured report -- built to answer Day 6's one
question (where does this pipeline's throughput break, and why) against a
live, watched Grafana dashboard, not to become the project's ongoing
load-testing surface. A team that wanted repeatable load testing going
forward would want something closer to a proper benchmark harness with
recorded baselines and regression thresholds, which this deliberately is not.
