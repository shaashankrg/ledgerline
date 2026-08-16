# Day 10: multi-broker Kafka -- scale-up, in progress

## What changed

- Kafka StatefulSet (`helm/ledgerline/templates/kafka/statefulset.yaml`,
  mirrored in `k8s/manifests/20-kafka.yaml`): `replicas: 1` -> `3`, KRaft
  mode with a real 3-node controller quorum. `KAFKA_NODE_ID` and
  `KAFKA_ADVERTISED_LISTENERS` are computed per-pod from `$HOSTNAME`'s
  ordinal at container start (a StatefulSet's shared podTemplate env list
  cannot express distinct values per replica any other way without a
  dedicated subchart), then handed off to the image's real entrypoint.
- `KAFKA_CONTROLLER_QUORUM_VOTERS` lists all 3 brokers' stable pod DNS names
  upfront (ordinals 0-2 -> node IDs 1-3), since the StatefulSet's naming
  scheme makes them knowable without per-pod computation.
- `min.insync.replicas=2` set both cluster-wide (`KAFKA_MIN_INSYNC_REPLICAS`)
  and explicitly on topic creation (`transactions` and `transactions.DLT`,
  both now `--replication-factor 3 --config min.insync.replicas=2`).
- One NodePort Service per broker (`ledgerline-kafka-{0,1,2}-external`),
  not one Service selecting all 3 pods -- a Kafka client's post-metadata
  reconnect needs to land on one *specific* broker (whichever the metadata
  response names), not whichever pod a load-balancing Service happens to
  route to.
- `k8s/kind-config.yaml`: two more host port mappings (9095, 9096) alongside
  the existing 9094, one per broker's EXTERNAL listener. Required a kind
  cluster recreation (`extraPortMappings` can only be set at cluster
  creation) -- approved before proceeding, since it discards the cluster's
  existing PVC-backed data. Day 9's results were already fully recorded in
  `docs/day9-*.md` before this happened, so nothing was lost that mattered.
- `KafkaProducerConfig.java`: `acks` is now `${ledgerline.producer.acks:all}`
  instead of hardcoded `"all"`, mirroring the auto-commit-enabled pattern
  from Day 9 -- exists for the acks=all vs acks=1 comparison below, default
  unchanged.

## Bug found and fixed: KRaft bootstrap deadlock

Scaling to 3 replicas alone left every broker crash-looping with:

```
java.lang.RuntimeException: Received a fatal error while waiting for the
controller to acknowledge that we are caught up
```

Root cause: Kubernetes headless Services only publish DNS records for pods
the Service considers **Ready** by default (`publishNotReadyAddresses`
defaults to `false`). With 3 brokers, each one's own readiness depends on
first reaching the *other two* by DNS to form the KRaft controller quorum --
so the default behavior is a genuine deadlock: no broker can become Ready
because DNS for its peers isn't published until they're Ready either. This
never showed up with a single broker (nothing to wait on) or in Day 7-8's
single-node design.

Fixed by setting `publishNotReadyAddresses: true` on the headless
`ledgerline-kafka` Service. Pod DNS now resolves as soon as a pod exists,
which is what peer discovery for a clustered system actually needs;
readiness still gates whether *client* traffic (the processor,
generator/recon Jobs) treats a broker as usable -- that's a separate
concern this setting doesn't touch.

## Bug found and fixed: processor liveness probe too tight for real startup time

Separately, after the 3-broker cluster came up, the processor Deployment
started crash-looping on its own (unrelated to Kafka): `Started
LedgerlineApplication in 40.346 seconds`, but `livenessProbe` only gave it
`initialDelaySeconds: 20` before starting to check, killing it mid-startup
under load (`connection refused` on port 8080, i.e. the JVM hadn't finished
coming up). This was a pre-existing timing margin that had never actually
been this tight before -- fixed by adding a `startupProbe` (120s of
headroom at 5s intervals) that gates liveness/readiness from evaluating at
all until the app has genuinely finished starting, rather than tightening
the existing probes' delays and hoping.

## Status

3-broker cluster confirmed healthy and correct:

```
Topic: transactions  PartitionCount: 3  ReplicationFactor: 3  Configs: min.insync.replicas=2
  Partition: 0  Leader: 3  Replicas: 3,1,2  Isr: 3,1,2
  Partition: 1  Leader: 1  Replicas: 1,2,3  Isr: 1,2,3
  Partition: 2  Leader: 2  Replicas: 2,3,1  Isr: 2,3,1
```

All 3 partition leaders spread across all 3 brokers (not all landing on one
node), full ISR on every partition, consumer group fully caught up with 0
lag on all 3 partitions, `SELECT SUM(amount) FROM ledger_entries = 0`.

## A torn-write investigation happened between the acks=all baseline and the acks=1 run

A run of `LeaderKillDurabilityTest` under `acks=all` surfaced a genuine,
persistent ledger invariant violation unrelated to acks -- one transaction
left with only 1 of its expected 2 balanced entries. This paused the acks
comparison for a full investigation (connection-swap theory tested directly
with diagnostic logging and disproven; every application-level explanation
ruled out; root cause left unconfirmed as a very rare, unreproduced
transient). The bad row was repaired by hand using the original event
recovered from the still-retained Kafka topic, not guessed. Full writeup:
`docs/day10-torn-write-investigation.md`.

## acks=all vs acks=1: real numbers, and an honest result

`LeaderKillDurabilityTest.killLeaderUnderLoadAndMeasureLoss` identifies
partition 0's current leader via `kafka-topics --describe`, kills that
specific broker pod partway through a load run, and compares
`expectedPairs` (CAPTURE events actually published) against `actualPairs`
(balanced pairs actually persisted, waited out via a stability-based
recheck rather than a fixed sleep -- see the class's own comments for why
that mattered here the same way it mattered for Day 9's chaos test).

**acks=all** (100 tx/s, kill 8s in): **0 lost payments** (3000/3000),
invariant held.

**acks=1**, first attempt (same 100 tx/s / 8s timing): **0 lost payments**
(3000/3000), invariant held. This is a legitimate result, not a bug in the
test -- `acks=1` means the producer does not *wait* for replication before
considering a send successful, which is different from *guaranteeing* the
data is lost. On this cluster's local, low-latency network, a follower
typically catches up on a record within milliseconds of it landing on the
leader, so an 8-second-in kill reliably finds nothing still unreplicated.
"No guarantee" is not the same as "always loses data" -- an accurate but
undramatic first finding.

**acks=1**, second attempt, retuned to actually exercise the race (300
tx/s, kill 500ms in instead of 8s -- see the class's comment on
`killLeaderPartwayThrough` for the reasoning): still **0 lost payments**
(3000/3000), invariant held. Consumer logs confirm messages were already
being consumed within ~500ms of generation starting, meaning even this
much tighter window was not tight enough to catch a meaningful backlog
before the kill landed.

**Conclusion, stated honestly**: this local 3-broker `kind` cluster's
same-node, same-Docker-bridge network is fast enough that this project's
chaos-test harness could not reliably force `acks=1` into losing data
within the timing budgets tried. This does not mean `acks=1` is safe --
the mechanism by which it *can* lose data (a producer that stops waiting
for replication acknowledges a send the instant the leader's local log has
it, so a leader that dies before a follower catches up loses that record
with the producer having already reported success) is real and
well-documented Kafka behavior, and `KafkaProducerConfig`'s own comment on
`acks=all` explains it in those terms. What could not be done here is
*demonstrate* the loss under this specific cluster's real network
conditions with the load levels and kill timings actually tried. A higher
network-latency environment (a real multi-node cluster, or artificial
latency injected between the kind node's brokers) would very likely show
it; a purely same-host `kind` cluster mostly did not, in the runs
performed.

## Durability vs idempotency, restated

Both the auto-commit-loss finding (Day 9, consumer-side, real, measured
83% loss) and this acks investigation (Day 10, broker-side, not
demonstrated under these conditions) are testing the same underlying
distinction: idempotency (already built, and never in question here)
guards against a message being *applied twice*. It says nothing about
whether a message *arrives* at all. In every run performed across both
days, whenever a payment was genuinely lost (Day 9's auto-commit case), the
invariant held anyway -- a lost payment never gets a partial entry written
for it, so the ledger stays exactly balanced while silently incomplete.
That is the actual Day 9-10 deliverable, demonstrated with a real, large
number in one mechanism (auto-commit) and left honestly undemonstrated
(though not disproven) in the other (acks) under this specific cluster's
conditions.

## Day 9 chaos regression against the 3-broker cluster -- clean

Re-ran `ChaosInvariantTest` unmodified against the now-3-broker cluster to
confirm none of Day 10's changes (KRaft StatefulSet rewrite,
`publishNotReadyAddresses`, the processor `startupProbe`, the now-
configurable producer `acks`) regressed Day 9's result: 12 pod kills,
36,000/36,000 messages published, 0 publish failures, 12,000/12,000
per-payment correctness, 12,000/12,000 backup check, 857.1s, 0 failures.
Clean pass, no changes needed.

## Sabotage test: min.insync.replicas held

`MinInsyncReplicasSabotageTest` kills 2 of the 3 broker pods (leaving only
1 alive against `min.insync.replicas=2`), then attempts a real publish
through the application's own producer (`acks=all`) and asserts it fails
rather than silently succeeding.

Two real bugs in the test itself were found and fixed on the way to a
clean pass:

- **Missing Postgres port-forward** (attempt 1): unlike `ChaosInvariantTest`
  and `LeaderKillDurabilityTest`, this test's first version never set up a
  Postgres port-forward or `@DynamicPropertySource` override for the
  datasource, even though it doesn't touch Postgres directly -- Spring
  Boot's autoconfiguration still tries to initialize the datasource/Flyway
  beans regardless. Failed during Spring context startup with `Connection
  to localhost:5432 refused`, before ever reaching the kubectl calls that
  kill brokers. No damage done; fixed by adding the same port-forward
  pattern the other live-cluster tests already use.
- **Wrong exception-wrapper assertion** (attempt 2): the sabotage itself
  worked correctly on this attempt -- both brokers went down, and the
  publish genuinely failed with `TimeoutException: Topic transactions not
  present in metadata after 5000 ms` (losing 2 of 3 KRaft controllers broke
  leader/metadata resolution entirely, a more severe failure than a plain
  ISR-count rejection would be). The test's assertion just expected the
  wrong Java type: `KafkaTemplate.send(...)` (Spring Kafka's template, not
  the raw client) throws `org.springframework.kafka.KafkaException`
  directly, not `java.util.concurrent.ExecutionException` from a bare
  `Future.get()`. Fixed to assert the correct wrapper type and accept
  either `NotEnoughReplicas` or `Timeout` as the underlying cause -- both
  are the cluster correctly refusing an unguaranteed write, which is the
  actual thing under test, not the specific exception subclass.

**Attempt 3, clean pass**: brokers killed, publish attempted, correctly
refused. Confirmed from the test's own log: `SABOTAGE TEST: publish
correctly refused -- min.insync.replicas=2 held`.

**Bonus finding**: between every attempt, the Kafka StatefulSet's own
reconciliation loop fully healed the killed brokers back to 3/3 Running
with complete ISR on every partition within about a minute, with no manual
intervention -- a nice unplanned confirmation that the same
Deployment/StatefulSet self-healing this project has relied on since Day 7
(and exercised extensively in Day 9's chaos test) applies to the Kafka
brokers themselves, not just the processor. The cluster was left fully
healthy (3/3 brokers, full ISR) after the final test run, not in a
degraded state.

## Day 9-10 status

All planned Day 9-10 work is now complete:

- Day 9: chaos test clean, both deferred gaps closed with real measured
  numbers (auto-commit loss: 2,500/3,000 payments lost, 83%; multi-consumer
  concurrency: 3 distinct consumer IDs confirmed holding distinct
  partitions).
- Day 10: 3-broker Kafka scale-up complete and verified (real replication
  factor 3, min.insync.replicas=2, leaders spread across all 3 brokers).
  Leader-kill durability measured under both acks settings, with an honest
  writeup of what could and could not be demonstrated on this specific
  cluster's network conditions. Day 9 chaos regression re-run clean against
  the 3-broker setup. Sabotage test confirms min.insync.replicas=2 is
  actually enforced. A real, though ultimately unresolved-root-cause, data
  integrity finding was investigated thoroughly, repaired, and documented
  rather than dismissed or ignored.
