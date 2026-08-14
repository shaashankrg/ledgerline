# Day 6 load ramp: findings

One ramp run (`LoadRampCommand`, `--spring.profiles.active=load`), against the
real docker-compose stack (real Postgres, real Kafka, a real consumer
process) with the Grafana dashboard live throughout -- not an ephemeral
Testcontainers instance nobody is watching. Rates 20/50/100/200/400 tx/s,
20s per step, two accounts.

## What happened

The generator's actual publish rate flattened well before the consumer could
keep up:

| target rate | messages published | wall time | actual rate |
|---|---|---|---|
| 20 tx/s | 1,200 | 20.0s | 59.9 msg/s |
| 50 tx/s | 3,000 | 20.0s | 149.9 msg/s |
| 100 tx/s | 6,000 | 20.0s | 299.8 msg/s |
| 200 tx/s | 12,000 | 25.4s | 471.9 msg/s |
| 400 tx/s | 24,000 | 48.3s | 496.7 msg/s |

(Each "transaction" is 3 messages -- AUTHORIZE, CAPTURE, SETTLE -- which is
why actual rate is roughly 3x the target tx/s at the low end.) Producer-side
throughput itself caps out around **500 msg/s**; the 400 tx/s step took more
than twice its allotted 20s to finish publishing at all. That is a real
ceiling worth naming but is not this write-up's subject -- the consumer fell
behind long before the producer's own ceiling mattered.

## The bottleneck: single-threaded consumer, not the database

Immediately after the ramp (`ledgerline.consumer.concurrency=1`, the
project's default):

- **Consumer lag**: 9,113 / 10,190 / 3,697 messages queued on partitions 0/1/2
  (~23,000 total) and still climbing during the ramp itself.
- **`ledgerline_end_to_end_latency_seconds_max`**: 209.7s -- a message that
  waited over three minutes between publish and ledger commit.
- **`hikaricp_connections_active`**: 1 (out of a pool of 10, on both the
  primary and recon-role pools) -- the database was doing essentially nothing
  while a quarter of the transactions topic sat unconsumed.

That combination -- deep, growing consumer lag next to an almost entirely
idle connection pool -- rules out the database as the bottleneck by direct
evidence, not by assumption. `EXPLAIN`-driven suspicion of a slow query or
lock contention would show up as *high* pool utilization with connections
waiting on the database, not one idle connection sitting next to thousands
of unconsumed messages. The shape of the evidence points at the consumer
side specifically: `KafkaConsumerConfig` runs exactly one listener thread
(`ledgerline.consumer.concurrency` defaults to `1`) against a topic with
three partitions, so two of the three partitions could never be worked in
parallel with the third at all -- one thread serially draining three queues.

Measured drain rate after the ramp stopped (concurrency=1): total lag fell
from ~25,200 to ~12,357 over 240s, roughly **54 msg/s** sustained.

## Causal check: does the ceiling actually move?

Restarted the app with `--ledgerline.consumer.concurrency=3` (matching the
topic's partition count -- confirmed in the boot log, one listener thread per
partition: `transactions-0`, `transactions-1`, `transactions-2`, each on its
own container thread) and reran the identical ramp.

Post-ramp lag this time: 5,929 / 5,289 / 4,900 (~16,118 total) -- already
markedly lower than the concurrency=1 run's ~23,000 for the same input.
Measured drain rate: ~16,118 messages gone within 120s, roughly **134 msg/s
sustained** -- about **2.5x** the concurrency=1 drain rate. `hikaricp_connections_active`
stayed at 0-2 throughout, confirming the database still was not the limiting
resource even once the consumer could actually apply back-pressure to it.

**The named cause moved the ceiling in the predicted direction, by a
substantial margin, with the resource everyone would expect to be
constrained (the DB pool) staying slack in both runs.** That is the causal
verification Day 6 calls for -- not "concurrency helps in general," a claim
already obvious from queueing theory, but this specific bottleneck, on this
specific pipeline, tested by nudging exactly the resource identified and
watching the actual number change.

## What this does not establish

- **Where the *next* ceiling sits once the consumer is no longer the limit.**
  Concurrency=3 was chosen to match the partition count, not derived from a
  second ramp against it -- the Day 6 prompt is explicit that one ramp run is
  sufficient and a second run or a connection-pool ablation is deliberately
  out of scope here (that rigor already exists as a published methodology
  elsewhere). A natural next question -- does the producer's own ~500 msg/s
  ceiling become the binding constraint once the consumer is no longer
  serialized, or does something else (DB write contention at higher
  concurrency, broker fsync limits) show up first -- is left for whoever
  picks this up next, not answered here.
- **Production-appropriate concurrency.** 3 was chosen to match this
  environment's partition count for the causal test, not as a recommendation
  -- a real deployment's right value depends on partition count, consumer
  hardware, and downstream write capacity together.

## Reproducing this

```
make up                                  # postgres, kafka, prometheus, grafana
./mvnw spring-boot:run                   # the consumer under test (add
                                          #   -Dspring-boot.run.arguments="--ledgerline.consumer.concurrency=N"
                                          #   to vary concurrency)
./mvnw spring-boot:run \
    -Dspring-boot.run.profiles=load \
    -Dspring-boot.run.arguments="--rates=20,50,100,200,400 --seconds-per-step=20"
```

Watch consumer lag, the invariant gauges, and reconciliation exceptions live
at http://localhost:3000 (Ledgerline dashboard, provisioned automatically)
while the ramp runs.
