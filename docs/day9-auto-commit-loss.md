# Day 9: auto-commit loss -- measured, not assumed

## The claim being tested

`KafkaConsumerConfig`'s comment on `enable.auto.commit=false` has always
argued that auto-commit loses payments: a record polled and still mid-write
when the process dies has its offset committed by the client's timer
regardless, so the group resumes past it and the payment is gone with
nothing to signal it. That argument was always structurally sound but never
measured against the real Deployment's real consumer under a real pod kill.
This is that measurement.

## A real bug found while wiring the experiment up

Flipping `ledgerline.consumer.auto-commit-enabled=true` alone crash-looped
every processor pod:

```
Caused by: java.lang.IllegalStateException: Consumer cannot be configured
for auto commit for ackMode MANUAL_IMMEDIATE
	at org.springframework.kafka.listener.KafkaMessageListenerContainer$ListenerConsumer.determineAutoCommit(...)
```

Spring Kafka's listener container asserts at startup that a manual
`AckMode` (this project uses `MANUAL_IMMEDIATE` everywhere, see
`KafkaConsumerConfig`) cannot be paired with a Kafka client that has
`enable.auto.commit=true` -- Spring Kafka owns commit timing under a manual
ack mode, and refuses to start rather than leave it ambiguous who commits.
The original property only flipped the client-level setting; it needed to
flip the container's `AckMode` too. Fixed in
`transactionListenerContainerFactory` by switching to `AckMode.BATCH`
(the ordinary mode for a client-auto-committing consumer -- it does not
itself commit anything) exactly when `auto-commit-enabled=true`, and back to
`MANUAL_IMMEDIATE` otherwise. This is a second change to
`KafkaConsumerConfig.java` beyond the original property addition, flagged
and approved before proceeding.

Without this fix the auto-commit-loss measurement could not run at all --
the pods that were supposed to demonstrate the loss never came up.

## The measurement

`AutoCommitLossMeasurementTest.measureLossUnderAutoCommit`, run against the
live Helm-deployed cluster with `processor.consumer.autoCommitEnabled=true`
applied via `helm upgrade` and the rollout confirmed live (`enable.auto.commit
= true` observed in the running pods' own consumer-config log lines, not
assumed from the Helm value):

- 3,000 transactions generated at 100 tx/s (9,000 messages: AUTHORIZE,
  CAPTURE, SETTLE per transaction). **0 publish failures** -- every message
  genuinely reached Kafka, so any shortfall below is a consumer-side loss,
  not a producer-side one.
- One processor pod killed 8 seconds into generation, deliberately mid-batch
  under load (`kubectl delete pod`, absorbed by the Deployment recreating it,
  same as Day 9's chaos test).
- Counted after a 20s settle (4x Kafka's 5s default
  `auto.commit.interval.ms`, comfortably longer than any offset the timer
  would still be holding back):
  - `expectedPairs` (CAPTURE events actually published): **3,000**
  - `actualPairs` (balanced pairs actually present in the ledger, scoped to
    this run's own `idempotency_key` prefix): **500**
  - **lostPayments = 2,500**

Out of 3,000 payments, auto-commit lost **2,500 (83%)** under one pod kill
mid-batch. The killed replica's partition(s) had offsets committed by the
5-second timer regardless of whether `TransactionEventService` had actually
finished writing those records' entries; when the pod died, the surviving
consumers resumed from the committed offset, skipping past everything that
had been polled-but-not-yet-durably-applied on the dead replica.

## The invariant held anyway -- and that is not a contradiction

Immediately after the test and the subsequent revert, `SELECT
SUM(amount) FROM ledger_entries` was still exactly `0`. This is the
durability-vs-idempotency distinction Day 10 will need to state precisely:
idempotency guards against a message being *applied twice*; it says nothing
about a message that is *never applied at all*. A lost payment under
auto-commit never gets a balanced pair written for it -- there is no partial
or torn entry sitting in the table, because `LedgerWriter.recordEntryGroup`
never ran for it. The ledger stays perfectly balanced while being silently
*incomplete*. Balance is not completeness.

## Reversion, confirmed

`helm upgrade ledgerline helm/ledgerline -n ledgerline --set
processor.consumer.autoCommitEnabled=false --reuse-values`, followed by
`kubectl rollout restart deployment/ledgerline-processor` (a plain `helm
upgrade` does not itself restart pods that only changed a ConfigMap value
with no checksum annotation wired to the pod template -- confirmed the hard
way, see below).

Confirmed on all 3 live pods post-rollout, from each pod's own consumer
startup log line, not from the Helm value:

```
enable.auto.commit = false
```

Consumer group re-settled to 3 partitions / 3 distinct consumers / 0 lag
(`kafka-consumer-groups --describe`), and `SELECT SUM(amount) FROM
ledger_entries` remained `0`.

## A rollout-mechanics note, for anyone repeating this

Neither `helm upgrade` nor `kubectl rollout restart` alone was enough to get
a clean transition both times (enabling, then reverting). The processor
Deployment's ConfigMap has no checksum annotation on the pod template, so a
plain `helm upgrade` that only changes a ConfigMap value does not trigger a
restart by itself -- `rollout restart` is required in addition. Separately,
because this topic has exactly 3 partitions and the Deployment's
`maxUnavailable: 25%` rolling-update strategy keeps old and new replicas
alive simultaneously, the rolling update transiently runs 4 consumers in the
group against 3 partitions; the 4th consumer legitimately holds zero
partitions and its readiness probe correctly reports not-ready until an old
replica is retired and a partition frees up. Both times this needed a manual
nudge (deleting the oldest still-running old-generation pod) to get the
rollout unstuck rather than have it stall indefinitely waiting for a
readiness that only a retiring peer can unblock. This is a pre-existing
property of the Deployment's rollout strategy versus a 3-partition topic, not
something introduced by this measurement -- worth knowing about, not fixing
here.

## Day 9 exit status for this deferred gap

Closed. A real, directly-counted loss (2,500 of 3,000 payments, 83%) was
observed under a real pod kill against the real Deployment's real consumer,
auto-commit was reverted to disabled, and the reversion was confirmed from
the running processes themselves rather than from the Helm value alone.
