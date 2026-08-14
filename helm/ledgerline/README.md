# Ledgerline Helm chart

`helm install ledgerline helm/ledgerline --namespace ledgerline --create-namespace`
produces a fully working system with no manual steps beyond the install
command itself: Postgres and Kafka come up, migrations run exactly once,
topics are created, three processor replicas start consuming, a generator
Job publishes a deterministic fault-injected stream, and Prometheus/Grafana
are live with the dashboard already provisioned.

## values.yaml controls the whole demo

Replica count, generation rate, seed, fault rates, and the reconciliation
window are all set in `values.yaml` -- nobody should need to hand-edit a
rendered template to change how the demo runs. See that file for every
tunable and why each default was chosen.

## The Flyway migration race, and why it's structurally closed rather than merely locked against

**Why the race exists at all.** `spring.flyway.enabled=true` runs Flyway
migrations synchronously during Spring context startup, before the
application is otherwise ready. A Kubernetes Deployment with
`replicas: 3` gives no ordering guarantee between pods -- all three can
reach that startup step within the same second, each opening its own
database connection and each independently deciding "the schema history
table doesn't have this migration yet, I should apply it."

**Why depending on Flyway's own lock is a more fragile answer than it looks.**
Flyway does have an advisory lock that prevents concurrent migration runs
from corrupting the schema -- *when it works as intended*. But relying on
that as the only defense means: three processes contend for one lock, two
of them block on a live database connection doing nothing useful while the
winner runs (extra, avoidable startup latency multiplied by replica count),
and the actual safety of the whole arrangement depends on an assumption
about a dependency's internal locking behavior continuing to hold, forever,
under exactly this concurrency pattern -- an assumption this chart would
otherwise be making silently, with nothing anywhere stating it out loud.

**What this chart does instead.** `templates/jobs/migrate-job.yaml` is a
`pre-install,pre-upgrade` Helm hook that runs the same application image
with `spring.profiles.active=migrate` (see
`application-migrate.properties` and `com.ledgerline.cli.MigrateCommand`)
and `spring.flyway.enabled=true`. It runs to completion -- or the release
fails outright -- *before Helm creates the processor Deployment at all*.
The processor's own pods run with `SPRING_FLYWAY_ENABLED=false`
(`templates/processor/deployment.yaml`) and never attempt a migration
themselves. The distinction that matters: this is not "the lock arbitrated
a race that happened, and happened to arbitrate it correctly" -- it's "no
race could happen," because by the time any processor pod's container is
even created, exactly one process has ever attempted a migration, ever, for
this release. The first version degrades gracefully only as long as an
assumption about Flyway's internals keeps holding across releases; the
second doesn't depend on that assumption existing in the first place.

**Hook ordering.** Postgres and Kafka are themselves `pre-install` hooks
(weights `-3` and `-2`) so they exist before anything tries to connect to
them. `migrate` runs at weight `0`, `kafka-init` (topic creation) at
weight `1` -- both must complete before the processor Deployment, a normal
(non-hook) resource, is created. The generator Job is a `post-install`
hook: it publishes real traffic, which only makes sense once the processor
exists to consume it.

Postgres and Kafka are deliberately `pre-install`-only (not `pre-upgrade`):
they carry PVC-backed state that must survive a `helm upgrade`, not be torn
down and recreated by hook-delete-policy on every release. This does mean a
`values.yaml` change to `postgres.storage` or `kafkaStorage` after first
install requires a manual step (this chart does not attempt an in-place PVC
resize) -- an accepted tradeoff for a demo-scale chart, stated here rather
than left for someone to discover.

## Readiness: driven by a real Kafka rebalance, not a proxy signal

See `com.ledgerline.messaging.KafkaConsumerGroupHealth` for the mechanism
and `KafkaConsumerGroupHealthRebalanceTest` for the sabotage test proving it
actually flips false during a real rebalance and true again after. Scaling
`ledgerline-processor` past the topic's partition count (3 by default) will
always leave at least one replica with zero partitions and therefore
`readinessState: DOWN` -- this is correct behavior, not a bug, and is
directly observable:

```
kubectl scale deployment ledgerline-processor -n ledgerline --replicas=4
kubectl get pods -n ledgerline -l app=ledgerline-processor  # one shows 0/1
kubectl scale deployment ledgerline-processor -n ledgerline --replicas=3
kubectl get pods -n ledgerline -l app=ledgerline-processor  # all 1/1 again
```
