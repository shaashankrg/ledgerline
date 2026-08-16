# Single-command entry points into the real, current system: a kind cluster
# running the Helm-deployed chart (3-broker Kafka, 3 processor replicas,
# Postgres, Prometheus/Grafana), the same infrastructure every chaos test in
# this project runs against. Everything here assumes JAVA_HOME points at a
# JDK 21 (see mvnw's own error if it does not), and that Docker is running.
#
# `make demo` is the one target Day 11 explicitly verifies from a genuinely
# fresh clone (no leftover images, no already-running cluster) -- every
# target below is written to hold up under that same condition: no
# undocumented prerequisite a reader would have to already know about.

.PHONY: demo chaos recon bench sabotage down

IMAGE := ledgerline:dev
CLUSTER := ledgerline
NAMESPACE := ledgerline

# Builds the processor image, creates the kind cluster if it doesn't already
# exist, loads the image, and installs the Helm chart. A fresh clone has no
# cluster and no image, so this always does the full sequence; re-running it
# against an already-up cluster is intentionally still safe (kind create
# no-ops if the cluster exists; helm install fails loudly on a name
# collision rather than silently reinstalling over live data, which is the
# correct behavior -- `make down` first if a clean reinstall is what's
# wanted).
demo:
	@if ! kind get clusters 2>/dev/null | grep -qx "$(CLUSTER)"; then \
		echo "==> creating kind cluster $(CLUSTER)"; \
		kind create cluster --config k8s/kind-config.yaml --name $(CLUSTER); \
	else \
		echo "==> kind cluster $(CLUSTER) already exists, reusing it"; \
	fi
	@echo "==> building $(IMAGE)"
	docker build -t $(IMAGE) .
	@echo "==> loading $(IMAGE) into $(CLUSTER)"
	kind load docker-image $(IMAGE) --name $(CLUSTER)
	@echo "==> installing the chart"
	helm install ledgerline helm/ledgerline -n $(NAMESPACE) --create-namespace --timeout 5m
	@echo "==> waiting for the processor rollout"
	kubectl rollout status deployment/ledgerline-processor -n $(NAMESPACE) --timeout=180s
	@echo ""
	@echo "Ledgerline is up."
	@echo "  Grafana:    http://localhost:3000  (dashboard \"Ledgerline\", provisioned automatically)"
	@echo "  Prometheus: http://localhost:9090"
	@echo "  Processor:  http://localhost:8080/actuator/health"
	@echo ""
	@echo "The install-time generator Job already published a fault-injected"
	@echo "stream (see helm/ledgerline/values.yaml's generator.* settings) --"
	@echo "the dashboard has real traffic to show without any further steps."

# Runs the real 10-minute chaos test (ChaosInvariantTest) against the live
# cluster `make demo` stood up -- kills a random processor pod every 30-60s
# under sustained load, and asserts the ledger invariant, per-payment
# correctness, and the Day 10 torn-write signature never break. Requires
# `make demo` to have completed first; this target doesn't stand the
# cluster up itself, since re-running the chaos test against an
# already-running cluster (rather than tearing down and rebuilding every
# time) is the normal case.
chaos:
	./mvnw -q test -Dtest=ChaosInvariantTest#chaosRunHoldsTheInvariantContinuouslyAndLosesNothing -Dledgerline.chaostest=true

# Triggers the real reconciliation CronJob (helm/ledgerline/templates/jobs/
# recon-cronjob.yaml) immediately, the same job that otherwise only runs on
# its own schedule (recon.schedule in values.yaml). Not a separate local
# process against port-forwarded services -- this runs the actual mechanism
# a fresh clone's reader would see in production, which is a real Kubernetes
# Job spawned from the CronJob's own PodTemplate.
recon:
	kubectl create job -n $(NAMESPACE) --from=cronjob/ledgerline-recon manual-recon-run-$(shell date +%s)

# Runs LoadRampCommand (Day 6's load ramp) against the live cluster, so the
# real processor Deployment's consumers -- not an ephemeral local one -- are
# what a Grafana dashboard watched during the run would show degrading.
# ledgerline.consumer.enabled=false keeps this one-shot publisher JVM from
# starting its own competing consumer in the same group, which would split
# partitions with the replicas actually being measured. Connects through
# the same host-reachable EXTERNAL Kafka listener every chaos test uses;
# override RATES/STEP to change the ramp, e.g. `make bench RATES=100,200 STEP=60`.
RATES ?= 50,100,200,400,800
STEP ?= 30

bench:
	./mvnw -q spring-boot:run \
		-Dspring-boot.run.profiles=load \
		-Dspring-boot.run.arguments="--rates=$(RATES) --seconds-per-step=$(STEP)" \
		-Dspring-boot.run.jvmArguments="-Dspring.kafka.bootstrap-servers=localhost:9094"

# Runs MinInsyncReplicasSabotageTest against the live cluster: kills 2 of
# the 3 Kafka broker pods (violating min.insync.replicas=2 with only 1
# broker left) and asserts a real publish attempt fails loudly rather than
# silently succeeding on a write the cluster cannot make durable. See
# docs/day10-multi-broker.md for the full result and reasoning.
#
# WARNING: this test does NOT restore the killed brokers afterward (see
# the test class's own Javadoc) -- recovering a 3-broker KRaft quorum after
# losing 2 of 3 controllers is a cluster-recovery operation, not a quick
# undo. Run this last, or run `make down` and `make demo` again afterward
# to get a clean cluster back.
sabotage:
	./mvnw -q test -Dtest=MinInsyncReplicasSabotageTest -Dledgerline.chaostest=true

# Tears down the kind cluster entirely -- the clean-slate counterpart to
# `make demo`, for when a fresh install (rather than reusing whatever state
# a prior `make demo`/`make sabotage` left behind) is what's wanted.
down:
	kind delete cluster --name $(CLUSTER)
