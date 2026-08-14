# Convenience targets over the Maven build and the local compose stack.
# Every target assumes JAVA_HOME points at a JDK 21; see mvnw's own error if
# it does not.

.PHONY: up down build test demo dashboard

# Brings up Postgres, Kafka, Prometheus, and Grafana. Run once before test or
# demo on a machine that has not started them yet. The app itself runs on the
# host, not in this stack (see `demo` below), and Prometheus scrapes it at
# host.docker.internal:8080 -- see observability/prometheus/prometheus.yml.
up:
	docker compose up -d

down:
	docker compose down

build:
	./mvnw -q package -DskipTests

test:
	./mvnw test

# Emits one transaction's lifecycle to the transactions topic and exits --
# does not require the load generator. Requires `make up` first, and a
# consumer running separately (`./mvnw spring-boot:run`, no profile) to
# actually write it to the ledger: this target only publishes, the same
# separation the pipeline itself has since the intake endpoint was retired.
#
# Defaults to a plain transfer (accounts 1 and 2, $50.00, AUTHORIZE ->
# CAPTURE -> SETTLE). Override any of TXN, FROM, TO, AMOUNT, CURRENCY,
# EVENTS, e.g.:
#   make demo EVENTS=AUTHORIZE,CAPTURE,REFUND AMOUNT=12.34
#
# The dashboard populates from whatever traffic the running consumer has
# actually processed -- this target alone (one message) will show real but
# sparse panels. Run the load generator (or several `make demo` calls)
# against a running consumer first for a dashboard worth looking at, then
# open http://localhost:3000 -- the Ledgerline dashboard is provisioned
# automatically, no manual import needed.
demo: build
	./mvnw -q spring-boot:run \
		-Dspring-boot.run.profiles=emit \
		-Dspring-boot.run.arguments="--from=$(or $(FROM),1) --to=$(or $(TO),2) --amount=$(or $(AMOUNT),50.00) --currency=$(or $(CURRENCY),USD) $(if $(TXN),--txn=$(TXN),) $(if $(EVENTS),--events=$(EVENTS),)"

# Opens the provisioned dashboard directly, once `make up` has Grafana running.
dashboard:
	@echo "Ledgerline dashboard: http://localhost:3000/d/ledgerline-core"
