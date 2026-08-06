# Convenience targets over the Maven build and the local compose stack.
# Every target assumes JAVA_HOME points at a JDK 21; see mvnw's own error if
# it does not.

.PHONY: up down build test demo

# Brings up Postgres and Kafka. Run once before test or demo on a machine
# that has not started them yet.
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
demo: build
	./mvnw -q spring-boot:run \
		-Dspring-boot.run.profiles=emit \
		-Dspring-boot.run.arguments="--from=$(or $(FROM),1) --to=$(or $(TO),2) --amount=$(or $(AMOUNT),50.00) --currency=$(or $(CURRENCY),USD) $(if $(TXN),--txn=$(TXN),) $(if $(EVENTS),--events=$(EVENTS),)"
