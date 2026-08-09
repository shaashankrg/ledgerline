# Incident: Schema drift against the shared dev database

**Date:** 2026-08-08

## What happened

While iterating on migration `V8__settlement_simulator.sql` during Day 1
development, the migration file was edited twice mid-session (once to fix a
`DROP DEFAULT` that broke every `FaultLedger` insert, per the review that
produced this incident's own root-cause fix). Each edit changed the file's
Flyway checksum after it had already been applied once to the long-lived
Postgres instance started by `docker-compose` (`ledgerline-postgres`,
`localhost:5432`).

Running the full test suite afterward failed with:

```
org.flywaydb.core.api.exception.FlywayValidateException: Validate failed:
Migrations have failed validation
```

Manual cleanup attempts (deleting the `V8` row from `flyway_schema_history`,
re-running) left the schema in a state that didn't match any consistent
Flyway history: `recon_batches` and `settlement_records` existed as tables,
but `faultlab.injected_faults.source` and the `recon_role` role were absent,
and `flyway_schema_history` had no `V8` row at all -- a partially-applied
migration with no record of having run.

## How it was noticed

`TransactionProducerTest` began failing with `ApplicationContext` load
errors during a full-suite run, even though its own four tests have nothing
to do with the settlement schema. The stack trace bottomed out in
`FlywayValidateException` / `column "source" ... already exists` /
`relation "recon_batches" already exists` on different runs, depending on
exactly what state the shared database happened to be in at that moment.

## Forensics and cleanup

1. Inspected `flyway_schema_history` directly via `psql` inside the
   `ledgerline-postgres` container to see the actual applied checksums.
2. Confirmed no Flyway Maven plugin was configured (migrations run only via
   Spring Boot autoconfiguration at application startup), so `flyway:repair`
   wasn't available as a one-command fix.
3. Manually reverted every `V8`-introduced object (`DROP TABLE
   settlement_records, recon_batches`; dropped the `source` column and its
   check constraint; `REASSIGN OWNED BY recon_role` then `DROP ROLE`) and
   deleted the `V8` row from `flyway_schema_history`, restoring the schema to
   its exact pre-`V8` state.
4. Ran `TransactionProducerTest` alone to confirm `V8` re-applied cleanly
   from a known-good baseline before trusting the full suite again.

## Root cause

One test class -- `TransactionProducerTest` -- booted its Spring context
against `localhost:5432`, the docker-compose Postgres, instead of an
ephemeral Testcontainers instance like every other test class in the suite.
Its own comment explained why: *"the producer touches no database, so
starting Postgres for it would only slow the run."* That reasoning doesn't
hold under `@SpringBootTest`, though -- the full application context boots
regardless of what the test itself exercises, so Postgres and Flyway get
touched either way. The class ended up depending on Postgres implicitly,
via whatever `application.properties` defaults to, rather than explicitly
provisioning its own.

A database that survives between runs accumulates state between them. A
migration edited mid-development against a database that had already applied
an earlier version of that same migration is exactly the scenario Flyway's
checksum validation exists to catch -- and it caught it, correctly. The bug
was using a long-lived database for iteration in the first place, not the
validation that flagged it.

## What it generalizes to

**A green test against a mutable shared database is not evidence.** It can
pass because of state a previous run happened to leave behind, and fail
because of a half-applied migration a completely unrelated change triggered
-- in this incident, `TransactionProducerTest`'s Kafka-only tests failed
because of a settlement-schema migration they never reference. This is the
same failure class as a test that passes while asserting against a dead code
path: the green checkmark is real, but it isn't proof of the property the
test claims to verify. Every other test class in this suite avoids the
problem by construction, by starting its own disposable Postgres per JVM;
this incident is what happens when one class quietly opts out of that
guarantee for a performance reasoning that turned out not to apply.

## Fix applied

`TransactionProducerTest` now starts its own ephemeral `PostgreSQLContainer`
via `@DynamicPropertySource`, the same pattern `TransactionGeneratorTest`
already uses for Kafka + Postgres together. See
`src/test/java/com/ledgerline/messaging/TransactionProducerTest.java`.
