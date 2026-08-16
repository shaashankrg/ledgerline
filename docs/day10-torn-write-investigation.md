# Day 10: torn ledger write -- investigated, repaired, unresolved root cause

## What was found

During the first real-cluster leader-kill runs (`LeaderKillDurabilityTest`,
`acks=all`), one transaction's balance invariant assertion failed:
`SELECT SUM(amount) FROM ledger_entries` returned `-247.2750` instead of
`0`, and stayed that way on recheck minutes later -- ruling out the kind of
transient concurrent-read timing artifact Day 9 had already diagnosed and
fixed (see `docs/day9-chaos-test.md`).

Root query:

```sql
SELECT e.transaction_id, count(*), SUM(e.amount), t.idempotency_key
FROM ledger_entries e JOIN transactions t ON t.id = e.transaction_id
GROUP BY e.transaction_id, t.idempotency_key
HAVING count(*) <> 2 OR SUM(e.amount) <> 0;
```

Found exactly one offending row: transaction 28038
(`leaderkill-1786843219682-txn-2157:CAPTURE`), with a single ledger entry
(`account_id=2, amount=-247.2750`) instead of the balanced debit/credit
pair every CAPTURE should produce.

## Why this should be structurally impossible

- `EntryPolicy.movement()` (the only path that builds a CAPTURE's entries)
  unconditionally constructs `List.of(debit, credit)` -- there is no branch
  that returns fewer than 2 entries for a CAPTURE.
- `EntryGroup`'s compact constructor sums every entry and throws on any
  nonzero total, so a genuinely unbalanced or single-entry group cannot even
  be constructed.
- `LedgerWriter.recordEntryGroup` (`@Transactional`) loops over exactly the
  2-element list `EntryGroup.entries()` returns, calling `insertEntry` for
  each -- ordinary Spring transaction semantics make this atomic: both
  inserts commit together or neither does.
- No fault injection was in play: `LeaderKillDurabilityTest` constructs its
  `GeneratorConfig` with an empty `EnumMap<FaultType, Double>`, and
  `GeneratorConfig.rateOf` (`getOrDefault(type, 0.0)`) confirms that means
  zero rate for every fault type, not a default.
- No redelivery: the transaction's full AUTHORIZE/CAPTURE/SETTLE sequence
  each claimed a distinct idempotency key exactly once (confirmed via the
  `transactions` table), so this was not a retried or duplicated event.
- No bypass: `grep`ing the entire `src` tree for `INSERT INTO
  ledger_entries` finds exactly one production call site
  (`LedgerWriter.insertEntry`), called only from `recordEntries`/
  `recordEntryGroup`. No `REQUIRES_NEW` propagation, no manual transaction
  management, no second `PlatformTransactionManager` touches this table (the
  recon role's datasource is read-only against `ledger_entries`).
- No sequence gap: `ledger_entries.id` is a plain sequence, and the ids
  immediately before and after the orphaned row (18662, 18664) belong to
  other, unrelated, fully-balanced transactions with no missing value in
  between. This rules out "the second insert was attempted and rolled
  back" -- it means the second `insertEntry` call was **never issued at
  all**, despite `group.entries()` being an immutable 2-element list at the
  point the loop started.

## What was ruled out directly, with live evidence

Suspecting a HikariCP connection swap mid-transaction (the pool handing a
different physical connection to the second `insertEntry` call after the
`PSQLException: Connection reset` errors repeatedly observed elsewhere in
this session's chaos runs), temporary diagnostic logging was added to
`LedgerWriter.recordEntryGroup`: at transaction start and after every
insert, it logged `DataSourceUtils.getConnection(dataSource)`'s identity
hash and whether it matched the transaction's starting connection.

Re-running `LeaderKillDurabilityTest` (`acks=all`, leader killed under
load) with this logging active produced:

- 1982 `recordEntryGroup` calls, every one logging `entries=2`.
- Exactly 3964 "after insertEntry" lines (1982 x 2) -- a perfectly clean
  1-start-then-2-inserts pattern with zero truncated groups.
- **Zero** `sameAsStart=false` occurrences across every line logged.
- No torn write reproduced in this run (confirmed both from the DIAG logs
  and directly querying the database for any new unbalanced transaction).
- Postgres itself has 0 restarts since this cluster was created, ruling out
  a crash/WAL-recovery explanation for lost data.

This directly disproves the connection-swap theory for this attempt. The
original incident was not reproduced on demand.

## Disposition

- The offending row (transaction 28038) was repaired by hand: the original
  CAPTURE event was recovered from the still-retained Kafka topic itself
  (`kafka-console-consumer --from-beginning`), confirming
  `fromAccountId=2, toAccountId=1, amount=247.2750` -- exactly matching the
  surviving debit entry. The missing credit
  (`account_id=1, amount=+247.2750`) was inserted directly, restoring
  `SUM(amount) = 0` cluster-wide. This is the one and only manual write
  against `ledger_entries` in this project's history, done with the
  original event payload in hand rather than guessed.
- The diagnostic logging was removed; `LedgerWriter.java` is back to its
  pre-investigation state.
- **Root cause remains unconfirmed.** Every application-level, fault-
  injection, redelivery, bypass, and (directly, with live evidence)
  connection-swap explanation has been ruled out. One occurrence in
  30,000+ ledger entries written across this session's chaos runs, not
  reproduced under a dedicated repro attempt with instrumentation active.
  The remaining plausible explanations are lower in this project's own
  stack than application code reaches: a narrower race in the JDBC
  driver/HikariCP interaction during the specific network churn a broker
  leader election causes, or something in the container/network layer of
  the local `kind` cluster itself. Pinning it further would need
  JDBC-level fault injection (e.g. a proxy that can kill the physical
  socket mid-statement on demand) rather than more passive logging.

## Follow-up (not done here)

- Deliberate JDBC-layer fault injection (a toxiproxy-style TCP proxy in
  front of Postgres that can sever the connection at a chosen byte offset)
  to try to reproduce this on demand rather than waiting for it to recur
  under real chaos conditions.
- If reproduced, instrument at the HikariCP pool level (borrow/return
  events, physical connection lifecycle) rather than only at the
  application's `DataSourceUtils` view of the transaction-bound connection.
