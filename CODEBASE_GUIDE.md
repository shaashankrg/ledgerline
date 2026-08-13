# LedgerLine, explained end to end

This document walks the entire codebase in the order that makes it learnable:
concepts before code that uses them, foundations before features. If you read
it top to bottom, each section only depends on things you've already seen.

It assumes you know some Java and have heard of REST APIs, but nothing about
Spring, Kafka, or double-entry accounting. By the end you should be able to
read any file in this repo and understand not just *what* it does but *why*
it's shaped that way.

**How to use this alongside the code**: open the file being discussed in your
editor as you read that section. The line numbers and snippets here are meant
to orient you, not replace reading the real file — the comments in the source
are part of the explanation too.

---

## Table of contents

1. [The big idea: what is this system?](#1-the-big-idea-what-is-this-system)
2. [Why double-entry bookkeeping?](#2-why-double-entry-bookkeeping)
3. [The database schema](#3-the-database-schema)
4. [The domain layer — pure logic, no I/O](#4-the-domain-layer--pure-logic-no-io)
5. [The ledger layer — talking to Postgres](#5-the-ledger-layer--talking-to-postgres)
6. [The transfer layer — validation and orchestration](#6-the-transfer-layer--validation-and-orchestration)
7. [The messaging layer — Kafka producer and consumer](#7-the-messaging-layer--kafka-producer-and-consumer)
8. [The API layer — read-only HTTP endpoints](#8-the-api-layer--read-only-http-endpoints)
9. [The generator — synthetic load and fault injection](#9-the-generator--synthetic-load-and-fault-injection)
10. [The CLI — a one-shot demo tool](#10-the-cli--a-one-shot-demo-tool)
11. [Tests — how this codebase proves itself](#11-tests--how-this-codebase-proves-itself)
12. [Infrastructure: Docker, Maven, Makefile](#12-infrastructure-docker-maven-makefile)
13. [How a transaction actually flows through the system](#13-how-a-transaction-actually-flows-through-the-system)
14. [Glossary](#14-glossary)

---

## 1. The big idea: what is this system?

LedgerLine is a **payment ledger** — a system of record for money movements.
Think of it as the backend a payment processor would run: transactions arrive
as a sequence of lifecycle events (a card gets **authorized**, then
**captured**, then **settled**, and sometimes later **refunded**), and the
ledger has to record every movement of money correctly, exactly once, even
when the network is unreliable, processes crash mid-write, and the same
message gets delivered twice.

The system has three moving parts once fully assembled:

- **A producer** that publishes transaction events to a Kafka topic.
- **A consumer** that reads those events and writes them into a Postgres
  database as double-entry ledger records.
- **A generator** that can simulate realistic (and deliberately broken)
  traffic, for testing that the whole pipeline behaves correctly under
  failure.

There is no UI. This is a backend service, and the read surface is a small set
of HTTP endpoints for inspecting balances — mostly built for debugging while
building the rest.

**Why build it this way instead of a simple `UPDATE balance = balance + amount`
in Postgres?** Because a single mutable balance column can never tell you
*how* it got to its current value, cannot be audited, and offers no way to
detect when something has gone wrong. Every serious financial system in
production is built on the model this project uses — an **append-only
ledger of individual movements**, with the balance always *derived* by summing
them. You'll see this idea justified for real starting in the next section.

---

## 2. Why double-entry bookkeeping?

This is the single most important idea in the whole codebase, and it shapes
almost every design decision that follows. If you understand this section,
the rest of the code will make sense; if you skip it, a lot of the "why" comments
in the source will feel arbitrary.

### The core rule

Every transaction is recorded as **two or more entries that sum to exactly
zero.** If Alice pays Bob $50, the ledger doesn't write "Alice: -50". It
writes two rows:

```
Alice:  -50.00
Bob:    +50.00
```

These two rows are called a **balanced pair**, and they always belong to the
same **transaction**. The negative entry is a **debit**, the positive one is a
**credit** — money is never destroyed or created, only moved between two
named accounts.

### Why this matters — the invariant you can query for

Because every entry pair sums to zero, the *entire ledger*, no matter how many
millions of transactions it holds, must also sum to zero. This becomes a
single SQL query that either passes or catches a bug:

```sql
SELECT transaction_id, SUM(amount)
FROM ledger_entries
GROUP BY transaction_id
HAVING SUM(amount) != 0;
```

If this query *ever* returns a row, something in the system is broken — money
was created or destroyed by a bug. You'll see this exact query, or a close
variant, throughout the test suite (most directly in
[`LedgerInvariantTest`](src/test/java/com/ledgerline/ledger/LedgerInvariantTest.java)).
The whole point of the schema design is that correctness becomes something you
can *query for*, not something you have to trust the application code got
right.

### Why balances are never stored

You will not find a `balance` column anywhere in this schema. A balance is
always computed live:

```sql
SELECT COALESCE(SUM(amount), 0) FROM ledger_entries WHERE account_id = ?
```

A stored `balance` column is a **second source of truth** — a number that has
to be kept in sync with the entries by disciplined application code, and which
can silently drift from the truth if a bug or a race condition ever updates
one without the other. Deriving it live means there is only ever one place the
truth lives: the entries themselves. See
[`LedgerQueries.balanceOf`](src/main/java/com/ledgerline/ledger/LedgerQueries.java)
for exactly this query, and its class comment for the same reasoning spelled
out.

### Why `NUMERIC`, never `FLOAT` or `double`

Binary floating point cannot represent most decimal fractions exactly —
`0.1 + 0.2` famously does not equal `0.3` in a `double`. In a system whose
entire correctness claim is "the sum is exactly zero," a number type that
can't guarantee exact decimal arithmetic is disqualifying. Every amount in
this codebase, from the database column (`NUMERIC(19,4)`) through every Java
type (`BigDecimal`) to every JSON payload (a quoted **string**, never a bare
number), is chosen specifically to avoid ever passing money through a
`double`. You'll see this rule enforced repeatedly — search the codebase for
comments mentioning "never a double" or "JSON number" and you'll find the same
reasoning applied at each boundary (HTTP responses, Kafka messages, the
generator).

### The four amounts and what "correctness" means here

There's a subtlety worth naming up front, because it explains why this project
eventually needs a *reconciliation engine* (a piece not yet built, but the
schema is already shaped for it): in real payment processing, there are
**two independent views** of a transaction's amount — what your own system
captured, and what the outside settlement file later confirms was actually
moved. If your ledger derived its "settled" figure *from* the settlement file
by writing new entries, you'd only ever be comparing the settlement file
against itself. That's why [`EntryPolicy`](src/main/java/com/ledgerline/domain/EntryPolicy.java)
deliberately writes **no entries** for a `SETTLE` event — settlement is kept
as an external fact to check the ledger against, not a second way to write the
ledger. Keep this in mind; it explains a design choice that looks odd in
isolation.

---

## 3. The database schema

The schema is defined incrementally through **Flyway migrations** — numbered
SQL files in
[`src/main/resources/db/migration/`](src/main/resources/db/migration/) that
run in order, once each, and are never edited after being applied (that's the
"forward-only" rule you'll see mentioned in commit messages — once a migration
has run against a real database, changing it retroactively would desync
whatever already ran it from a fresh environment). Each file is small and
focused, and reading them in order is a good way to watch the data model grow
alongside the features that needed it.

### V1 — `init.sql`

A trivial baseline table, `app_metadata`, whose only purpose is to prove
Flyway itself is wired up correctly before anything real gets built on top of
it.

### V2 — `create_ledger_tables.sql`

The core schema. Three tables:

**`accounts`** — who can hold money. Each row has an `id`, a `name`, and an
`account_type` (a placeholder for real accounting categories like
asset/liability, not yet used for logic).

**`transactions`** — one row per *thing that happened*. Critically, this table
has **no amount column**. That's deliberate: a transaction row only claims
"this event occurred," and the actual money movement lives entirely in
`ledger_entries`. This closes off any shortcut where code could write a total
directly and skip the double-entry structure. The most important column here
is:

```sql
idempotency_key VARCHAR(100) NOT NULL UNIQUE
```

This is a client-supplied string that uniquely identifies *this specific
attempt* to write a transaction. If a network call times out and the caller
retries with the same key, Postgres's `UNIQUE` constraint rejects the
duplicate insert outright — the database enforces "don't double-write" as a
structural guarantee, not something application code has to remember to
check. You'll see this idea, called **idempotency**, come up constantly; it's
one of the two or three ideas this whole project is built to demonstrate
correctly.

**`ledger_entries`** — the actual movements. Each row has a `transaction_id`
(a foreign key — Postgres physically refuses to insert an entry pointing at a
transaction that doesn't exist), an `account_id`, and an `amount NUMERIC(19,4)`
with a `CHECK (amount <> 0)` constraint (a zero-amount entry is always a bug,
so the schema rejects it before it can even be written). Two indexes support
the two access patterns everything in this codebase needs: "give me every
entry for this transaction" and "give me every entry for this account."

### V3 — `seed_accounts.sql`

Six starter accounts (Alice, Bob, Carol, Merchant Revenue, Platform Fees,
Reserve Pool) so there's something realistic to write tests against.

### V4 — `add_payload_hash_and_currency.sql`

Two additions, both driven by real gaps found while building the write path:

- `transactions.payload_hash` — a SHA-256 hash of the *canonical* request
  body. The `idempotency_key` alone answers "have I seen this key before?" but
  not "was it used for the *same* request both times?" If a caller reuses a
  key for a materially different transfer, that's a bug on the caller's side,
  and the system needs to reject it rather than silently treating the second,
  different request as a harmless retry of the first. `payload_hash` is what
  makes that distinction possible — see
  [`EventPayloadHasher`](src/main/java/com/ledgerline/transfer/EventPayloadHasher.java)
  for how the hash is computed.
- `accounts.currency` — so the system can reject a transfer between accounts
  denominated in different currencies. Mixing currencies inside one balanced
  pair would make the numbers sum to zero while being financially
  meaningless.

### V5 — `transaction_states.sql`

This migration marks a real turning point in the project's design, worth
pausing on. Early versions of this system treated **one message as one
complete transfer** — a single event that fully described a movement of
money. But real payment processing doesn't work that way: a transaction is a
*sequence* of events (authorize, then capture, then settle, maybe later a
refund), arriving as separate messages, potentially far apart in time.

`transaction_states` holds **one row per transaction**, tracking which state
it currently sits in (`NEW`, `AUTHORIZED`, `CAPTURED`, `SETTLED`, `REFUNDED`,
`VOIDED`, `EXPIRED`). This is distinct from `transactions`, which is still the
per-*message* idempotency ledger — one transaction can now legitimately
produce several `transactions` rows over its life (one per event that writes
entries), but only ever one `transaction_states` row. Section 4 covers the
state machine that reads and writes this table in detail.

### V6 — `injected_faults.sql`

Supports the synthetic load generator (section 9). What's structurally
interesting here is *where* this table lives: it's created inside its own
Postgres schema, `faultlab`, not alongside the ledger tables in `public`. Read
the migration file's comment for the full reasoning, but the short version is:
this table is the **answer key** for a future fault-detection engine, and an
engine graded against an answer key it can freely read isn't being tested at
all. Putting it in a separate schema means a reconciliation query can't
resolve `injected_faults` without explicitly writing `faultlab.injected_faults`
— and a real deployment could revoke read access to that schema entirely for
whatever role runs reconciliation.

### V7 — `parked_events.sql`

Supports out-of-order event handling (section 6.4). Holds events that arrived
before they could legally be applied — e.g., a `CAPTURE` message that shows up
before its `AUTHORIZE` has been processed. These aren't errors; they're just
early. This table is where they wait until the event they were waiting for
arrives.

---

## 4. The domain layer — pure logic, no I/O

Package: [`com.ledgerline.domain`](src/main/java/com/ledgerline/domain/)

This is the most important package to understand deeply, because it contains
**zero database calls, zero Kafka calls, zero Spring annotations that do
anything at runtime.** Every class here is either a plain data record or a
pure function. That's a deliberate architectural choice: logic that doesn't
touch the outside world can be tested exhaustively, fast, and without any
infrastructure running. You'll see this pay off directly in
[`TransactionStateMachineTest`](src/test/java/com/ledgerline/domain/TransactionStateMachineTest.java),
which achieves **100% branch coverage** — every single decision path in the
state machine is proven correct by a test, something that's realistic to
demand of pure logic and unrealistic to demand of code that talks to a
database.

### 4.1 The building-block records

Four small immutable records model the domain vocabulary:

**[`TransactionEvent`](src/main/java/com/ledgerline/domain/TransactionEvent.java)**
— "one thing that happened." Carries an `externalTxnId` (identifies the
transaction across its *whole* lifecycle — every event for one payment shares
this value), an `eventId` (identifies *this specific event* — this is what
idempotency is keyed on, since five events legitimately share one
`externalTxnId`), an `EventType`, the two accounts involved, an amount, a
currency, and when it happened. Note the distinction between `externalTxnId`
and `eventId` carefully — conflating them is a bug that shows up more than
once in this project's history if you read the commit log.

**[`LedgerEntry`](src/main/java/com/ledgerline/domain/LedgerEntry.java)** — one
signed movement against one account. The constructor itself refuses to build
a zero-amount entry (`IllegalArgumentException`), matching the database's
`CHECK` constraint — the illegal state is rejected in two independent places,
which is exactly the kind of redundancy that catches bugs a single check
might miss.

**[`EntryGroup`](src/main/java/com/ledgerline/domain/EntryGroup.java)** — the
balanced set of entries produced by applying one event. This is where the
double-entry invariant gets enforced at the *type* level, not just by a later
test: the constructor sums every entry's amount and throws if it isn't
exactly zero. **You cannot construct an unbalanced `EntryGroup` in this
codebase** — the illegal state is unrepresentable. This is a stronger
guarantee than "we tested that it's always balanced"; it's "the type system
won't let you build a wrong one." An empty group (no entries at all) is
explicitly legal, because some events — like `SETTLE` — advance the
transaction's state without moving any money.

**`TransactionState`** and **`EventType`** are plain enums — the vocabulary
the state machine operates over. `TransactionState` has one small piece of
logic worth reading: `isTerminal()`. Notice `SETTLED` is *not* terminal — a
settled payment can still be refunded, which is the ordinary customer-return
path, not an exception. This was actually a real bug caught by a test during
development (a test asserted every state's terminality matched what the
transition table allowed, and the two initially disagreed) — a good example of
why redundant checks are worth writing even when they feel repetitive.

### 4.2 `TransactionStateMachine` — the lifecycle rules as data

This is the class to study most closely if you want to understand good API
design for "a set of rules that must be provably complete."

The naive way to write this would be a chain of `if/else` statements, or a
`switch` buried inside whatever service needs to check a transition. The
problem with that approach: **you cannot ask an `if/else` chain "have you
covered every case?"** — the only way to find out is to exercise every branch
and hope you thought of them all.

Instead, `TransactionStateMachine` stores the legal transitions as **data** —
a `Map<TransactionState, Map<EventType, TransactionState>>` built once in a
static initializer:

```java
table.put(TransactionState.AUTHORIZED, transitions(
        entry(EventType.CAPTURE, TransactionState.CAPTURED),
        entry(EventType.VOID, TransactionState.VOIDED),
        entry(EventType.EXPIRE, TransactionState.EXPIRED)));
```

Read this as: "from `AUTHORIZED`, a `CAPTURE` event moves you to `CAPTURED`, a
`VOID` moves you to `VOIDED`," and so on. Because this is a table rather than
control flow, [`TransactionStateMachineTest`](src/test/java/com/ledgerline/domain/TransactionStateMachineTest.java)
can do something a test of an `if/else` chain never could: it computes the
**complete cross product** of every state × every event type, checks that
each pair is classified as either legal (with an asserted destination) or
illegal (asserted to throw), and fails loudly if any pair is neither — see
`everyStateEventPairIsClassified()`. There is no way for a transition to be
silently untested.

Public methods worth knowing:

- `next(from, event)` — applies an event, throws `IllegalTransitionException`
  if it's not legal.
- `isLegal(from, event)` / `peek(from, event)` — non-throwing forms, used by
  callers that want to *decide* what to do rather than catch an exception.
- `legalEventsFrom(state)` — every event a given state currently accepts.
- `table()` — the whole table, returned as an unmodifiable copy, exposed
  purely so tests can enumerate it.

Trace through the actual rules once, since they encode real payment-processing
knowledge:

```
NEW        --AUTHORIZE--> AUTHORIZED
AUTHORIZED --CAPTURE-----> CAPTURED
AUTHORIZED --VOID--------> VOIDED
AUTHORIZED --EXPIRE------> EXPIRED
CAPTURED   --SETTLE------> SETTLED
CAPTURED   --REFUND------> REFUNDED
SETTLED    --REFUND------> REFUNDED
```

Notice `CAPTURED` does **not** accept `VOID` — voiding releases a *hold* that
never became a real movement; once money has actually moved (at capture), the
only way to correct it is a `REFUND`, which writes real reversing entries
rather than pretending nothing happened.

### 4.3 `EntryPolicy` — what each event costs

Deliberately a **separate class** from the state machine, because it answers
a different question. `TransactionStateMachine` asks "is this event allowed
here?" — a question about *ordering*. `EntryPolicy` asks "what does this event
cost, in ledger entries?" — a question about *accounting treatment*. Keeping
them apart means you can change the accounting rules (say, deciding `SETTLE`
should write something after all) without touching the lifecycle rules, and
vice versa.

The policy, spelled out in the class's own comment and worth internalizing:

| Event | Entries written |
|---|---|
| `AUTHORIZE` | none — a hold isn't a movement |
| `CAPTURE` | the forward pair (debit payer, credit payee) |
| `SETTLE` | none — see section 2's note on why |
| `REFUND` | the reverse pair (debit payee, credit payer) |
| `VOID` | none — nothing to reverse |
| `EXPIRE` | none — nothing to reverse |

`entriesFor(event)` is a good example of Java's newer `switch` expression
syntax — notice it's *exhaustive* over `EventType`; if a new event type were
added to the enum without a case here, the code wouldn't compile. That's the
compiler doing the same "did you cover every case?" job the state machine's
test does at runtime.

### 4.4 The exception types

Five exception classes model distinct failure reasons, all extending the
common base [`TransferException`](src/main/java/com/ledgerline/domain/TransferException.java):
`SameAccountTransferException`, `AccountNotFoundException`,
`CurrencyMismatchException`, `AmountScaleException`, and
`IdempotencyKeyReuseException`. Two more model failures specific to the state
machine and the concurrency-safety mechanism:
`IllegalTransitionException` and `TransitionConflictException`.

The distinction between those last two matters and is easy to conflate at
first glance:

- **`IllegalTransitionException`** — the event genuinely cannot follow the
  transaction's current state, and no amount of retrying changes that. This
  is a *permanent* failure.
- **`TransitionConflictException`** — another writer updated the transaction's
  state between when this caller read it and when it tried to write. This is
  a *transient*, timing-based failure — worth retrying, because the state
  really might have moved to something that makes this event legal (or the
  retry discovers it was already applied).

You'll see this distinction drive real routing decisions in the messaging
layer (section 7): one type goes to a dead-letter queue forever, the other
gets retried.

---

## 5. The ledger layer — talking to Postgres

Package: [`com.ledgerline.ledger`](src/main/java/com/ledgerline/ledger/)

Everything in the domain layer is pure logic with no side effects. This layer
is where that logic actually gets persisted — the boundary between "what
should happen" and "writing rows to a real database." All classes here use
Spring's `NamedParameterJdbcTemplate`, a thin, direct wrapper over JDBC (there
is no JPA/Hibernate anywhere in this project — every SQL statement is written
by hand, deliberately, so nothing about how a query executes is hidden behind
an ORM).

### 5.1 `LedgerWriter` — the only component that writes to `ledger_entries`

This is, by explicit design and by a test that verifies it (see section 11),
the **single place in the entire codebase** that inserts rows into
`transactions` or `ledger_entries`. That constraint is load-bearing: if two
different code paths could both write ledger rows, nothing would guarantee
they stay consistent with each other. Concentrating every write in one class
means every guarantee this class provides — atomicity, idempotency — applies
everywhere, automatically.

Key methods, in the order you'd naturally meet them:

- **`recordEntryGroup(transactionId, group)`** — the general-purpose writer.
  Takes an already-validated `EntryGroup` (recall: its constructor already
  proved it balances) and inserts each entry. No re-validation happens here,
  deliberately — re-checking balance would imply the type could have arrived
  unbalanced, which it structurally cannot.
- **`claimEvent(eventId, payloadHash, description)`** — the idempotency
  mechanism, and worth reading closely:

  ```java
  jdbc.query(
      "INSERT INTO transactions (idempotency_key, payload_hash, description) "
          + "VALUES (:eventId, :payloadHash, :description) "
          + "ON CONFLICT (idempotency_key) DO NOTHING "
          + "RETURNING id", ...)
  ```

  This is the single most important SQL pattern in the whole codebase, so
  it's worth understanding exactly why it's built this way rather than the
  seemingly simpler alternative:

  ```java
  // WRONG — do not do this
  if (!existsWithKey(eventId)) {
      insert(eventId, ...);
  }
  ```

  The "check, then insert" version has a **race condition**: if two threads
  (or two Kafka consumers) run this at the same moment, both can pass the
  `existsWithKey` check *before either has inserted*, and both proceed to
  insert — producing a duplicate. `INSERT ... ON CONFLICT DO NOTHING` closes
  that window entirely, because the check and the write happen as one atomic
  database operation; Postgres's unique index is the single source of truth
  about who "won," not a decision made in application code that can be
  outpaced by a second caller. `RETURNING id` yields no row when the conflict
  clause suppresses the insert — that absence of a row is precisely the
  signal "someone already claimed this."

  This exact pattern — and the exact bug of getting it wrong — recurs
  throughout this project's test suite as a **sabotage exercise**: several
  tests deliberately swap the correct `ON CONFLICT` version for the naive
  "check-then-insert" version and prove that concurrent duplicates slip
  through. If you want to see the race condition happen with your own eyes,
  read [`EventIdempotencyAndValidationTest`](src/test/java/com/ledgerline/transfer/EventIdempotencyAndValidationTest.java)'s
  concurrency test and the project's commit history around it.

- **`requireMatchingPayload(eventId, payloadHash)`** — called when
  `claimEvent` finds the key already taken. Compares the stored hash against
  this attempt's hash: if they match, this is a harmless retry (a *replay*).
  If they don't, the same key was reused for a materially different request,
  which is an error (`IdempotencyKeyReuseException`).
- **`releaseClaim(eventId)`** — used when an event was claimed but then
  determined to be *early* rather than applicable (see `TransactionEventService`
  in section 6) — the claim is given back so a later, real application of the
  event can take it properly.

### 5.2 `TransactionStateRepository` — the same idempotency pattern, applied to state

Reads and writes `transaction_states`. The interesting method is
`compareAndAdvance`:

```java
jdbc.update(
    "UPDATE transaction_states SET state = :next, version = version + 1, updated_at = now() "
        + "WHERE external_txn_id = :id AND state = :expected", ...)
```

This is the **same race-condition-closing idea** as `claimEvent`, applied to a
different problem: advancing a transaction's state. The `WHERE` clause names
the state the caller *believes* the transaction is currently in. If another
writer changed it in between, this `UPDATE` matches zero rows, and the caller
finds out immediately (via the return value) rather than silently overwriting
someone else's work. This pattern is called **compare-and-swap**, and you'll
see it named explicitly in the code comments. It's a "lock-free" way to make a
read-then-write sequence atomic — instead of locking the row while you decide
what to do, you just try the write and check whether it actually took effect.

### 5.3 `ParkedEventRepository` — holding events that arrived too early

Supports the "capture arrived before authorize" scenario described in
section 3's V7 discussion. Two methods worth noting for their shape:

- **`park(event)`** — inserts with `ON CONFLICT (event_id) DO NOTHING`, the
  same idempotency pattern again, this time preventing a redelivered early
  event from being parked twice.
- **`claimForDrain(parkedId)`** — another compare-and-swap: reserves a parked
  row for exactly one "draining" attempt, so that if a transaction's
  `AUTHORIZE` is somehow processed twice, the second attempt to drain its
  parked events finds nothing left to do.

Notice the file's own comment explaining *why* claiming and recording the
outcome are two separate steps rather than one — an earlier version of this
class combined them and had a real bug as a result (an abandoned event's
outcome silently failed to save, because the row no longer matched the
"still pending" condition by the time the abandon-write ran). Reading that
comment is a good lesson in how "make illegal states unrepresentable" applies
even to something as small as an UPDATE statement's WHERE clause.

### 5.4 `LedgerQueries` — everything the read API needs

The only *read*-focused class in this package (everything above is about
writing). Two capabilities:

- **`balanceOf(accountId)`** — the live `SUM(amount)` query discussed in
  section 2.
- **`entriesBefore(accountId, beforeCreatedAt, beforeId, limit)`** — powers
  paginated history browsing. This one is worth reading closely if you've
  never seen **keyset pagination** before (also called "cursor-based"
  pagination) — it's a materially better technique than the more familiar
  `LIMIT ... OFFSET`, and this codebase deliberately avoids `OFFSET`
  entirely. The reasoning, from the class comment: `OFFSET` counts rows from
  the start of the *current* result set, so if new rows are inserted between
  two page requests, everything shifts — a client can see the same row twice
  or skip one entirely. Keyset pagination instead says "give me everything
  strictly after this specific row" (compared on `(created_at, id)` — the
  `id` tiebreak matters because two entries of the same transfer can share an
  identical timestamp), which is stable no matter what gets inserted in
  between. See [`EntryCursor`](src/main/java/com/ledgerline/api/EntryCursor.java)
  in the API layer for how this position gets encoded into an opaque token a
  client passes back on the next request.

---

## 6. The transfer layer — validation and orchestration

Package: [`com.ledgerline.transfer`](src/main/java/com/ledgerline/transfer/)

If the domain layer is "the rules" and the ledger layer is "how to persist
things," this layer is "the orchestrator that applies the rules and calls the
persistence code in the right order, inside one atomic transaction." This is
the smallest package by file count but arguably the most important to
understand, because [`TransactionEventService`](src/main/java/com/ledgerline/transfer/TransactionEventService.java)
is where every other piece of this system actually meets.

### 6.1 `EventValidator` — checks that need live data

`validate()` runs four checks, in this order, and short-circuits entirely for
events that carry no money (`if (!event.movesMoney()) return;` — an
`AUTHORIZE` or `SETTLE` names no accounts, so there's nothing to check):

- Are `fromAccountId` and `toAccountId` different? This one is checked purely
  against the event itself, no database needed.
  <br>→ `SameAccountTransferException`
- Does the amount fit the ledger's scale (`NUMERIC(19,4)` — at most 4 decimal
  places)? Also pure — just inspects `amount.scale()`.
  <br>→ `AmountScaleException`
- Does each account actually **exist**? This one genuinely needs the
  database — a `SELECT` against `accounts`, catching
  `EmptyResultDataAccessException` (Spring's "no row found" signal) and
  translating it into the domain's own exception type.
  <br>→ `AccountNotFoundException`
- Does each account's stored `currency` match the event's declared currency
  (case-insensitively)?
  <br>→ `CurrencyMismatchException`

### 6.2 `EventPayloadHasher` — canonical hashing

Computes the SHA-256 hash used by `LedgerWriter.claimEvent` /
`requireMatchingPayload`. The word **canonical** is doing real work here: the
hash has to be identical for two requests that mean the same thing, even if
they're formatted differently — `"50.0"` and `"50.00"` should hash the same,
because they're the same amount. The hasher achieves this by normalizing the
amount to a fixed scale (`setScale(4)`) and building a fixed-order,
delimiter-separated string before hashing:

```java
externalTxnId + "|" + type + "|" + fromAccountId + "|" + toAccountId + "|" + amount + "|" + currency
```

Notice the `type` (the `EventType`) is included in the hash. This is
deliberate and worth pausing on: without it, a `CAPTURE` of $50 and a
`REFUND` of $50 for the same transaction would hash identically — two
*opposite* movements of money, indistinguishable by their hash. A test in
this codebase specifically checks that a capture and a refund of the same
amount are correctly treated as different events, not as a duplicate of each
other.

### 6.3 `TransactionEventService.apply()` — the orchestration, step by step

This is the method to read line by line if you want to understand how this
whole system fits together. It's the single entry point for "apply this
event to the ledger," and it does four distinct things, in this order, all
inside one `@Transactional` method (meaning: all of it commits together, or
none of it does — a crash halfway through leaves no partial trace):

**Step 1 — Validate.** `validator.validate(event)` runs first. If it throws,
nothing below has happened yet, so nothing needs to be undone.

**Step 2 — Claim the idempotency key.**
```java
Optional<Long> claimedRowId = ledgerWriter.claimEvent(event.eventId(), payloadHash, ...);
if (claimedRowId.isEmpty()) {
    ledgerWriter.requireMatchingPayload(event.eventId(), payloadHash);
    return new EventResult(currentStateOf(event), true);  // replayed = true
}
```
If the claim fails (key already used), this is either a harmless redelivery
(same payload — return normally, marked as a **replay**, having written
nothing new) or a genuine conflict (different payload — throw
`IdempotencyKeyReuseException`). This is exactly the two-question split
described for `LedgerWriter` in section 5.1.

**Step 3 — Gate and advance the state.**
```java
if (!TransactionStateMachine.isLegal(current, event.type())) {
    return parkOrReject(event, current, transactionIsNew);
}
TransactionState next = TransactionStateMachine.next(current, event.type());
if (!states.compareAndAdvance(event.externalTxnId(), current, next)) {
    throw new TransitionConflictException(event.externalTxnId(), current);
}
```
This is where the state machine from section 4.2 and the compare-and-swap
from section 5.2 meet. If the event isn't legal from the transaction's
current state, it's either parked (see 6.4) or permanently rejected. If it
*is* legal, the state advances — and if another writer beat this one to it,
`compareAndAdvance` returns `false` and the caller finds out via an
exception rather than silently clobbering the other writer's update.

**Step 4 — Write the entries, then drain anything waiting.**
```java
writeEntries(event, claimedRowId.get());
if (event.type() == EventType.AUTHORIZE) {
    drainParkedEvents(event.externalTxnId());
}
```
`writeEntries` asks `EntryPolicy` what this event costs and hands the result
to `LedgerWriter`. If this event was an `AUTHORIZE`, it might have just
unblocked events that arrived early and were waiting — see the next
subsection.

### 6.4 Parking: how out-of-order delivery is handled

This is one of the more subtle pieces of design in the codebase, so it's
worth walking through concretely.

**The problem**: Kafka only guarantees ordering *within a single partition*.
This system's topic has three partitions (see section 7 and
[`docker-compose.yml`](docker-compose.yml)), and messages are keyed by
`externalTxnId` — which means all events for *one* transaction land on the
same partition and stay ordered relative to each other... usually. But a
retried `AUTHORIZE` (say, the network dropped the first attempt) can still
arrive *after* its own `CAPTURE`, because the retry is a genuinely new
publish that races against everything already queued.

**The naive response** would be to treat an early `CAPTURE` as an error and
send it to the dead-letter topic (see section 7). But that's wrong — the
capture isn't broken, it's just early. Discarding it would silently lose a
real transfer.

**The actual response**, in `parkOrReject`:

```java
boolean early = current == TransactionStateMachine.initialState();  // i.e., state == NEW
if (!early) {
    throw new IllegalTransitionException(current, event.type());  // permanent — dead letter
}
ledgerWriter.releaseClaim(event.eventId());   // give back the idempotency claim — nothing was applied
parkedEvents.park(event);                      // store it, waiting
```

The distinction is exactly "is this transaction brand new (nothing has
happened to it yet, so this event has simply outrun its authorize) versus
does this transaction already have a *history* this event contradicts (e.g.,
a `CAPTURE` arriving against an already-`REFUNDED` transaction — nothing will
ever make that legal)." The first case is parked; the second is permanently
rejected.

When an `AUTHORIZE` *does* succeed, `drainParkedEvents` replays everything
waiting for that transaction, **oldest-first by when it actually happened**
(`occurred_at`), not by arrival order — because two out-of-order events
replayed in the wrong relative order would fail exactly the same way they did
the first time. Each parked row is claimed with the same compare-and-swap
pattern before being replayed, which is what makes draining safe even if two
consumers somehow process the same `AUTHORIZE` concurrently, or if the same
`AUTHORIZE` gets redelivered later — the second drain finds every row already
claimed and does nothing.

If a parked event is *still* illegal after being drained (its transaction's
real history moved on to something incompatible while it waited), it's
**abandoned** — marked with a reason in the `parked_events` table — rather
than left pending to be retried forever by every future event on that
transaction. Read `drainParkedEvents`'s comment for the full reasoning.

---

## 7. The messaging layer — Kafka producer and consumer

Package: [`com.ledgerline.messaging`](src/main/java/com/ledgerline/messaging/)

Before reading this section, it helps to know the absolute basics of Kafka:
a **topic** is a named stream of messages; a topic is split into
**partitions** for parallelism, and Kafka only guarantees message order
*within* one partition; messages are published with a **key**, and Kafka
routes all messages with the same key to the same partition (deterministically,
by hashing the key) — which is exactly how this system keeps every event for
one transaction in relative order. A **consumer group** is one or more
consumer processes sharing the work of reading a topic; Kafka assigns each
partition to exactly one consumer in the group at a time, and can reassign
partitions between consumers (a **rebalance**) if one joins or leaves.

### 7.1 `TransactionMessage` — the wire format

A record describing one event as it appears in a Kafka message: transaction
id, event id, event type, both accounts, amount, currency. The amount is
annotated to serialize as a JSON **string** (`PlainDecimalMessageSerializer`),
never a bare number — the same "never let money touch a double" rule from
section 2, applied at the Kafka boundary specifically. Most fields are also
serialized as strings for the same reason (a JSON number gets parsed as a
64-bit float by many client languages, silently losing precision on large
account ids too).

### 7.2 `KafkaProducerConfig` — how messages are published, and why every setting matters

Configures two Kafka producers (one for real transaction messages, one for
dead letters). The important settings, and *why* each one is set explicitly
rather than left at its default:

```java
config.put(ProducerConfig.ACKS_CONFIG, "all");
```
`acks=all` means the producer waits for confirmation from every in-sync
replica before considering a message "sent," not just the partition leader.
The alternative, `acks=1`, only waits for the leader — if that leader crashes
before the message replicates, the message is silently gone, but the producer
already told the caller it succeeded. For a financial transaction, a silently
lost message is unacceptable. Read the file's comment for the full argument;
it's a good example of choosing a setting for a reason you could defend in an
interview, not just because it's "safer."

```java
config.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);
```
Makes the *producer itself* deduplicate on retry — if a network blip causes
the client to resend a message the broker already received, the broker
recognizes and discards the duplicate at the Kafka protocol level. This is a
different, complementary idempotency mechanism from the application-level
`idempotency_key` in Postgres — one protects against duplicate *delivery*,
the other against duplicate *processing*.

```java
config.put(ProducerConfig.MAX_BLOCK_MS_CONFIG, 5_000);
```
Caps how long a publish call can block waiting for cluster metadata if the
broker is unreachable — worth noting because this setting was added after a
real bug: without it, an unreachable broker caused requests to hang for the
Kafka client's 60-second default, which was longer than any reasonable HTTP
timeout above it.

### 7.3 `KafkaConsumerConfig` — the two settings that make correctness possible

This is one of the most important files to understand precisely, because
getting these two settings wrong is the single most common way to either lose
or duplicate messages in any Kafka-based system.

```java
config.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
...
factory.getContainerProperties().setAckMode(AckMode.MANUAL_IMMEDIATE);
```

**Auto-commit**, if enabled, tells the Kafka client to periodically mark
messages as "processed" *on a timer*, regardless of whether your code
actually finished handling them. Imagine a message is read, your code starts
writing it to the database, and *before that write finishes*, the auto-commit
timer fires and tells Kafka "everything up to here is done." If the process
then crashes mid-write, Kafka believes that message was handled and will
never redeliver it — **the transaction is silently lost**, with no error
anywhere.

Manual commit with `MANUAL_IMMEDIATE` inverts this: the offset is only
committed *after* your code calls `acknowledge()`, and the consumer in this
codebase (see 7.5) only calls `acknowledge()` after the database write has
actually returned successfully. A crash before that point means the message
gets redelivered — which is safe here specifically *because* the whole system
is idempotent (section 5.1, 6.3). This combination — "deliver at least once,
but make processing idempotent so a duplicate delivery is harmless" — is a
named strategy in distributed systems, usually called **at-least-once
delivery with idempotent processing**, and it's the correctness model this
entire project is built around.

The other consumer settings worth knowing:

- `AUTO_OFFSET_RESET_CONFIG = "earliest"` — a brand-new consumer group reads
  the topic from the very beginning, rather than only seeing messages
  published after it first connects.
- `KEY_DESERIALIZER_CLASS_CONFIG` / `VALUE_DESERIALIZER_CLASS_CONFIG` are both
  `StringDeserializer`, not a typed JSON deserializer. This is deliberate: a
  typed deserializer that fails on a malformed message fails *inside the
  Kafka client itself*, before any of this project's code runs — which means
  there's no way to capture the broken bytes and route them to the
  dead-letter topic. Deserializing to a raw string first, then parsing by
  hand (see 7.4), keeps the original bytes available no matter how badly
  malformed the payload is.
- `setConcurrency(concurrency)` — how many consumer threads run in parallel
  within this one process, each behaving as a separate member of the
  consumer group. Relevant to the rebalancing tests in section 11.

### 7.4 `TransactionMessageParser` — turning bytes into domain events

Reads the raw JSON string, parses it into a `TransactionMessage`, and expands
it into one or more `TransactionEvent`s. The interesting wrinkle: a message
with no `eventType` set (the shape an older, simpler producer might still
send) is read as an **implicit two-event sequence** — an `AUTHORIZE`
immediately followed by its `CAPTURE` — rather than being rejected. This is
what lets the system stay compatible with a "one message = one complete
transfer" caller while still modeling transactions internally as full
lifecycles. Any failure to parse throws `PermanentMessageException` — a type
name chosen specifically to signal "retrying this will never help," which
matters for the routing decision in 7.5.

### 7.5 `TransactionConsumer` — where everything meets

The `@KafkaListener` method itself is intentionally tiny — read the class
comment, which explicitly states the listener should stay under about ten
lines, and explains why: this class's *only* job is to parse a message, hand
it to `TransactionEventService.applyAll()`, and acknowledge. All the actual
logic — validation, idempotency, state transitions, entry-writing — lives in
the transfer and ledger layers covered above. If you ever find yourself
wanting to add a business rule here, that's a sign it belongs one layer down.

The one piece of real logic in this class is the `try/catch` that decides
where a failure goes:

```java
try {
    List<TransactionEvent> events = parser.parse(record.value());
    EventResult result = eventService.applyAll(events);
    logOutcome(record, result);
} catch (PermanentMessageException | TransferException | IllegalTransitionException e) {
    deadLetter(record, e);
}
acknowledgment.acknowledge();
```

Read this carefully: **every path through this method acknowledges the
message.** That's correct even for the failure path, and it's worth
understanding exactly why. The three caught exception types are all
*permanent* failures — a malformed payload, a business rule violation, an
illegal state transition. Redelivering the same broken message will produce
exactly the same failure every time, so acknowledging it and routing it to
the dead-letter topic is what unblocks the partition for every message behind
it.

What's conspicuously **not** caught here: a database connection failure, or
`TransitionConflictException` (a timing race, not a permanent problem). Those
propagate up uncaught, `acknowledge()` is never reached, and the container's
error handler (configured in `KafkaConsumerConfig` with `FixedBackOff` and
`UNLIMITED_ATTEMPTS`) retries the message indefinitely. This is the correct
behavior for a transient failure — if the database is briefly down, blocking
that one partition until it recovers is better than either losing the message
or skipping ahead and leaving a gap.

Also note: a **parked** event (section 6.4) never reaches the `catch` block
at all — `applyAll` returns normally with `result.parked() == true`, and the
message is acknowledged like any success. That's correct: the event is safely
stored in `parked_events`, and redelivering it would only park it again,
achieving nothing.

### 7.6 `DeadLetterPublisher`

Publishes a failed message, byte-for-byte as it originally arrived, to
`transactions.DLT`, along with headers recording the failure reason and where
it came from (original topic, partition, offset). Forwarding the *original*
bytes rather than a re-encoded version matters — if the payload itself is
malformed, a "cleaned up" re-encoding would hide the very thing that broke
parsing in the first place.

---

## 8. The API layer — read-only HTTP endpoints

Package: [`com.ledgerline.api`](src/main/java/com/ledgerline/api/)

There is currently **no write endpoint** in this project — an earlier version
had `POST /api/v1/transfers`, but it was deliberately removed once the system
moved to the event-lifecycle model (section 3's V5 discussion), because
having two independent paths into the ledger (HTTP and Kafka) that had to stay
consistent with each other was judged not worth the risk. The only way to
write to the ledger now is by publishing to Kafka. What remains is a small
debugging surface for *reading* ledger state.

### 8.1 `AccountReadController`

Two endpoints:

- `GET /api/v1/accounts/{id}/balance` — calls `LedgerQueries.balanceOf`
  (section 5.4) and returns the live-summed balance plus the count of entries
  it was computed from.
- `GET /api/v1/accounts/{id}/entries` — paginated entry history, using the
  keyset pagination from section 5.4. Notice `clampLimit` — a request for more
  entries than the configured maximum is silently **clamped**, not rejected
  with an error, on the reasoning that a client asking for "as many as
  possible" is better served by getting the maximum than by being told to
  guess a smaller number.

An unknown account produces `AccountNotFoundInReadException`, mapped to a 404
— deliberately a *different* exception type from the domain layer's
`AccountNotFoundException` (which maps to 422 elsewhere), because the same
underlying fact ("this account doesn't exist") means something different
depending on context: on a write, it's "the request named something invalid";
on a read, it's "the resource you asked for isn't there," which is exactly
what HTTP 404 means.

### 8.2 `EntryCursor` — the opaque pagination token

Encodes a `(createdAt, id)` position as a base64 string. Clients treat this as
an opaque token — they're never meant to construct or interpret it themselves,
only pass back whatever the server gave them. `decode()` rejects anything that
isn't a token this server actually produced, via `MalformedCursorException`,
mapped to a 400 rather than crashing.

### 8.3 `ApiExceptionHandler` and `ErrorTypes`

A `@RestControllerAdvice` — Spring's mechanism for centralizing "if any
controller throws this exception, respond with this HTTP status and body,"
rather than repeating try/catch in every endpoint. Responses follow
**RFC 9457** ("Problem Details for HTTP APIs"), a standard JSON error shape
with a `type` field that's a stable URI (defined in `ErrorTypes`) a client can
switch on programmatically, rather than string-matching a human-readable
message that might change wording later.

Worth reading `handleUnexpected` closely — the catch-all for anything not
specifically handled. It deliberately returns **no exception detail** to the
caller, only a random correlation id, while logging the full exception
server-side against that same id. This is a real security practice: stack
traces and raw exception messages can leak internal implementation details
(table names, library versions, file paths) to anyone who can trigger an
error, which is valuable reconnaissance for an attacker and offers no benefit
to a legitimate caller.

### 8.4 `PlainDecimalSerializer`

The HTTP-layer sibling of `PlainDecimalMessageSerializer` from section 7.1 —
same job (force `BigDecimal` to serialize as a plain-notation JSON string,
never scientific notation, never a bare number), same reasoning, deliberately
a **separate class** rather than a shared one. The class comment explains
why: an HTTP response and a Kafka message are separate contracts with
separate consumers, and a change made for one shouldn't silently ripple into
the other just because they happened to share code.

---

## 9. The generator — synthetic load and fault injection

Package: [`com.ledgerline.generator`](src/main/java/com/ledgerline/generator/)

This package exists to answer a question none of the rest of the system can
answer on its own: **does the pipeline actually behave correctly under
realistic, messy traffic** — duplicates, out-of-order delivery, and outright
data corruption — **and can we prove it, repeatably?**

### 9.1 `FaultType`

Five kinds of defect the generator can inject, each modeling something that
genuinely happens in real payment pipelines (not invented anomalies):

- **`DUPLICATE_PUBLISH`** — the same event sent 2–5 times, simulating what
  at-least-once delivery and retries naturally produce.
- **`OUT_OF_ORDER`** — a capture published before its authorize.
- **`ORPHAN_CAPTURE`** — a capture whose authorize is never published at all.
- **`AMOUNT_DRIFT`** — the settled amount differs from the captured amount.
  This is the fault a future reconciliation engine exists specifically to
  catch (recall section 2's discussion of why `SETTLE` writes no ledger
  entries — this is exactly why: if it did, "drift" would be invisible,
  because the ledger would just believe whatever the settlement said).
- **`MISSING_SETTLEMENT`** — a transaction captures and simply never settles.

### 9.2 `GeneratorConfig` and determinism

A config record specifying a run: a seed, how many transactions to produce,
a target rate, and a fault rate (0.0–1.0) per fault type.

The single most important property of the whole generator is **determinism**:
the same seed and config must produce a *byte-identical* stream of published
messages, every time. Read `TransactionGenerator.planTransaction` closely to
see how this is achieved — one `java.util.Random`, seeded once at the start of
a run, and every random decision (which accounts, what amount, which faults
fire, how many duplicate copies) drawn from it **in a fixed order**, regardless
of which fault rates happen to be zero. This matters because a fault type
added to the enum later must not silently shift the sequence of draws for a
seed that was already recorded — otherwise a run from last month could never
be reproduced exactly once the code changes.

Why does reproducibility matter this much? Because the eventual purpose of
this generator is to measure a reconciliation engine's **accuracy** — what
fraction of injected faults it correctly detects. An accuracy number is
meaningless if you can't reproduce the exact stream it was measured against;
you'd have no way to compare two versions of a detection algorithm fairly, or
to debug a specific fault a detector missed.

### 9.3 `FaultLedger` — writing the answer key

Records every injected fault to `faultlab.injected_faults` (section 3's V6),
plus one row per run to `faultlab.generator_runs` recording the seed and
config — the durable record that makes a run reproducible after the fact.
Read `TransactionGenerator.generate()`'s loop to see the discipline this
requires: **every** fault decided for a transaction is recorded via
`faultLedger.record(fault)` *before* moving on, so the count of rows in
`injected_faults` always exactly matches the count of faults actually
published. A fault injected without a matching ground-truth row is worse than
no fault at all — grading would score a detector that correctly finds it as
having produced a *false positive*, when the fault was real.

---

## 10. The CLI — a one-shot demo tool

File: [`EmitTransactionCommand`](src/main/java/com/ledgerline/cli/EmitTransactionCommand.java)

A small Spring `ApplicationRunner` that publishes one transaction's full
lifecycle (by default: authorize, capture, settle) and then exits — useful for
manually verifying a change works end-to-end without running the whole
generator. It's only active under the `emit` Spring profile
(`application-emit.properties` disables the web server and the consumer for
this run, since a one-shot publisher has no use for either), and it explicitly
calls `SpringApplication.exit(context, () -> 0)` at the end — worth noting as
a small but real lesson: a Spring Boot app with a running Kafka listener or
web server will *not* exit on its own once your code finishes; something has
to tell it to shut down.

Run it via `make demo` — see section 12.

---

## 11. Tests — how this codebase proves itself

This project treats its test suite as load-bearing documentation of *why* the
code is shaped the way it is, not just a check that it currently works. A
recurring pattern throughout the test files (and something worth adopting in
your own work) is the **sabotage test**: deliberately break something the
code currently does correctly, confirm a specific test catches it with the
exact failure you'd expect, and then revert. This is how you prove a test
*actually* verifies the property it claims to, rather than passing
coincidentally.

A few tests worth reading specifically, because each teaches a distinct
lesson about testing correctness in a concurrent, database-backed system:

**[`LedgerInvariantTest`](src/test/java/com/ledgerline/ledger/LedgerInvariantTest.java)**
— asserts the double-entry invariant from section 2 after a variety of
transfers, using **soft assertions** (`SoftAssertions` from AssertJ) so that
if multiple checks fail, you see *all* of them at once rather than only the
first. Worth noting why: a per-account balance check is strictly *stronger*
than a sum-to-zero check (two entries landing on the *wrong* accounts can
still sum to zero), so if the weaker check happened to run first and pass, it
must never be allowed to hide a failure in the stronger one that runs after
it — soft assertions remove that ordering hazard entirely.

**[`TransactionStateMachineTest`](src/test/java/com/ledgerline/domain/TransactionStateMachineTest.java)**
— the exhaustive-coverage test described in section 4.2. Also contains
`everyStateIsReachable()`, a nice example of a *structural* test: it walks
the transition table from `NEW` and verifies every declared state can
actually be reached by *some* sequence of events — catching the case where a
state exists in the enum but the transition table has a typo that makes it
permanently unreachable.

**[`EventIdempotencyAndValidationTest`](src/test/java/com/ledgerline/transfer/EventIdempotencyAndValidationTest.java)**
— includes a genuine concurrency test using `CountDownLatch` to force two
threads to attempt the *exact same* event simultaneously, looped 20 times
(because a single attempt doesn't reliably trigger the race — you need many
tries to be confident the timing actually overlapped). This is the test that
proves `ON CONFLICT DO NOTHING` (section 5.1) actually closes the race
condition it claims to.

**[`OutOfOrderEventTest`](src/test/java/com/ledgerline/transfer/OutOfOrderEventTest.java)**
— exercises the parking mechanism from section 6.4: a capture that arrives
early gets parked and later applied once its authorize lands; an orphaned
capture (no authorize ever comes) stays parked forever without blocking
anything else; parked events drain in the order they *occurred*, not the
order they *arrived*.

**[`RebalanceAndOrderingTest`](src/test/java/com/ledgerline/messaging/RebalanceAndOrderingTest.java)**
— forces a real Kafka consumer group **rebalance** mid-batch (stopping one
consumer abnormally, not gracefully, so its unacknowledged work gets
redelivered to whichever consumer picks up its partitions) and confirms
nothing is lost or double-written. Also proves the ledger's correctness does
*not* depend on the order transfers are processed in — since a balance is
just a sum, and addition doesn't care what order you add numbers in.

**[`CrashRecoveryTest`](src/test/java/com/ledgerline/messaging/CrashRecoveryTest.java)**
— the most elaborate test in the project, and worth reading even if you never
run it (it's gated behind `-Dledgerline.crashtest=true` because it's slow: it
spawns the application as a **real, separate operating-system process** and
sends it an actual, unrecoverable kill signal — `Process.destroyForcibly()` —
mid-batch, specifically because an in-process `container.stop()` shuts down
gracefully and commits its Kafka offset cleanly on the way out, which proves
nothing about what happens when a process dies with no warning at all. The
test then restarts the consumer and checks that every transaction still ends
up with exactly one balanced pair of ledger entries — no losses, no
duplicates.

**Almost every "real" test in this codebase uses Testcontainers** — a library
that spins up an actual, disposable Postgres and Kafka broker inside Docker
for the duration of a test run — rather than mocking the database or the
message broker. You'll see this justified directly in several class comments:
the properties under test (does `ON CONFLICT` actually prevent a race
condition? does Kafka actually redeliver an unacknowledged message?) are
properties of the *real* database and the *real* broker's protocol, which a
mock cannot demonstrate by construction — a mock only ever does exactly what
you programmed it to do.

---

## 12. Infrastructure: Docker, Maven, Makefile

### 12.1 `docker-compose.yml`

Brings up two services locally: **Postgres 16** and **Kafka**, running in
**KRaft mode**. If you've used Kafka before and expect to see ZooKeeper —
KRaft is Kafka's newer mode where the brokers themselves manage cluster
metadata internally, removing the separate ZooKeeper dependency entirely
(this is why the compose file's `kafka` service has both
`KAFKA_PROCESS_ROLES: broker,controller` set on the one node).

A third service, `kafka-init`, runs once at startup specifically to create
the `transactions` topic (3 partitions — enough for meaningful parallelism
and rebalancing, see section 7) and the `transactions.DLT` dead-letter topic
(1 partition, since nothing consumes it automatically). It's set up this way,
rather than relying on Kafka's auto-topic-creation, so that a typo in a topic
name anywhere in the code fails loudly (the topic simply doesn't exist)
instead of silently creating a new, wrongly-named topic that messages vanish
into.

### 12.2 `pom.xml`

Standard Spring Boot Maven project. Two dependencies worth knowing the
purpose of beyond "the framework needs them":

- **Flyway** (`flyway-core`, `flyway-database-postgresql`) — runs the
  migrations from section 3 automatically on every application startup,
  applying only whatever hasn't run yet.
- **JaCoCo**, scoped specifically to `com/ledgerline/domain/**` — this is
  what produces the 100%-branch-coverage number mentioned in section 4. It's
  deliberately scoped only to the pure-logic domain package, because 100%
  coverage is a realistic and meaningful target for code with no I/O, and an
  unrealistic, gameable one for code that talks to a database or a network.

### 12.3 `Makefile`

Four targets: `up` / `down` (the Docker Compose stack), `build`, `test`, and
`demo` (runs `EmitTransactionCommand` from section 10). Read the comment on
`demo` — it clarifies that this target only *publishes* a transaction; a
separate consumer process needs to be running (e.g., a plain
`./mvnw spring-boot:run`) to actually pick the message up and write it to the
ledger, mirroring the real architecture where producing and consuming are
genuinely separate processes.

---

## 13. How a transaction actually flows through the system

Tying every previous section together, here's the full path a `CAPTURE`
event takes, start to finish, with the class responsible for each step:

1. **Something publishes a message** to the `transactions` Kafka topic —
   either `EmitTransactionCommand` (section 10), `TransactionGenerator`
   (section 9), or in principle any external producer. The message is a JSON
   `TransactionMessage` (section 7.1), keyed by `externalTxnId` so it lands
   on a deterministic partition.

2. **`TransactionConsumer.consume()`** (section 7.5) picks it up. It calls
   `TransactionMessageParser.parse()` (section 7.4), which turns the raw JSON
   into a `TransactionEvent` (section 4.1).

3. **`TransactionEventService.apply()`** (section 6.3) runs, inside one
   database transaction:
   - `EventValidator.validate()` (section 6.1) checks the accounts exist and
     agree on currency.
   - `LedgerWriter.claimEvent()` (section 5.1) tries to atomically claim this
     event's idempotency key. If it's already claimed with a matching
     payload hash, this is a replay — stop here, nothing more happens.
   - `TransactionStateMachine.isLegal()` (section 4.2) checks whether
     `CAPTURE` is legal from the transaction's current state. If not, and the
     transaction has no history yet, the event is **parked** (section 6.4)
     rather than rejected.
   - `TransactionStateRepository.compareAndAdvance()` (section 5.2) moves the
     transaction's state from `AUTHORIZED` to `CAPTURED`, atomically.
   - `EntryPolicy.entriesFor()` (section 4.3) computes the balanced pair of
     entries a capture produces.
   - `LedgerWriter.recordEntryGroup()` (section 5.1) writes those entries to
     `ledger_entries`.

4. If anything in step 3 throws a **permanent** failure (bad payload,
   business rule violation, illegal transition), `TransactionConsumer`
   catches it and calls `DeadLetterPublisher.publish()` (section 7.6) — the
   original message, byte-for-byte, goes to `transactions.DLT`.

5. Either way — success, replay, or dead-lettered — the consumer calls
   `acknowledgment.acknowledge()` (section 7.3), which commits the Kafka
   offset for this message. If the database write itself failed for a
   *transient* reason (connection lost), none of this happens, the exception
   propagates uncaught, and Kafka redelivers the message after a backoff.

6. Later, someone can inspect the result via
   `GET /api/v1/accounts/{id}/balance` (section 8.1), which recomputes the
   balance live by summing `ledger_entries` — never reading a cached number.

Every one of these steps is exercised by at least one test in section 11, and
several of them are exercised under deliberately adversarial conditions
(concurrent duplicates, out-of-order delivery, mid-write process kills) to
prove the guarantees actually hold, not just that the happy path works.

---

## 14. Glossary

**Idempotent / idempotency** — an operation that produces the same result no
matter how many times it's applied. In this codebase, specifically: retrying
a request with the same idempotency key never writes duplicate data.

**Double-entry bookkeeping** — an accounting method where every transaction is
recorded as balanced debits and credits that sum to zero. See section 2.

**Debit / credit** — a debit *decreases* an account (negative entry in this
schema); a credit *increases* it (positive entry). Money always moves from a
debit to a credit.

**Compare-and-swap (CAS)** — a lock-free way to update something atomically:
attempt a write conditioned on the current value being what you expect, and
find out immediately (via the write's result) whether it succeeded, rather
than locking first and updating second.

**At-least-once delivery** — a messaging guarantee that a message will be
delivered one or more times, never zero, but possibly more than once.
Requires idempotent processing downstream to be safe. (Compare: at-most-once,
where a message might be silently lost but is never duplicated —
significantly worse for a financial system.)

**Partition (Kafka)** — a topic is split into partitions for parallelism;
Kafka guarantees message order only within a single partition, never across
partitions of the same topic.

**Consumer group / rebalance** — a set of consumer processes sharing the work
of reading a topic; a rebalance is Kafka reassigning which consumer owns
which partition, which happens when a consumer joins, leaves, or is
considered dead.

**Dead-letter queue (DLQ / DLT)** — a separate topic or queue where messages
that can never be successfully processed are routed, so they don't block
everything behind them.

**Keyset pagination** — paginating by "give me everything after this specific
row," rather than "skip N rows" (`OFFSET`). Stable under concurrent inserts;
`OFFSET` is not. See section 5.4.

**RFC 9457 ("Problem Details")** — a standard shape for HTTP API error
responses, used throughout the API layer (section 8.3).

**Testcontainers** — a library used throughout this project's tests that
starts real, disposable Docker containers (Postgres, Kafka) for a test run,
rather than using mocks. See section 11.

**Sabotage test** — a testing technique used throughout this project: break
something on purpose, confirm the specific test that should catch it actually
fails with the expected error, then revert. Proves a test's claimed coverage
is real, not accidental.
