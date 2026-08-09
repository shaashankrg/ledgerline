# Sabotage log

A record of deliberate, temporary breakages used to prove that a specific
test actually catches the failure it claims to. Each entry: what was broken,
what was predicted before running, what was actually observed, and what that
proves. Every entry here has been reverted; none of these breakages are
present in the current code.

---

## 2026-08-08 — Settlement simulator: suppressed NETWORK_DROPPED_ROW labelling

**Sabotage:** In `SettlementSimulator.applyFaults`, commented out the call to
`labelDropped(config, dropped, faults)` while leaving fault selection and
injection untouched. The dropped payment's settlement row was still omitted
from the CSV exactly as before -- the fault was still genuinely injected into
the file -- but no corresponding `faultlab.injected_faults` row was written
for it.

**Predicted:**
- `SettlementSimulatorTest.droppedRowFaultAppearsWhenItsRateIsNonZero` (the
  external-expectation test for this fault type) -- **red**. It sets the rate
  to 1.0 and asserts a `NETWORK_DROPPED_ROW` ground-truth row exists; with
  labelling suppressed, none does.
- `SettlementSimulatorTest.atMostOneNetworkFaultPerTransaction` (the closest
  thing to a self-consistency check in this suite -- it only inspects the
  ground-truth table's own internal structure, never the file) -- **green**.
  A fault that was never labelled cannot violate "no external_txn_id appears
  twice in the ground truth," because it does not appear in the ground truth
  at all.

**Observed:** exactly as predicted.

```
[ERROR] Tests run: 15, Failures: 1, Errors: 0, Skipped: 0
[ERROR] SettlementSimulatorTest.droppedRowFaultAppearsWhenItsRateIsNonZero -- FAILURE!
java.lang.AssertionError:
[NETWORK_DROPPED_ROW should appear when its rate is 1.0]
Expecting actual: 0L to be greater than: 0L
```

All other 14 tests, including `atMostOneNetworkFaultPerTransaction`, stayed
green.

**Proved:** the external-expectation test genuinely depends on the
ground-truth row existing -- it is not passing by coincidence or by checking
something the sabotage left untouched. It also demonstrates concretely why
Deliverable 5's test #3 has to be shaped as an *external* expectation ("the
rate is 1.0, therefore a row must exist") rather than a self-consistency
check: a self-consistency check has nothing to disagree with when a fault's
label is silently missing, because the row that would represent the
disagreement is the exact thing that got suppressed. Only a check imported
from outside the data -- the configured rate -- can notice its absence.

**Reverted:** the `labelDropped` call was restored immediately after this
observation; the full test suite (197 tests) was re-run green before moving
on.

---

*Note on this file's provenance: no `docs/` directory or sabotage ledger
existed in the repository before this entry. The Day 1 task spec assumed one
did ("in the same format as the existing entries") -- this is a deviation
from that assumption, recorded in the Day 1 final report.*
