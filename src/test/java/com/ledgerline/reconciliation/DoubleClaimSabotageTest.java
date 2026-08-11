package com.ledgerline.reconciliation;

import static org.assertj.core.api.Assertions.assertThat;

import java.math.BigDecimal;
import java.sql.Timestamp;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.UUID;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;

import com.ledgerline.AbstractPostgresTest;
import com.ledgerline.domain.EventType;
import com.ledgerline.domain.TransactionEvent;

/**
 * Day 3 sabotage 2, kept as a permanent probe rather than run by hand and
 * thrown away.
 *
 * The question it answers: with the partial unique index on
 * {@code (recon_run_id, matched_txn_id)} dropped, does the matcher's own
 * candidate-exclusion logic still prevent one payment being claimed by two
 * settlement lines -- or was the index the only thing holding it?
 *
 * Both answers are findings. "Application logic alone holds" means the index
 * is defence in depth; "only the index held" means the exclusion logic has a
 * gap and the constraint is load-bearing. This probe drops the index inside
 * the test, runs a batch built to provoke the double claim, records what
 * happened, and restores the index afterwards, so the sabotage is repeatable
 * on every build instead of being a one-off note in a report.
 *
 * Modelled on the existing sabotage probes this project already runs in CI
 * for the dual-write and commit-ordering properties.
 */
class DoubleClaimSabotageTest extends AbstractPostgresTest {

    private static final Instant CAPTURED_AT = Instant.parse("2026-01-15T06:00:00Z");
    private static final Instant SETTLED_AT = CAPTURED_AT.plus(Duration.ofHours(18));

    @Autowired
    private com.ledgerline.transfer.TransactionEventService eventService;

    @Autowired
    private ReconciliationService reconciliationService;

    @Autowired
    private ReconciliationAudit audit;

    @Autowired
    private JdbcTemplate jdbc;

    private long alice;
    private long merchant;

    @BeforeEach
    void setUp() {
        jdbc.update("DELETE FROM recon_line_outcomes");
        jdbc.update("DELETE FROM recon_exceptions");
        jdbc.update("DELETE FROM recon_runs");
        jdbc.update("DELETE FROM settlement_records");
        jdbc.update("DELETE FROM recon_batches");
        jdbc.update("DELETE FROM ledger_entries");
        jdbc.update("DELETE FROM transactions");
        jdbc.update("DELETE FROM transaction_states");
        jdbc.update("DELETE FROM parked_events");

        alice = jdbc.queryForObject("SELECT id FROM accounts WHERE name = 'Alice Checking'", Long.class);
        merchant = jdbc.queryForObject("SELECT id FROM accounts WHERE name = 'Merchant Revenue'", Long.class);
    }

    @AfterEach
    void restoreIndex() {
        jdbc.update("CREATE UNIQUE INDEX IF NOT EXISTS recon_line_outcomes_one_payment_per_run "
                + "ON recon_line_outcomes (recon_run_id, matched_txn_id) "
                + "WHERE matched_txn_id IS NOT NULL");
    }

    @Test
    void withoutTheUniqueIndexApplicationLogicAloneStillPreventsADoubleClaim() {
        jdbc.update("DROP INDEX IF EXISTS recon_line_outcomes_one_payment_per_run");

        String runId = "run-" + UUID.randomUUID();
        String batchId = "batch-" + UUID.randomUUID();

        captureAt(runId + "-txn-1", new BigDecimal("50.00"), "merch-1");

        jdbc.update(
                "INSERT INTO recon_batches (batch_id, run_id, seed, base_instant, row_count, file_sha256) "
                        + "VALUES (?, ?, 1, ?, 2, 'deadbeef')",
                batchId, runId, Timestamp.from(CAPTURED_AT));
        // Two id-less lines, both a perfect attribute fit for the single
        // payment. Without the index, only the matcher's `claimed` set stands
        // between this and a double claim.
        insertLine(batchId, 1, "merch-1", 5000);
        insertLine(batchId, 2, "merch-1", 5000);

        reconciliationService.run(batchId);

        long reconRunId = jdbc.queryForObject(
                "SELECT recon_run_id FROM recon_runs WHERE batch_id = ?", Long.class, batchId);

        List<String> claims = jdbc.queryForList(
                "SELECT matched_txn_id FROM recon_line_outcomes "
                        + "WHERE recon_run_id = ? AND matched_txn_id IS NOT NULL ORDER BY line_number",
                String.class, reconRunId);

        System.out.println("[DoubleClaimSabotageTest] index dropped; claims recorded = " + claims);

        // The finding: the exclusion logic in runFuzzyPass holds on its own.
        // The payment is added to `claimed` the moment line 1 takes it, and
        // line 2's candidate query excludes it, so the second claim is never
        // attempted -- the index never gets the chance to reject anything.
        // That makes the index defence in depth against a future refactor
        // that loses the exclusion, not the sole guarantee.
        assertThat(claims).containsExactly(runId + "-txn-1");

        // The audit's own double-claim check agrees, and would have caught it
        // had the logic not held.
        assertThat(audit.auditRun(reconRunId).problems()).isEmpty();
    }

    private void captureAt(String externalTxnId, BigDecimal amountMajor, String merchantId) {
        eventService.apply(new TransactionEvent(
                externalTxnId, externalTxnId + ":AUTHORIZE", EventType.AUTHORIZE,
                null, null, null, null, CAPTURED_AT, merchantId));
        eventService.apply(new TransactionEvent(
                externalTxnId, externalTxnId + ":CAPTURE", EventType.CAPTURE,
                alice, merchant, amountMajor, "USD", CAPTURED_AT, merchantId));
        jdbc.update("UPDATE transactions SET created_at = ? WHERE idempotency_key = ?",
                Timestamp.from(CAPTURED_AT), externalTxnId + ":CAPTURE");
    }

    private void insertLine(String batchId, int lineNumber, String merchantId, long grossAmountMinor) {
        jdbc.update(
                "INSERT INTO settlement_records "
                        + "(batch_id, line_number, external_txn_id, merchant_id, gross_amount_minor, "
                        + " fee_minor, currency, settled_at, raw_line) "
                        + "VALUES (?, ?, NULL, ?, ?, 0, 'USD', ?, 'raw')",
                batchId, lineNumber, merchantId, grossAmountMinor, Timestamp.from(SETTLED_AT));
    }
}
