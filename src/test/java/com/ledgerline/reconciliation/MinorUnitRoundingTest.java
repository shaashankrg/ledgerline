package com.ledgerline.reconciliation;

import static org.assertj.core.api.Assertions.assertThat;

import java.math.BigDecimal;
import java.sql.Timestamp;
import java.time.Duration;
import java.time.Instant;
import java.util.UUID;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;

import com.ledgerline.AbstractPostgresTest;
import com.ledgerline.domain.EventType;
import com.ledgerline.domain.TransactionEvent;

/**
 * Pins the sub-cent rounding boundary shared by two code paths that must
 * agree.
 *
 * {@code ledger_entries.amount} is {@code NUMERIC(19,4)} and
 * {@code EventValidator} rejects only amounts with scale greater than 4 --
 * so scale 3 and 4 are accepted, and a value like {@code 10.005} reaches the
 * ledger intact. Sub-cent amounts are therefore reachable, not hypothetical,
 * and the rounding in both conversions is live code rather than a defensive
 * no-op.
 *
 * Two independent implementations convert those amounts to the minor units a
 * settlement file reports:
 *
 *   - {@code ReconciliationService.toMinorUnits}, in Java:
 *     {@code movePointRight(2).setScale(0, HALF_UP)}
 *   - {@code CapturedLedgerView.fuzzyCandidatesFor}, in SQL:
 *     {@code ROUND(e.amount * 100)::bigint}
 *
 * If they ever disagree, a payment becomes simultaneously an
 * {@code AMOUNT_MISMATCH} by the Java path and a fuzzy candidate by the SQL
 * path, or vice versa -- a contradiction the engine has no way to notice and
 * that Day 4 would score as a matcher defect. Postgres's ROUND on NUMERIC is
 * half-up away from zero, which matches BigDecimal.HALF_UP for the positive
 * amounts a credit entry carries; this test is what holds that equivalence
 * to account at the exact boundary where the two rules could diverge.
 */
class MinorUnitRoundingTest extends AbstractPostgresTest {

    private static final Instant CAPTURED_AT = Instant.parse("2026-01-15T06:00:00Z");
    private static final Instant SETTLED_AT = CAPTURED_AT.plus(Duration.ofHours(18));

    @Autowired
    private com.ledgerline.transfer.TransactionEventService eventService;

    @Autowired
    private ReconciliationService reconciliationService;

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

    /**
     * Exactly half a cent, the value where HALF_UP and half-to-even disagree:
     * 10.005 -> 1001 half-up, 1000 half-to-even. A plain {@code ::bigint}
     * cast in the SQL path would round half-to-even and produce 1000, which
     * is why the query uses ROUND explicitly.
     */
    @Test
    void sqlAndJavaAgreeOnAHalfCentBoundary() {
        String runId = "run-" + UUID.randomUUID();
        String batchId = "batch-" + UUID.randomUUID();
        String txnId = runId + "-txn-1";

        // Scale 3, accepted by EventValidator (which rejects only scale > 4).
        captureAt(txnId, new BigDecimal("10.005"), "merch-1");

        // Confirm the sub-cent value survived into the ledger rather than
        // being rounded on the way in -- the premise of the whole test.
        BigDecimal stored = jdbc.queryForObject(
                "SELECT amount FROM ledger_entries WHERE amount > 0", BigDecimal.class);
        assertThat(stored).isEqualByComparingTo(new BigDecimal("10.005"));

        // What the SQL path computes for the same row.
        long sqlMinorUnits = jdbc.queryForObject(
                "SELECT ROUND(amount * 100)::bigint FROM ledger_entries WHERE amount > 0",
                Long.class);
        // What the Java path computes, mirrored here exactly.
        long javaMinorUnits = new BigDecimal("10.005")
                .movePointRight(2).setScale(0, java.math.RoundingMode.HALF_UP).longValueExact();

        assertThat(sqlMinorUnits)
                .as("SQL ROUND and BigDecimal.HALF_UP must agree at the half-cent boundary")
                .isEqualTo(javaMinorUnits)
                .isEqualTo(1001L);

        // And the two paths agree in situ: a settlement line reporting 1001
        // exact-matches (Java path, via AMOUNT_MISMATCH not firing) rather
        // than disagreeing with what the candidate query would have found.
        jdbc.update(
                "INSERT INTO recon_batches (batch_id, run_id, seed, base_instant, row_count, file_sha256) "
                        + "VALUES (?, ?, 1, ?, 1, 'deadbeef')",
                batchId, runId, Timestamp.from(CAPTURED_AT));
        jdbc.update(
                "INSERT INTO settlement_records "
                        + "(batch_id, line_number, external_txn_id, merchant_id, gross_amount_minor, "
                        + " fee_minor, currency, settled_at, raw_line) "
                        + "VALUES (?, 1, ?, 'merch-1', 1001, 0, 'USD', ?, 'raw')",
                batchId, txnId, Timestamp.from(SETTLED_AT));

        reconciliationService.run(batchId);

        long reconRunId = jdbc.queryForObject(
                "SELECT recon_run_id FROM recon_runs WHERE batch_id = ?", Long.class, batchId);
        assertThat(jdbc.queryForObject(
                "SELECT outcome FROM recon_line_outcomes WHERE recon_run_id = ? AND line_number = 1",
                String.class, reconRunId))
                .isEqualTo("MATCHED");
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
}
