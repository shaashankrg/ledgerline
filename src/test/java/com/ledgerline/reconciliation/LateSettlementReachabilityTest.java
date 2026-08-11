package com.ledgerline.reconciliation;

import static org.assertj.core.api.Assertions.assertThat;

import java.math.BigDecimal;
import java.sql.Timestamp;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;

import com.ledgerline.AbstractPostgresTest;
import com.ledgerline.domain.EventType;
import com.ledgerline.domain.TransactionEvent;

/**
 * Documents, by demonstration, that the NETWORK_LATE_SETTLEMENT fault is
 * currently unreachable by any of the five exception types.
 *
 * The Day 1 simulator's late-settlement fault shifts a row's settled_at 20-90
 * days beyond the honest 18-hour lag (SettlementSimulator's
 * LATE_SETTLEMENT_MIN_SHIFT / MAX_SHIFT) but leaves its external_txn_id
 * intact. Pass 1 therefore matches it exactly on id -- and exact matching
 * does not consider time at all -- so the row lands MATCHED with no exception
 * raised. Pass 2 never sees it, because only MISSING_IN_LEDGER lines are
 * eligible for fuzzy recovery.
 *
 * The consequence for Day 4: an injected fault with a ground-truth row in
 * faultlab and no reachable classification in the engine. Scored naively it
 * is a permanent false negative, blamed on a matcher that was never given a
 * mechanism to catch it -- the same shape as the VOIDED/STATE_CONFLICT gap
 * the Day 2 follow-up closed.
 *
 * This test asserts the *current* behaviour deliberately. It is not a wish;
 * it is a pin, so that if a later change makes late settlement detectable,
 * this test fails and forces the Day 4 mapping table to be updated in the
 * same breath. No fix is implemented here -- per the Day 3 spec, the
 * remediation is a Day 4 mapping/scoping decision, not a Day 3 code change.
 */
class LateSettlementReachabilityTest extends AbstractPostgresTest {

    private static final Instant CAPTURED_AT = Instant.parse("2026-01-15T06:00:00Z");
    /** Honest settlement: 18h after capture. */
    private static final Instant HONEST_SETTLED_AT = CAPTURED_AT.plus(Duration.ofHours(18));
    /** Late settlement: the low end of the simulator's 20-90 day shift. */
    private static final Instant LATE_SETTLED_AT = HONEST_SETTLED_AT.plus(Duration.ofDays(20));

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

    @Test
    void lateSettlementIsMatchedAndRaisesNoException() {
        String runId = "run-" + UUID.randomUUID();
        String batchId = "batch-" + UUID.randomUUID();

        captureAt(runId + "-txn-1", new BigDecimal("50.00"), "merch-1");

        jdbc.update(
                "INSERT INTO recon_batches (batch_id, run_id, seed, base_instant, row_count, file_sha256) "
                        + "VALUES (?, ?, 1, ?, 1, 'deadbeef')",
                batchId, runId, Timestamp.from(CAPTURED_AT));
        // The fault exactly as the simulator injects it: correct id, correct
        // amount, correct merchant, settled_at 20 days late.
        jdbc.update(
                "INSERT INTO settlement_records "
                        + "(batch_id, line_number, external_txn_id, merchant_id, gross_amount_minor, "
                        + " fee_minor, currency, settled_at, raw_line) "
                        + "VALUES (?, 1, ?, 'merch-1', 5000, 0, 'USD', ?, 'raw')",
                batchId, runId + "-txn-1", Timestamp.from(LATE_SETTLED_AT));

        reconciliationService.run(batchId);

        long reconRunId = jdbc.queryForObject(
                "SELECT recon_run_id FROM recon_runs WHERE batch_id = ?", Long.class, batchId);

        Map<String, Object> row = jdbc.queryForMap(
                "SELECT * FROM recon_line_outcomes WHERE recon_run_id = ? AND line_number = 1",
                reconRunId);
        List<String> exceptionTypes = jdbc.queryForList(
                "SELECT type FROM recon_exceptions WHERE recon_run_id = ?", String.class, reconRunId);

        System.out.println("[LateSettlementReachabilityTest] outcome=" + row.get("outcome")
                + " match_method=" + row.get("match_method")
                + " exceptions=" + exceptionTypes
                + " (settled " + Duration.between(HONEST_SETTLED_AT, LATE_SETTLED_AT).toDays()
                + " days later than honest)");

        // The finding, pinned: a 20-day-late settlement is still classified
        // MATCHED with no exception raised -- the engine does not yet treat
        // lateness as a defect. That part of the original finding still
        // holds; the fix-up item this test now also proves is narrower than
        // "detect lateness": it is "stop discarding the evidence."
        assertThat(row.get("outcome")).isEqualTo("MATCHED");
        assertThat(row.get("match_method")).isEqualTo("EXACT");
        assertThat(exceptionTypes).isEmpty();

        // time_delta_seconds is now populated for exact matches, not just
        // fuzzy ones -- the Day 3 fix-up's item 3. A large positive value
        // (settled_at ran behind capture time, same sign convention as
        // delta_minor) is on the row even though nothing yet classifies it,
        // so a later decision has data to work from instead of an empty
        // column.
        long expectedDeltaSeconds = Duration.between(CAPTURED_AT, LATE_SETTLED_AT).getSeconds();
        assertThat(row.get("time_delta_seconds")).isNotNull();
        assertThat(((Number) row.get("time_delta_seconds")).longValue())
                .isEqualTo(expectedDeltaSeconds)
                .isPositive();
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
