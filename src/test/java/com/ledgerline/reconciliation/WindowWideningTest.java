package com.ledgerline.reconciliation;

import static org.assertj.core.api.Assertions.assertThat;

import java.math.BigDecimal;
import java.sql.Timestamp;
import java.time.Duration;
import java.time.Instant;
import java.util.LinkedHashMap;
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
 * Day 3 sabotage 3: what does widening the fuzzy window actually do?
 *
 * The Day 3 plan predicts widening produces false positives. This probe does
 * not assume that. It runs one batch at several windows and reports the
 * distribution across FUZZY_MATCHED / AMBIGUOUS / MISSING_IN_LEDGER, because
 * with ambiguity refusal working, a wider window admits *more candidates per
 * line*, and more candidates frequently means two or more -- which produces
 * refusals, not false matches.
 *
 * The table is the deliverable here, not a passing assertion. The only thing
 * asserted is the invariant that must hold at every window: no false match is
 * silently recorded where the matcher should have refused, i.e. every line
 * ends in exactly one bucket and no payment is claimed twice.
 *
 * The batch is built so that widening genuinely changes the answer:
 * capture times are spread across several hours, so each successive window
 * admits a different number of them.
 */
class WindowWideningTest extends AbstractPostgresTest {

    private static final Instant BASE_CAPTURE = Instant.parse("2026-01-15T06:00:00Z");
    /** Every settlement line settles 18h after the *first* capture. */
    private static final Instant SETTLED_AT = BASE_CAPTURE.plus(Duration.ofHours(18));

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

    @Test
    void reportsWhatWideningTheWindowActuallyDoes() {
        String runId = "run-" + UUID.randomUUID();
        String batchId = "batch-" + UUID.randomUUID();

        // Four payments on one merchant at one amount, captured 1h, 6h, 12h,
        // and 30h before the settlement instant. As the window widens, each
        // comes into range in turn -- so the same line goes from no candidate,
        // to one, to several.
        captureAt(runId + "-txn-1", new BigDecimal("50.00"), "merch-1",
                SETTLED_AT.minus(Duration.ofHours(1)));
        captureAt(runId + "-txn-2", new BigDecimal("50.00"), "merch-1",
                SETTLED_AT.minus(Duration.ofHours(6)));
        captureAt(runId + "-txn-3", new BigDecimal("50.00"), "merch-1",
                SETTLED_AT.minus(Duration.ofHours(12)));
        captureAt(runId + "-txn-4", new BigDecimal("50.00"), "merch-1",
                SETTLED_AT.minus(Duration.ofHours(30)));

        // One id-less line: it must pick one of the four, or refuse.
        jdbc.update(
                "INSERT INTO recon_batches (batch_id, run_id, seed, base_instant, row_count, file_sha256) "
                        + "VALUES (?, ?, 1, ?, 1, 'deadbeef')",
                batchId, runId, Timestamp.from(BASE_CAPTURE));
        jdbc.update(
                "INSERT INTO settlement_records "
                        + "(batch_id, line_number, external_txn_id, merchant_id, gross_amount_minor, "
                        + " fee_minor, currency, settled_at, raw_line) "
                        + "VALUES (?, 1, NULL, 'merch-1', 5000, 0, 'USD', ?, 'raw')",
                batchId, Timestamp.from(SETTLED_AT));

        int[] windows = {
                (int) Duration.ofMinutes(30).toSeconds(),
                (int) Duration.ofHours(2).toSeconds(),
                (int) Duration.ofHours(8).toSeconds(),
                (int) Duration.ofHours(24).toSeconds(),
                (int) Duration.ofHours(48).toSeconds(),
        };

        Map<String, Map<String, Integer>> table = new LinkedHashMap<>();

        for (int window : windows) {
            reconciliationService.run(batchId, window);
            long reconRunId = jdbc.queryForObject(
                    "SELECT recon_run_id FROM recon_runs WHERE batch_id = ? AND window_seconds = ?",
                    Long.class, batchId, window);

            Map<String, Integer> counts = new LinkedHashMap<>();
            for (String outcome : new String[] {"FUZZY_MATCHED", "AMBIGUOUS", "MISSING_IN_LEDGER"}) {
                counts.put(outcome, jdbc.queryForObject(
                        "SELECT count(*) FROM recon_line_outcomes "
                                + "WHERE recon_run_id = ? AND outcome = ?",
                        Integer.class, reconRunId, outcome));
            }
            Integer candidateCount = jdbc.queryForObject(
                    "SELECT candidate_count FROM recon_line_outcomes "
                            + "WHERE recon_run_id = ? AND line_number = 1",
                    Integer.class, reconRunId);
            counts.put("candidates_seen", candidateCount);
            table.put(Duration.ofSeconds(window).toString(), counts);

            // The invariant that must hold at every window: exactly one
            // outcome per line, no payment claimed twice. Widening may change
            // *which* bucket a line lands in; it must never break the
            // partition.
            assertThat(audit.auditRun(reconRunId).problems()).isEmpty();
        }

        System.out.println("[WindowWideningTest] window | FUZZY_MATCHED | AMBIGUOUS | MISSING_IN_LEDGER | candidates");
        table.forEach((window, counts) -> System.out.println(
                "[WindowWideningTest] " + window
                        + " | " + counts.get("FUZZY_MATCHED")
                        + " | " + counts.get("AMBIGUOUS")
                        + " | " + counts.get("MISSING_IN_LEDGER")
                        + " | " + counts.get("candidates_seen")));

        // Five distinct runs coexist over one batch -- the Day 4 sweep in
        // miniature.
        assertThat(jdbc.queryForObject(
                "SELECT count(*) FROM recon_runs WHERE batch_id = ?", Integer.class, batchId))
                .isEqualTo(windows.length);
    }

    private void captureAt(String externalTxnId, BigDecimal amountMajor,
            String merchantId, Instant capturedAt) {
        eventService.apply(new TransactionEvent(
                externalTxnId, externalTxnId + ":AUTHORIZE", EventType.AUTHORIZE,
                null, null, null, null, capturedAt, merchantId));
        eventService.apply(new TransactionEvent(
                externalTxnId, externalTxnId + ":CAPTURE", EventType.CAPTURE,
                alice, merchant, amountMajor, "USD", capturedAt, merchantId));
        jdbc.update("UPDATE transactions SET created_at = ? WHERE idempotency_key = ?",
                Timestamp.from(capturedAt), externalTxnId + ":CAPTURE");
    }
}
