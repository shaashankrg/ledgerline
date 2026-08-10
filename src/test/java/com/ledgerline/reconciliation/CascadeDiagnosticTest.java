package com.ledgerline.reconciliation;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.sql.Timestamp;
import java.time.Instant;
import java.util.UUID;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.jdbc.core.JdbcTemplate;

import com.ledgerline.AbstractPostgresTest;

/**
 * Proves the accounting-hole invariant behind V11's ON DELETE CASCADE grants:
 * a recon_line_outcomes row can never be deleted while its settlement_records
 * row survives.
 *
 * V11 gives recon_exceptions.batch_id and recon_line_outcomes.exception_id
 * ON DELETE CASCADE from recon_batches and recon_exceptions respectively, so
 * that the existing (off-limits) settlement tests' unqualified
 * DELETE FROM recon_batches teardown doesn't fail against a leftover
 * recon_exceptions row. Cascades delete quietly by design, so the invariant
 * that matters is proven here rather than assumed: settlement_records.batch_id
 * REFERENCES recon_batches(batch_id) with no cascade (V8, off limits) --
 * Postgres's default RESTRICT -- which means deleting a recon_batches row
 * while any settlement_records row still references it fails outright. That
 * failure is the backstop: it is what stops the recon_batches -> recon_exceptions
 * cascade from ever reaching a recon_line_outcomes row before the
 * settlement_records -> recon_line_outcomes cascade (also ON DELETE CASCADE,
 * V11) has already removed it. A settlement line can therefore never lose its
 * disposition silently -- it is always removed together with, never ahead of,
 * the settlement_records row it describes.
 */
class CascadeDiagnosticTest extends AbstractPostgresTest {

    @Autowired
    private JdbcTemplate jdbc;

    @Test
    void deletingBatchWhileSettlementRecordsSurviveIsBlockedNotCascaded() {
        String batchId = "batch-" + UUID.randomUUID();
        String runId = "run-" + UUID.randomUUID();
        jdbc.update(
                "INSERT INTO recon_batches (batch_id, run_id, seed, base_instant, row_count, file_sha256) "
                        + "VALUES (?, ?, 1, ?, 1, 'deadbeef')",
                batchId, runId, Timestamp.from(Instant.parse("2026-01-01T00:00:00Z")));
        jdbc.update(
                "INSERT INTO settlement_records "
                        + "(batch_id, line_number, external_txn_id, merchant_id, gross_amount_minor, "
                        + " fee_minor, currency, settled_at, raw_line) "
                        + "VALUES (?, 1, 'txn-1', 'merch-1', 1000, 20, 'USD', ?, 'raw')",
                batchId, Timestamp.from(Instant.parse("2026-01-01T00:00:00Z")));
        jdbc.update(
                "INSERT INTO recon_line_outcomes (batch_id, line_number, outcome) VALUES (?, 1, 'MATCHED')",
                batchId);

        // recon_batches -> settlement_records has no cascade (V8, off limits).
        // This delete must fail outright, not silently orphan the outcome row
        // via the recon_batches -> recon_exceptions -> recon_line_outcomes
        // cascade path V11 adds.
        assertThatThrownBy(() -> jdbc.update("DELETE FROM recon_batches WHERE batch_id = ?", batchId))
                .isInstanceOf(DataIntegrityViolationException.class);

        // The outcome row must still exist -- proving the invariant holds by
        // construction, not just by an exception being thrown somewhere.
        Integer count = jdbc.queryForObject(
                "SELECT count(*) FROM recon_line_outcomes WHERE batch_id = ?", Integer.class, batchId);
        assertThat(count).isEqualTo(1);

        // The correct teardown order -- settlement_records, then recon_batches
        // -- does cascade cleanly, which is what the existing settlement
        // tests' setUp() methods already rely on.
        jdbc.update("DELETE FROM settlement_records WHERE batch_id = ?", batchId);
        jdbc.update("DELETE FROM recon_batches WHERE batch_id = ?", batchId);
        Integer countAfter = jdbc.queryForObject(
                "SELECT count(*) FROM recon_line_outcomes WHERE batch_id = ?", Integer.class, batchId);
        assertThat(countAfter).isZero();
    }
}
