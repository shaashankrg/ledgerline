package com.ledgerline.reconciliation;

import static org.assertj.core.api.Assertions.assertThat;

import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Timestamp;
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
import com.ledgerline.reconciliation.ReconciliationAudit.AuditResult;
import com.ledgerline.transfer.TransactionEventService;

/**
 * Matcher pass 1 against real Postgres: exact matching and exception
 * classification.
 *
 * Fixtures are built directly -- one AUTHORIZE/CAPTURE (and sometimes REFUND)
 * through the real {@link TransactionEventService}, then hand-inserted
 * settlement_records/recon_batches rows -- rather than through the full
 * generator+Kafka+consumer pipeline. This test suite needs precise control
 * over individual scenarios (a null external_txn_id, a specific duplicate
 * count, a VOIDED-vs-REFUNDED state), which the generator's probabilistic
 * fault injection cannot target on demand; going through the real event
 * service still exercises the real state machine and entry-writing path
 * rather than hand-writing ledger rows, so what's under test on the ledger
 * side is the same code the rest of the pipeline runs.
 */
class ReconciliationServiceTest extends AbstractPostgresTest {

    @Autowired
    private TransactionEventService eventService;

    @Autowired
    private ReconciliationService reconciliationService;

    @Autowired
    private ReconciliationAudit audit;

    @Autowired
    private JdbcTemplate jdbc;

    private static final Instant SETTLED_AT = Instant.parse("2026-01-16T12:00:00Z");

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

    // ---- Fixture helpers -----------------------------------------------------

    private String runId() {
        return "run-" + UUID.randomUUID();
    }

    private String batchId() {
        return "batch-" + UUID.randomUUID();
    }

    /** Authorizes and captures externalTxnId for amountMajor, merchantId "merch-1". */
    private void capture(String externalTxnId, BigDecimal amountMajor) {
        capture(externalTxnId, amountMajor, "merch-1");
    }

    private void capture(String externalTxnId, BigDecimal amountMajor, String merchantId) {
        eventService.apply(new TransactionEvent(
                externalTxnId, externalTxnId + ":AUTHORIZE", EventType.AUTHORIZE,
                null, null, null, null, Instant.parse("2026-01-15T00:00:00Z"), merchantId));
        eventService.apply(new TransactionEvent(
                externalTxnId, externalTxnId + ":CAPTURE", EventType.CAPTURE,
                alice, merchant, amountMajor, "USD", Instant.parse("2026-01-15T01:00:00Z"), merchantId));
    }

    private void refund(String externalTxnId, BigDecimal amountMajor) {
        eventService.apply(new TransactionEvent(
                externalTxnId, externalTxnId + ":REFUND", EventType.REFUND,
                alice, merchant, amountMajor, "USD", Instant.parse("2026-01-15T02:00:00Z"), "merch-1"));
    }

    private void voidAuth(String externalTxnId) {
        eventService.apply(new TransactionEvent(
                externalTxnId, externalTxnId + ":AUTHORIZE", EventType.AUTHORIZE,
                null, null, null, null, Instant.parse("2026-01-15T00:00:00Z"), "merch-1"));
        eventService.apply(new TransactionEvent(
                externalTxnId, externalTxnId + ":VOID", EventType.VOID,
                null, null, null, null, Instant.parse("2026-01-15T00:30:00Z"), "merch-1"));
    }

    private void insertBatch(String batchId, String runId, int rowCount) {
        jdbc.update(
                "INSERT INTO recon_batches (batch_id, run_id, seed, base_instant, row_count, file_sha256) "
                        + "VALUES (?, ?, 1, ?, ?, 'deadbeef')",
                batchId, runId, Timestamp.from(SETTLED_AT), rowCount);
    }

    private void insertSettlementLine(
            String batchId, int lineNumber, String externalTxnId, String merchantId,
            long grossAmountMinor, long feeMinor) {
        jdbc.update(
                "INSERT INTO settlement_records "
                        + "(batch_id, line_number, external_txn_id, merchant_id, gross_amount_minor, "
                        + " fee_minor, currency, settled_at, raw_line) "
                        + "VALUES (?, ?, ?, ?, ?, ?, 'USD', ?, 'raw')",
                batchId, lineNumber, externalTxnId, merchantId, grossAmountMinor, feeMinor,
                Timestamp.from(SETTLED_AT));
    }

    /**
     * The recon_runs row for this batch at the default window. Exceptions and
     * outcomes are keyed on the run, not the batch, since V12 -- a batch can
     * have several runs, one per window.
     */
    private long runIdOf(String batchId) {
        return runIdOf(batchId, ReconciliationService.DEFAULT_WINDOW_SECONDS);
    }

    private long runIdOf(String batchId, int windowSeconds) {
        return jdbc.queryForObject(
                "SELECT recon_run_id FROM recon_runs WHERE batch_id = ? AND window_seconds = ?",
                Long.class, batchId, windowSeconds);
    }

    private List<Map<String, Object>> exceptionsFor(String batchId, String type) {
        return jdbc.queryForList(
                "SELECT * FROM recon_exceptions WHERE recon_run_id = ? AND type = ?",
                runIdOf(batchId), type);
    }

    private String outcomeOf(String batchId, int lineNumber) {
        return outcomeOf(batchId, ReconciliationService.DEFAULT_WINDOW_SECONDS, lineNumber);
    }

    private String outcomeOf(String batchId, int windowSeconds, int lineNumber) {
        return jdbc.queryForObject(
                "SELECT outcome FROM recon_line_outcomes WHERE recon_run_id = ? AND line_number = ?",
                String.class, runIdOf(batchId, windowSeconds), lineNumber);
    }

    /** One outcome row, whole, for assertions over the pass-2 metadata columns. */
    private Map<String, Object> outcomeRow(String batchId, int lineNumber) {
        return jdbc.queryForMap(
                "SELECT * FROM recon_line_outcomes WHERE recon_run_id = ? AND line_number = ?",
                runIdOf(batchId), lineNumber);
    }

    private int countOutcome(String batchId, String outcome) {
        return jdbc.queryForObject(
                "SELECT count(*) FROM recon_line_outcomes WHERE recon_run_id = ? AND outcome = ?",
                Integer.class, runIdOf(batchId), outcome);
    }

    // ---- One test per exception type -----------------------------------------

    @Test
    void matchedRowProducesNoException() {
        String runId = runId();
        String batchId = batchId();
        capture(runId + "-txn-1", new BigDecimal("50.00"));
        insertBatch(batchId, runId, 1);
        insertSettlementLine(batchId, 1, runId + "-txn-1", "merch-1", 5000, 100);

        reconciliationService.run(batchId);

        assertThat(outcomeOf(batchId, 1)).isEqualTo("MATCHED");
        assertThat(jdbc.queryForObject(
                "SELECT count(*) FROM recon_exceptions WHERE recon_run_id = ?",
                Integer.class, runIdOf(batchId)))
                .isZero();
        assertThat(jdbc.queryForObject(
                "SELECT exception_id FROM recon_line_outcomes WHERE recon_run_id = ? AND line_number = 1",
                Long.class, runIdOf(batchId)))
                .isNull();
    }

    @Test
    void feeIsNotSubtractedFromGrossInComparison() {
        String runId = runId();
        String batchId = batchId();
        capture(runId + "-txn-1", new BigDecimal("50.00"));
        insertBatch(batchId, runId, 1);
        // Nonzero fee, gross still equal to the captured amount -- must match,
        // not read as a mismatch of (gross - fee) against captured.
        insertSettlementLine(batchId, 1, runId + "-txn-1", "merch-1", 5000, 250);

        reconciliationService.run(batchId);

        assertThat(outcomeOf(batchId, 1)).isEqualTo("MATCHED");
    }

    @Test
    void missingInLedgerForUnknownExternalTxnId() {
        String runId = runId();
        String batchId = batchId();
        insertBatch(batchId, runId, 1);
        insertSettlementLine(batchId, 1, runId + "-txn-999", "merch-1", 5000, 100);

        reconciliationService.run(batchId);

        assertThat(outcomeOf(batchId, 1)).isEqualTo("MISSING_IN_LEDGER");
        List<Map<String, Object>> exceptions = exceptionsFor(batchId, "MISSING_IN_LEDGER");
        assertThat(exceptions).hasSize(1);
        assertThat(exceptions.get(0).get("subject_key")).isEqualTo(runId + "-txn-999");

        // No other exception type fires for this subject.
        assertThat(jdbc.queryForObject(
                "SELECT count(*) FROM recon_exceptions WHERE recon_run_id = ? AND subject_key = ?",
                Integer.class, runIdOf(batchId),runId + "-txn-999"))
                .isEqualTo(1);
    }

    @Test
    void missingInLedgerForNullExternalTxnId() {
        String runId = runId();
        String batchId = batchId();
        insertBatch(batchId, runId, 1);
        insertSettlementLine(batchId, 1, null, "merch-1", 5000, 100);

        reconciliationService.run(batchId);

        assertThat(outcomeOf(batchId, 1)).isEqualTo("MISSING_IN_LEDGER");
        List<Map<String, Object>> exceptions = exceptionsFor(batchId, "MISSING_IN_LEDGER");
        assertThat(exceptions).hasSize(1);
        assertThat(exceptions.get(0).get("subject_key")).isEqualTo("line:1");
        assertThat(exceptions.get(0).get("external_txn_id")).isNull();
    }

    @Test
    void duplicateSettlementCoversAllLinesWithOneException() {
        String runId = runId();
        String batchId = batchId();
        capture(runId + "-txn-1", new BigDecimal("50.00"));
        insertBatch(batchId, runId, 2);
        insertSettlementLine(batchId, 1, runId + "-txn-1", "merch-1", 5000, 100);
        insertSettlementLine(batchId, 2, runId + "-txn-1", "merch-1", 5000, 100);

        reconciliationService.run(batchId);

        assertThat(outcomeOf(batchId, 1)).isEqualTo("DUPLICATE_SETTLEMENT");
        assertThat(outcomeOf(batchId, 2)).isEqualTo("DUPLICATE_SETTLEMENT");

        List<Map<String, Object>> exceptions = exceptionsFor(batchId, "DUPLICATE_SETTLEMENT");
        assertThat(exceptions).hasSize(1);
        Integer[] lines = toIntegerArray(exceptions.get(0).get("settlement_line_numbers"));
        assertThat(lines).containsExactly(1, 2);

        // No AMOUNT_MISMATCH or STATE_CONFLICT fires for the same subject even
        // though the payment exists and agrees in amount -- duplication takes
        // precedence.
        assertThat(jdbc.queryForObject(
                "SELECT count(*) FROM recon_exceptions WHERE recon_run_id = ? AND subject_key = ?",
                Integer.class, runIdOf(batchId),runId + "-txn-1"))
                .isEqualTo(1);
    }

    @Test
    void amountMismatchIsSignedSettlementMinusLedger() {
        String runId = runId();
        String batchId = batchId();
        capture(runId + "-txn-1", new BigDecimal("50.00"));
        insertBatch(batchId, runId, 1);
        // Settlement reports MORE than captured: delta must be positive.
        insertSettlementLine(batchId, 1, runId + "-txn-1", "merch-1", 5500, 110);

        reconciliationService.run(batchId);

        assertThat(outcomeOf(batchId, 1)).isEqualTo("AMOUNT_MISMATCH");
        List<Map<String, Object>> exceptions = exceptionsFor(batchId, "AMOUNT_MISMATCH");
        assertThat(exceptions).hasSize(1);
        assertThat(((Number) exceptions.get(0).get("delta_minor")).longValue()).isEqualTo(500L);
        assertThat(((Number) exceptions.get(0).get("settlement_amount_minor")).longValue()).isEqualTo(5500L);
        assertThat(((Number) exceptions.get(0).get("ledger_amount_minor")).longValue()).isEqualTo(5000L);
    }

    @Test
    void amountMismatchDirectionFlipsWhenSettlementReportsLess() {
        String runId = runId();
        String batchId = batchId();
        capture(runId + "-txn-1", new BigDecimal("50.00"));
        insertBatch(batchId, runId, 1);
        // Settlement reports LESS than captured: delta must be negative.
        insertSettlementLine(batchId, 1, runId + "-txn-1", "merch-1", 4500, 90);

        reconciliationService.run(batchId);

        List<Map<String, Object>> exceptions = exceptionsFor(batchId, "AMOUNT_MISMATCH");
        assertThat(((Number) exceptions.get(0).get("delta_minor")).longValue()).isEqualTo(-500L);
    }

    @Test
    void missingInSettlementForCapturedPaymentWithNoLine() {
        String runId = runId();
        String batchId = batchId();
        capture(runId + "-txn-1", new BigDecimal("50.00"));
        insertBatch(batchId, runId, 0);

        reconciliationService.run(batchId);

        List<Map<String, Object>> exceptions = exceptionsFor(batchId, "MISSING_IN_SETTLEMENT");
        assertThat(exceptions).hasSize(1);
        assertThat(exceptions.get(0).get("subject_key")).isEqualTo(runId + "-txn-1");
        assertThat(toIntegerArray(exceptions.get(0).get("settlement_line_numbers"))).isEmpty();

        // Produces no recon_line_outcomes row: there is no line to attach one to.
        assertThat(jdbc.queryForObject(
                "SELECT count(*) FROM recon_line_outcomes WHERE recon_run_id = ?",
                Integer.class, runIdOf(batchId)))
                .isZero();
    }

    @Test
    void voidedPaymentSettlementLineIsStateConflict() {
        // A VOIDED transaction never captures, so it never enters
        // CapturedLedgerView -- but it IS identifiable, via transaction_states
        // directly (PaymentStateView), which is exactly the distinction Item 1
        // of the fix-up spec exists to draw: existence and eligibility are two
        // different questions, and a payment can be identifiable without ever
        // having been captured.
        String runId = runId();
        String batchId = batchId();
        voidAuth(runId + "-txn-1");
        insertBatch(batchId, runId, 1);
        insertSettlementLine(batchId, 1, runId + "-txn-1", "merch-1", 5000, 100);

        reconciliationService.run(batchId);

        assertThat(outcomeOf(batchId, 1)).isEqualTo("STATE_CONFLICT");
        List<Map<String, Object>> exceptions = exceptionsFor(batchId, "STATE_CONFLICT");
        assertThat(exceptions).hasSize(1);
        assertThat(exceptions.get(0).get("payment_state")).isEqualTo("VOIDED");
    }

    @Test
    void refundedPaymentSettlementLineIsStateConflict() {
        String runId = runId();
        String batchId = batchId();
        capture(runId + "-txn-1", new BigDecimal("50.00"));
        refund(runId + "-txn-1", new BigDecimal("50.00"));
        insertBatch(batchId, runId, 1);
        insertSettlementLine(batchId, 1, runId + "-txn-1", "merch-1", 5000, 100);

        reconciliationService.run(batchId);

        assertThat(outcomeOf(batchId, 1)).isEqualTo("STATE_CONFLICT");
        List<Map<String, Object>> exceptions = exceptionsFor(batchId, "STATE_CONFLICT");
        assertThat(exceptions).hasSize(1);
        assertThat(exceptions.get(0).get("payment_state")).isEqualTo("REFUNDED");

        assertThat(jdbc.queryForObject(
                "SELECT count(*) FROM recon_exceptions WHERE recon_run_id = ? AND type = 'AMOUNT_MISMATCH'",
                Integer.class, runIdOf(batchId)))
                .isZero();
    }

    @Test
    void unknownTxnIdIsMissingInLedger() {
        // No transaction_states row at all -- this is the case that proves
        // MISSING_IN_LEDGER and STATE_CONFLICT are now genuinely distinguished.
        // If PaymentStateView were wired back to CapturedLedgerView, this would
        // still correctly read MISSING_IN_LEDGER (nothing captured either), so
        // this test alone doesn't catch that regression -- see the sabotage
        // test below, which does.
        String runId = runId();
        String batchId = batchId();
        insertBatch(batchId, runId, 1);
        insertSettlementLine(batchId, 1, runId + "-txn-nonexistent", "merch-1", 5000, 100);

        reconciliationService.run(batchId);

        assertThat(outcomeOf(batchId, 1)).isEqualTo("MISSING_IN_LEDGER");
        List<Map<String, Object>> exceptions = exceptionsFor(batchId, "MISSING_IN_LEDGER");
        assertThat(exceptions).hasSize(1);
        assertThat(exceptions.get(0).get("payment_state")).isNull();
    }

    @Test
    void authorizedButNotCapturedPaymentIsStateConflict() {
        // Decision (see written report): AUTHORIZED is treated the same as
        // VOIDED/REFUNDED -- identifiable, money never moved, settlement for
        // it is a conflict, not a missing-ledger case.
        String runId = runId();
        String batchId = batchId();
        eventService.apply(new TransactionEvent(
                runId + "-txn-1", runId + "-txn-1:AUTHORIZE", EventType.AUTHORIZE,
                null, null, null, null, Instant.parse("2026-01-15T00:00:00Z"), "merch-1"));
        insertBatch(batchId, runId, 1);
        insertSettlementLine(batchId, 1, runId + "-txn-1", "merch-1", 5000, 100);

        reconciliationService.run(batchId);

        assertThat(outcomeOf(batchId, 1)).isEqualTo("STATE_CONFLICT");
        List<Map<String, Object>> exceptions = exceptionsFor(batchId, "STATE_CONFLICT");
        assertThat(exceptions).hasSize(1);
        assertThat(exceptions.get(0).get("payment_state")).isEqualTo("AUTHORIZED");
    }

    // ---- Deliverable 4: completeness ------------------------------------------

    @Test
    void everyLineHasExactlyOneOutcome() {
        String runId = runId();
        String batchId = batchId();

        // MATCHED
        capture(runId + "-txn-1", new BigDecimal("10.00"));
        // AMOUNT_MISMATCH
        capture(runId + "-txn-2", new BigDecimal("20.00"));
        // STATE_CONFLICT
        capture(runId + "-txn-3", new BigDecimal("30.00"));
        refund(runId + "-txn-3", new BigDecimal("30.00"));
        // DUPLICATE_SETTLEMENT
        capture(runId + "-txn-4", new BigDecimal("40.00"));
        // MISSING_IN_SETTLEMENT
        capture(runId + "-txn-5", new BigDecimal("50.00"));
        // STATE_CONFLICT (voided, never captured -- distinct code path from txn-3's refund)
        voidAuth(runId + "-txn-6");

        insertBatch(batchId, runId, 7);
        insertSettlementLine(batchId, 1, runId + "-txn-1", "merch-1", 1000, 20); // MATCHED
        insertSettlementLine(batchId, 2, runId + "-txn-2", "merch-1", 2500, 50); // AMOUNT_MISMATCH
        insertSettlementLine(batchId, 3, runId + "-txn-3", "merch-1", 3000, 60); // STATE_CONFLICT (refunded)
        insertSettlementLine(batchId, 4, runId + "-txn-4", "merch-1", 4000, 80); // DUPLICATE_SETTLEMENT
        insertSettlementLine(batchId, 5, runId + "-txn-4", "merch-1", 4000, 80); // DUPLICATE_SETTLEMENT
        insertSettlementLine(batchId, 6, runId + "-txn-unknown", "merch-1", 7000, 140); // MISSING_IN_LEDGER
        insertSettlementLine(batchId, 7, runId + "-txn-6", "merch-1", 8000, 160); // STATE_CONFLICT (voided)
        // txn-5 has no settlement line at all -> MISSING_IN_SETTLEMENT

        reconciliationService.run(batchId);

        AuditResult result = audit.audit(batchId);
        assertThat(result.problems()).isEmpty();
        assertThat(result.complete()).isTrue();
    }

    @Test
    void rerunIsANoOp() {
        String runId = runId();
        String batchId = batchId();
        capture(runId + "-txn-1", new BigDecimal("10.00"));
        capture(runId + "-txn-2", new BigDecimal("20.00"));
        insertBatch(batchId, runId, 2);
        insertSettlementLine(batchId, 1, runId + "-txn-1", "merch-1", 1000, 20);
        insertSettlementLine(batchId, 2, runId + "-txn-2", "merch-1", 2500, 50);

        reconciliationService.run(batchId);
        List<String> exceptionsFirst = normalizedExceptionRows(batchId);
        List<Map<String, Object>> outcomesFirst = jdbc.queryForList(
                "SELECT * FROM recon_line_outcomes WHERE recon_run_id = ? ORDER BY line_number",
                runIdOf(batchId));

        reconciliationService.run(batchId);
        List<String> exceptionsSecond = normalizedExceptionRows(batchId);
        List<Map<String, Object>> outcomesSecond = jdbc.queryForList(
                "SELECT * FROM recon_line_outcomes WHERE recon_run_id = ? ORDER BY line_number",
                runIdOf(batchId));

        assertThat(exceptionsSecond).hasSameSizeAs(exceptionsFirst);
        assertThat(exceptionsSecond).isEqualTo(exceptionsFirst);
        assertThat(outcomesSecond).hasSameSizeAs(outcomesFirst);
        assertThat(outcomesSecond).isEqualTo(outcomesFirst);
    }

    /**
     * recon_exceptions rows as comparable strings, id and settlement_line_numbers
     * normalized out of PgArray form -- a fresh SELECT returns a new PgArray
     * instance each time, and PgArray does not implement value equality, so a
     * raw row-map comparison would fail even when the content is identical.
     */
    private List<String> normalizedExceptionRows(String batchId) {
        List<Map<String, Object>> rows = jdbc.queryForList(
                "SELECT * FROM recon_exceptions WHERE recon_run_id = ? ORDER BY id", runIdOf(batchId));
        List<String> normalized = new java.util.ArrayList<>();
        for (Map<String, Object> row : rows) {
            normalized.add(normalizedExceptionRow(row));
        }
        return normalized;
    }

    private String normalizedExceptionRow(Map<String, Object> row) {
        List<String> fields = new java.util.ArrayList<>();
        for (String key : new java.util.TreeSet<>(row.keySet())) {
            if (key.equals("id") || key.equals("detected_at") || key.equals("settlement_line_numbers")) {
                continue;
            }
            fields.add(key + "=" + row.get(key));
        }
        fields.add("settlement_line_numbers="
                + java.util.Arrays.toString(toIntegerArray(row.get("settlement_line_numbers"))));
        return String.join(",", fields);
    }

    @Test
    void exceptionsAreDeduplicatedForNullTxnIds() {
        String runId = runId();
        String batchId = batchId();
        insertBatch(batchId, runId, 2);
        insertSettlementLine(batchId, 1, null, "merch-1", 5000, 100);
        insertSettlementLine(batchId, 2, null, "merch-1", 6000, 120);

        reconciliationService.run(batchId);
        assertThat(exceptionsFor(batchId, "MISSING_IN_LEDGER")).hasSize(2);

        reconciliationService.run(batchId);
        assertThat(exceptionsFor(batchId, "MISSING_IN_LEDGER")).hasSize(2);
    }

    // ---- Isolation -------------------------------------------------------------

    @Test
    void reconciliationRoleCannotReadTheAnswerKey() throws SQLException {
        try (Connection connection = DriverManager.getConnection(
                jdbcUrl(), "recon_role", "recon_role")) {
            org.assertj.core.api.Assertions.assertThatThrownBy(() -> {
                try (var statement = connection.createStatement()) {
                    statement.executeQuery("SELECT * FROM faultlab.injected_faults");
                }
            }).isInstanceOf(SQLException.class);
        }
    }

    @Test
    void reconciliationRoleCanWriteItsOwnFindings() throws SQLException {
        String batchId = batchId();
        insertBatch(batchId, runId(), 0);

        try (Connection connection = DriverManager.getConnection(
                jdbcUrl(), "recon_role", "recon_role")) {
            try (var statement = connection.createStatement()) {
                // Must not throw -- recon_role needs INSERT on its own tables,
                // including recon_runs since V12, because the matcher resolves
                // or creates its own run row.
                //
                // Cleanup goes through the app's own JdbcTemplate, not this
                // connection: recon_role is deliberately granted INSERT and
                // SELECT only, not DELETE, so these probe rows are removed by
                // setUp()'s next DELETE instead.
                statement.execute(
                        "INSERT INTO recon_runs (batch_id, window_seconds, matcher_version) "
                                + "VALUES ('" + batchId + "', 3600, 'probe')");
                statement.execute(
                        "INSERT INTO recon_exceptions "
                                + "(recon_run_id, subject_key, type, external_txn_id, settlement_line_numbers) "
                                + "SELECT recon_run_id, 'probe-subject', 'MISSING_IN_LEDGER', "
                                + "'probe-txn', ARRAY[1] FROM recon_runs "
                                + "WHERE batch_id = '" + batchId + "' AND matcher_version = 'probe'");
            }
        }
    }

    /**
     * The JDBC URL AbstractPostgresTest's container is bound to. Read back
     * through an open connection rather than duplicating a second static
     * PostgreSQLContainer reference here, since the container itself is
     * private to that base class.
     */
    private String jdbcUrl() throws SQLException {
        try (Connection c = jdbc.getDataSource().getConnection()) {
            return c.getMetaData().getURL();
        }
    }

    /** settlement_line_numbers comes back as a java.sql.Array (PgArray), never a plain Integer[]. */
    private static Integer[] toIntegerArray(Object sqlArray) {
        try {
            Object[] raw = (Object[]) ((java.sql.Array) sqlArray).getArray();
            return java.util.Arrays.stream(raw).map(o -> (Integer) o).toArray(Integer[]::new);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }
}
