package com.ledgerline.generator;

import java.math.BigDecimal;

import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.stereotype.Component;

/**
 * Records what the generator deliberately broke.
 *
 * Writes to the `faultlab` schema, not `public`. That separation is the whole
 * design: this is the answer key a reconciliation engine is graded against, and
 * an engine that can read it is not being graded on detection, only on
 * transcription.
 *
 * Nothing in the ledger or the consumer path depends on this class, and nothing
 * here reads the ledger. The generator is the only writer.
 */
@Component
public class FaultLedger {

    private final NamedParameterJdbcTemplate jdbc;

    FaultLedger(NamedParameterJdbcTemplate jdbc) {
        this.jdbc = jdbc;
    }

    /** Opens a run, recording the seed that makes it reproducible. */
    public void startRun(GeneratorConfig config) {
        jdbc.update(
                "INSERT INTO faultlab.generator_runs "
                        + "(run_id, seed, transaction_count, rate_per_second, fault_rates) "
                        + "VALUES (:runId, :seed, :count, :rate, :faultRates)",
                new MapSqlParameterSource()
                        .addValue("runId", config.runId())
                        .addValue("seed", config.seed())
                        .addValue("count", config.transactionCount())
                        .addValue("rate", config.ratePerSecond())
                        .addValue("faultRates", config.faultRates().toString()));
    }

    public void finishRun(String runId) {
        jdbc.update(
                "UPDATE faultlab.generator_runs SET finished_at = now() WHERE run_id = :runId",
                new MapSqlParameterSource("runId", runId));
    }

    /**
     * Records one injected fault.
     *
     * Called once per fault, at the moment of injection, so the count of rows
     * here is exactly the count of faults in the stream. A fault injected
     * without a row is worse than no fault at all: it becomes a defect the
     * grading harness scores as a false positive when a detector correctly
     * finds it.
     */
    public void record(InjectedFault fault) {
        jdbc.update(
                "INSERT INTO faultlab.injected_faults "
                        + "(run_id, fault_type, external_txn_id, event_id, "
                        + " expected_amount, actual_amount, occurrences, detail) "
                        + "VALUES (:runId, :faultType, :txnId, :eventId, "
                        + " :expected, :actual, :occurrences, :detail)",
                new MapSqlParameterSource()
                        .addValue("runId", fault.runId())
                        .addValue("faultType", fault.type().name())
                        .addValue("txnId", fault.externalTxnId())
                        .addValue("eventId", fault.eventId())
                        .addValue("expected", fault.expectedAmount())
                        .addValue("actual", fault.actualAmount())
                        .addValue("occurrences", fault.occurrences())
                        .addValue("detail", fault.detail()));
    }

    /**
     * One recorded fault.
     *
     * @param expectedAmount what the amount should have been, for drift
     * @param actualAmount   what it became, for drift
     * @param occurrences    how many copies, for duplicate publishes
     */
    public record InjectedFault(
            String runId,
            FaultType type,
            String externalTxnId,
            String eventId,
            BigDecimal expectedAmount,
            BigDecimal actualAmount,
            Integer occurrences,
            String detail) {

        public static InjectedFault of(String runId, FaultType type, String txnId, String detail) {
            return new InjectedFault(runId, type, txnId, null, null, null, null, detail);
        }

        public static InjectedFault duplicate(
                String runId, String txnId, String eventId, int copies) {
            return new InjectedFault(runId, FaultType.DUPLICATE_PUBLISH, txnId, eventId,
                    null, null, copies, "published " + copies + " times");
        }

        public static InjectedFault drift(
                String runId, String txnId, String eventId,
                BigDecimal captured, BigDecimal settled) {
            return new InjectedFault(runId, FaultType.AMOUNT_DRIFT, txnId, eventId,
                    captured, settled, null,
                    "captured " + captured.toPlainString() + ", settled " + settled.toPlainString());
        }
    }
}
