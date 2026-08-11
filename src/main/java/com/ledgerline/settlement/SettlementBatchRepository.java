package com.ledgerline.settlement;

import java.math.BigDecimal;
import java.time.Instant;

import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.stereotype.Component;

/**
 * Records a settlement batch and its ground truth.
 *
 * The counterpart to {@link com.ledgerline.generator.FaultLedger} for the
 * network side of the comparison: writes to {@code recon_batches} (this
 * batch's identity) and to {@code faultlab.injected_faults} with
 * {@code source = 'NETWORK'} (what was deliberately broken in it).
 *
 * Nothing here reads the ledger, and nothing in the ledger or consumer path
 * depends on this class.
 */
@Component
class SettlementBatchRepository {

    private final NamedParameterJdbcTemplate jdbc;

    SettlementBatchRepository(NamedParameterJdbcTemplate jdbc) {
        this.jdbc = jdbc;
    }

    /**
     * @param publishFailures how many messages in the generator run behind
     *                        this batch failed to send. Makes the batch
     *                        self-describing about its own transmission
     *                        health -- see the V10 migration comment for why
     *                        that matters given fail-soft publish handling.
     */
    void recordBatch(SettlementConfig config, int rowCount, String fileSha256, int publishFailures) {
        jdbc.update(
                "INSERT INTO recon_batches "
                        + "(batch_id, run_id, seed, base_instant, row_count, file_sha256, publish_failures) "
                        + "VALUES (:batchId, :runId, :seed, :baseInstant, :rowCount, :fileSha256, :publishFailures)",
                new MapSqlParameterSource()
                        .addValue("batchId", config.batchId())
                        .addValue("runId", config.runId())
                        .addValue("seed", config.seed())
                        .addValue("baseInstant", java.sql.Timestamp.from(config.baseInstant()))
                        .addValue("rowCount", rowCount)
                        .addValue("fileSha256", fileSha256)
                        .addValue("publishFailures", publishFailures));
    }

    /**
     * Records one injected fault, from what was actually written to the file.
     *
     * Callers must derive {@code fault} from the emitted rows and omissions,
     * not from the decision to inject -- those are different claims, and a
     * label describing an intention rather than an outcome would be wrong if
     * the two ever diverged.
     */
    void recordFault(NetworkFault fault) {
        jdbc.update(
                "INSERT INTO faultlab.injected_faults "
                        + "(run_id, fault_type, external_txn_id, event_id, "
                        + " expected_amount, actual_amount, occurrences, detail, source) "
                        + "VALUES (:runId, :faultType, :txnId, :eventId, "
                        + " :expected, :actual, :occurrences, :detail, 'NETWORK')",
                new MapSqlParameterSource()
                        .addValue("runId", fault.batchId())
                        .addValue("faultType", fault.type().name())
                        .addValue("txnId", fault.externalTxnId())
                        .addValue("eventId", fault.eventId())
                        .addValue("expected", fault.expectedAmount())
                        .addValue("actual", fault.actualAmount())
                        .addValue("occurrences", fault.occurrences())
                        .addValue("detail", fault.detail()));
    }

    /**
     * One recorded network-side fault.
     *
     * @param batchId        stored in {@code run_id} -- the ground-truth table
     *                       is keyed on whatever run produced the fault, and for
     *                       a settlement batch that is the batch itself.
     * @param expectedAmount original amount before the fault, for drift; minor
     *                       units, still represented as a BigDecimal column to
     *                       match the existing table's NUMERIC type.
     * @param actualAmount   amount after the fault, for drift.
     * @param occurrences    how many times the row appears, for duplicates.
     */
    record NetworkFault(
            String batchId,
            NetworkFaultType type,
            String externalTxnId,
            String eventId,
            BigDecimal expectedAmount,
            BigDecimal actualAmount,
            Integer occurrences,
            String detail) {

        static NetworkFault of(String batchId, NetworkFaultType type, String txnId, String detail) {
            return new NetworkFault(batchId, type, txnId, null, null, null, null, detail);
        }

        static NetworkFault drift(
                String batchId, String txnId, long originalMinor, long driftedMinor) {
            long delta = driftedMinor - originalMinor;
            return new NetworkFault(batchId, NetworkFaultType.NETWORK_AMOUNT_DRIFT, txnId, null,
                    BigDecimal.valueOf(originalMinor), BigDecimal.valueOf(driftedMinor), null,
                    "original " + originalMinor + ", drifted " + driftedMinor
                            + ", delta " + (delta > 0 ? "+" + delta : delta));
        }

        static NetworkFault lateSettlement(
                String batchId, String txnId, Instant original, Instant shifted) {
            return new NetworkFault(batchId, NetworkFaultType.NETWORK_LATE_SETTLEMENT, txnId, null,
                    null, null, null,
                    "original " + original + ", shifted " + shifted);
        }

        static NetworkFault duplicate(String batchId, String txnId, int occurrences) {
            return new NetworkFault(batchId, NetworkFaultType.NETWORK_DUPLICATE_ROW, txnId, null,
                    null, null, occurrences, "settled " + occurrences + " times in this batch");
        }

        /**
         * @param originalTxnId the real payment this row settles -- the
         *                      ground truth a matcher is graded against
         *                      recovering, not the string actually written
         *                      to the file
         * @param writtenTxnId  the corrupted id as it appears in the
         *                      settlement file, kept in {@code detail} for
         *                      diagnosis; not the fault's own identity,
         *                      since {@link #externalTxnId} already carries
         *                      that
         */
        static NetworkFault mangledTxnId(String batchId, String originalTxnId, String writtenTxnId) {
            return new NetworkFault(batchId, NetworkFaultType.NETWORK_MANGLED_TXN_ID, originalTxnId, null,
                    null, null, null,
                    "original id " + originalTxnId + " written as " + writtenTxnId);
        }
    }
}
