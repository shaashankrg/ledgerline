package com.ledgerline.settlement;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.time.format.DateTimeParseException;
import java.util.ArrayList;
import java.util.List;

import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.stereotype.Component;

/**
 * Loads a settlement CSV into {@code settlement_records}.
 *
 * Idempotency choice: reject a load of a {@code batch_id} that already has
 * rows, rather than making the load itself idempotent (e.g. upsert on
 * {@code (batch_id, line_number)}). A settlement file is an external,
 * immutable artifact -- there is no legitimate reason to load the same batch
 * twice with different content, and silently accepting a second load either
 * doubles the rows (if naively re-inserted) or masks a caller bug (if
 * silently ignored). Rejecting loudly is the only option that cannot hide a
 * mistake; a caller that genuinely needs to reload deletes the batch's rows
 * first, which is a deliberate, visible act.
 */
@Component
public class SettlementLoader {

    private final NamedParameterJdbcTemplate jdbc;

    SettlementLoader(NamedParameterJdbcTemplate jdbc) {
        this.jdbc = jdbc;
    }

    /**
     * Loads every data row of {@code csv} into {@code settlement_records}
     * under {@code batchId}.
     *
     * @throws IllegalStateException    if the header doesn't match exactly, or
     *                                   if {@code batchId} already has rows
     * @throws SettlementParseException if any row fails to parse, carrying the
     *                                   1-based line number and the raw line
     */
    public int load(String batchId, InputStream csv) {
        if (alreadyLoaded(batchId)) {
            throw new IllegalStateException(
                    "batch " + batchId + " has already been loaded; delete its rows before reloading");
        }

        List<String> lines = readLines(csv);
        if (lines.isEmpty()) {
            throw new SettlementParseException(0, "", "file is empty, expected a header row");
        }

        String header = lines.get(0);
        if (!header.equals(SettlementCsvWriter.HEADER)) {
            throw new IllegalStateException(
                    "unexpected CSV header. expected [" + SettlementCsvWriter.HEADER
                            + "], got [" + header + "]");
        }

        int loaded = 0;
        for (int i = 1; i < lines.size(); i++) {
            String line = lines.get(i);
            int lineNumber = i; // physical 1-based position of this *data* row
            insertRow(batchId, lineNumber, line);
            loaded++;
        }
        return loaded;
    }

    private void insertRow(String batchId, int lineNumber, String rawLine) {
        List<String> fields = CsvLine.split(rawLine, lineNumber);
        if (fields.size() != 7) {
            throw new SettlementParseException(lineNumber, rawLine,
                    "expected 7 columns, found " + fields.size());
        }

        String csvBatchId = fields.get(0);
        String externalTxnId = fields.get(1).isEmpty() ? null : fields.get(1);
        String merchantId = fields.get(2);
        long grossAmountMinor = parseAmount(fields.get(3), lineNumber, rawLine, "gross_amount_minor");
        long feeMinor = parseAmount(fields.get(4), lineNumber, rawLine, "fee_minor");
        String currency = fields.get(5);
        Instant settledAt = parseInstant(fields.get(6), lineNumber, rawLine);

        jdbc.update(
                "INSERT INTO settlement_records "
                        + "(batch_id, line_number, external_txn_id, merchant_id, "
                        + " gross_amount_minor, fee_minor, currency, settled_at, raw_line) "
                        + "VALUES (:batchId, :lineNumber, :externalTxnId, :merchantId, "
                        + " :gross, :fee, :currency, :settledAt, :rawLine)",
                new MapSqlParameterSource()
                        .addValue("batchId", csvBatchId)
                        .addValue("lineNumber", lineNumber)
                        .addValue("externalTxnId", externalTxnId)
                        .addValue("merchantId", merchantId)
                        .addValue("gross", grossAmountMinor)
                        .addValue("fee", feeMinor)
                        .addValue("currency", currency)
                        .addValue("settledAt", java.sql.Timestamp.from(settledAt))
                        .addValue("rawLine", rawLine));
    }

    /**
     * Parses a minor-unit amount with {@link Long#parseLong}, never a CSV
     * library's type inference -- many hand back a {@code Double} for a
     * numeric-looking column, which silently corrupts values above 2^53. A
     * malformed amount is a loud parse failure carrying the raw line, never a
     * coerced zero.
     */
    private static long parseAmount(String field, int lineNumber, String rawLine, String columnName) {
        try {
            return Long.parseLong(field);
        } catch (NumberFormatException e) {
            throw new SettlementParseException(lineNumber, rawLine,
                    "malformed " + columnName + ": [" + field + "]");
        }
    }

    private static Instant parseInstant(String field, int lineNumber, String rawLine) {
        try {
            return Instant.parse(field);
        } catch (DateTimeParseException e) {
            throw new SettlementParseException(lineNumber, rawLine,
                    "malformed settled_at: [" + field + "]");
        }
    }

    private boolean alreadyLoaded(String batchId) {
        Integer count = jdbc.queryForObject(
                "SELECT count(*) FROM settlement_records WHERE batch_id = :batchId",
                new MapSqlParameterSource("batchId", batchId), Integer.class);
        return count != null && count > 0;
    }

    private static List<String> readLines(InputStream csv) {
        List<String> lines = new ArrayList<>();
        try (BufferedReader reader = new BufferedReader(
                new InputStreamReader(csv, StandardCharsets.UTF_8))) {
            String line;
            while ((line = reader.readLine()) != null) {
                lines.add(line);
            }
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        return lines;
    }

    /**
     * A malformed line, never silently defaulted or skipped.
     *
     * Carries the 1-based line number and the raw text, because "row 4102 is
     * broken" is diagnosable and "some row somewhere is broken" is not.
     */
    public static final class SettlementParseException extends RuntimeException {
        public SettlementParseException(int lineNumber, String rawLine, String reason) {
            super("settlement CSV line " + lineNumber + ": " + reason + " -- raw line: [" + rawLine + "]");
        }
    }
}
