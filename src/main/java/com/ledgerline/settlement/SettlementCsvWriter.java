package com.ledgerline.settlement;

import java.time.Instant;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.List;
import java.util.Locale;

/**
 * Renders settlement rows to RFC 4180 CSV text.
 *
 * Gross and fee are written as separate columns, never netted: given only a
 * net figure, a later reader cannot distinguish a fee change from an amount
 * error, which would make two different exception types indistinguishable.
 *
 * No CSV library is used. Not an oversight -- several popular ones
 * type-infer numeric-looking columns back to a {@code Double}, which silently
 * corrupts amounts above 2^53. Hand-writing avoids that class of bug by
 * construction: every field this class emits is a plain {@code String}, built
 * with {@link String#valueOf} on a {@code long}, never parsed and never
 * passed through a floating point type.
 *
 * Every formatting decision here is fixed regardless of platform or JVM
 * defaults -- {@link Locale#ROOT}, {@link ZoneOffset#UTC}, an explicit
 * {@code \n} line ending -- because this file's byte-identity across machines
 * is a correctness property a later parameter sweep depends on.
 */
final class SettlementCsvWriter {

    static final String HEADER =
            "batch_id,external_txn_id,merchant_id,gross_amount_minor,fee_minor,currency,settled_at";

    private static final DateTimeFormatter SETTLED_AT_FORMAT =
            DateTimeFormatter.ISO_INSTANT.withZone(ZoneOffset.UTC).withLocale(Locale.ROOT);

    private SettlementCsvWriter() {
    }

    static String write(String batchId, List<SettlementRow> rows) {
        StringBuilder sb = new StringBuilder();
        sb.append(HEADER).append('\n');
        for (SettlementRow row : rows) {
            appendRow(sb, batchId, row);
        }
        return sb.toString();
    }

    private static void appendRow(StringBuilder sb, String batchId, SettlementRow row) {
        field(sb, batchId, true);
        field(sb, row.externalTxnId(), true);
        field(sb, row.merchantId(), true);
        field(sb, Long.toString(row.grossAmountMinor()), true);
        field(sb, Long.toString(row.feeMinor()), true);
        field(sb, row.currency(), true);
        field(sb, SETTLED_AT_FORMAT.format(row.settledAt()), false);
        sb.append('\n');
    }

    private static void field(StringBuilder sb, String value, boolean trailingComma) {
        sb.append(quoteIfNeeded(value));
        if (trailingComma) {
            sb.append(',');
        }
    }

    /** Formats an instant the same way {@link #appendRow} does, for tests that need one column. */
    static String formatInstant(Instant instant) {
        return SETTLED_AT_FORMAT.format(instant);
    }

    /**
     * RFC 4180 quoting: wrap in {@code "} if the field contains a comma, a
     * quote, or a newline, doubling any internal {@code "}.
     */
    private static String quoteIfNeeded(String value) {
        if (value == null) {
            return "";
        }
        boolean needsQuoting = value.indexOf(',') >= 0
                || value.indexOf('"') >= 0
                || value.indexOf('\n') >= 0
                || value.indexOf('\r') >= 0;
        if (!needsQuoting) {
            return value;
        }
        return '"' + value.replace("\"", "\"\"") + '"';
    }
}
