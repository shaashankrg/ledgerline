package com.ledgerline.settlement;

import java.util.ArrayList;
import java.util.List;

/**
 * Splits one RFC 4180 CSV line into fields.
 *
 * The counterpart to {@link SettlementCsvWriter}'s quoting: a field wrapped in
 * {@code "} may contain commas and newlines verbatim, and an internal
 * {@code "} is escaped by doubling it. Hand-written for the same reason the
 * writer is -- no CSV library's type inference sits anywhere near amount
 * parsing, which stays in {@link SettlementLoader} as an explicit
 * {@link Long#parseLong}.
 */
final class CsvLine {

    private CsvLine() {
    }

    static List<String> split(String line, int lineNumber) {
        List<String> fields = new ArrayList<>();
        StringBuilder current = new StringBuilder();
        boolean inQuotes = false;
        int i = 0;
        int length = line.length();

        while (i < length) {
            char c = line.charAt(i);
            if (inQuotes) {
                if (c == '"') {
                    if (i + 1 < length && line.charAt(i + 1) == '"') {
                        current.append('"');
                        i += 2;
                        continue;
                    }
                    inQuotes = false;
                    i++;
                    continue;
                }
                current.append(c);
                i++;
                continue;
            }

            if (c == '"') {
                inQuotes = true;
                i++;
                continue;
            }
            if (c == ',') {
                fields.add(current.toString());
                current.setLength(0);
                i++;
                continue;
            }
            current.append(c);
            i++;
        }

        if (inQuotes) {
            throw new SettlementLoader.SettlementParseException(
                    lineNumber, line, "unterminated quoted field");
        }

        fields.add(current.toString());
        return fields;
    }
}
