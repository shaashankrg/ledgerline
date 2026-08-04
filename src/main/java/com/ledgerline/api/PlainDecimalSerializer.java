package com.ledgerline.api;

import java.io.IOException;
import java.math.BigDecimal;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.SerializerProvider;

/**
 * Writes a monetary amount as a JSON string, pinned to the ledger's scale and
 * always in plain notation.
 *
 * BigDecimal.toString switches to scientific notation for some values
 * (1E+3 for 1000 with a negative scale), which is valid JSON but hostile to
 * clients doing exact decimal parsing. toPlainString never does. The explicit
 * setScale means every amount reads back with the same number of decimal places
 * the ledger stores, so 50 and 50.0000 are not two different-looking answers to
 * the same question.
 */
class PlainDecimalSerializer extends JsonSerializer<BigDecimal> {

    /** Matches ledger_entries.amount NUMERIC(19,4). */
    static final int LEDGER_SCALE = 4;

    @Override
    public void serialize(BigDecimal value, JsonGenerator gen, SerializerProvider serializers) throws IOException {
        // No rounding mode: amounts are validated against this scale before
        // they are stored, so this only ever pads.
        gen.writeString(value.setScale(LEDGER_SCALE).toPlainString());
    }
}
