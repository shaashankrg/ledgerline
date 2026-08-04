package com.ledgerline.messaging;

import java.io.IOException;
import java.math.BigDecimal;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.SerializerProvider;

/**
 * Writes a monetary amount as a JSON string, pinned to the ledger's scale and
 * always in plain notation.
 *
 * Deliberately a sibling of the API layer's serializer rather than a shared
 * class: the two happen to agree today, but the wire format of a message and
 * the wire format of an HTTP response are separate contracts with separate
 * consumers. Coupling them would mean a change made for an HTTP client
 * silently reshaped every message already on the topic.
 *
 * BigDecimal.toString switches to scientific notation for some values, which is
 * valid JSON but hostile to exact decimal parsing; toPlainString never does.
 */
class PlainDecimalMessageSerializer extends JsonSerializer<BigDecimal> {

    /** Matches ledger_entries.amount NUMERIC(19,4). */
    static final int LEDGER_SCALE = 4;

    @Override
    public void serialize(BigDecimal value, JsonGenerator gen, SerializerProvider serializers) throws IOException {
        // No rounding mode: an amount carrying more scale than the ledger can
        // store is a bug upstream, and throwing here is better than quietly
        // publishing a rounded number.
        gen.writeString(value.setScale(LEDGER_SCALE).toPlainString());
    }
}
