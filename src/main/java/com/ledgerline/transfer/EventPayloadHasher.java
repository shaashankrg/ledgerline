package com.ledgerline.transfer;

import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HexFormat;
import java.util.Locale;

/**
 * Hashes an event's payload so a redelivery can be told from a key reuse.
 *
 * Carried over from the retired transfer path with the canonical form intact:
 * fixed field order, amount normalized to the ledger's scale, currency upper
 * cased. Two logically identical events therefore hash identically however the
 * producer formatted them, so a retry that reformats its own payload -- "50.0"
 * against "50.00" -- still reads as the same event rather than a conflict.
 *
 * The event type is part of the hash now. Under the transfer model a key
 * identified one movement; under the event model an eventId identifies one step
 * of a lifecycle, and a capture and a refund for the same amount are different
 * things that must not hash alike.
 */
final class EventPayloadHasher {

    /** Matches ledger_entries.amount NUMERIC(19,4). */
    static final int LEDGER_SCALE = 4;

    private static final char FIELD_SEPARATOR = '|';

    private EventPayloadHasher() {
    }

    /** @return the hash as 64 lowercase hex characters, matching CHAR(64) */
    static String hash(com.ledgerline.domain.TransactionEvent event) {
        // Separated rather than concatenated: without a delimiter, accounts
        // (1, 23) and (12, 3) would produce the same byte sequence.
        StringBuilder canonical = new StringBuilder()
                .append(event.externalTxnId()).append(FIELD_SEPARATOR)
                .append(event.type().name()).append(FIELD_SEPARATOR)
                .append(event.fromAccountId()).append(FIELD_SEPARATOR)
                .append(event.toAccountId()).append(FIELD_SEPARATOR)
                .append(event.amount() == null
                        ? "" : normalizedAmount(event.amount()).toPlainString())
                .append(FIELD_SEPARATOR)
                .append(event.currency() == null
                        ? "" : event.currency().toUpperCase(Locale.ROOT));

        return sha256Hex(canonical.toString());
    }

    /**
     * Normalizes an amount to the ledger's scale.
     *
     * setScale with no rounding mode throws if the value would lose precision,
     * which cannot happen here: the validator rejects over-scale amounts before
     * hashing, so this only ever pads (50.0 -> 50.0000).
     */
    static BigDecimal normalizedAmount(BigDecimal amount) {
        return amount.setScale(LEDGER_SCALE);
    }

    private static String sha256Hex(String canonical) {
        try {
            MessageDigest digest = MessageDigest.getInstance("SHA-256");
            return HexFormat.of().formatHex(digest.digest(canonical.getBytes(StandardCharsets.UTF_8)));
        } catch (NoSuchAlgorithmException e) {
            // SHA-256 is required of every JVM, so this is unreachable.
            throw new IllegalStateException("SHA-256 unavailable", e);
        }
    }
}
