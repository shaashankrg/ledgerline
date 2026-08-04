package com.ledgerline.transfer;

import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HexFormat;
import java.util.Locale;

/**
 * Hashes a transfer request so a retry can be told apart from a key reuse.
 *
 * The hash is taken over a canonical form: fixed field order, amount normalized
 * to the ledger's scale, currency upper-cased. Two logically identical requests
 * therefore hash identically no matter how the caller formatted them -- "50.0"
 * and "50.00" are the same transfer, and a retry that reformats its own payload
 * must not read as a conflict.
 */
final class PayloadHasher {

    /** Matches ledger_entries.amount NUMERIC(19,4). */
    static final int LEDGER_SCALE = 4;

    private static final char FIELD_SEPARATOR = '|';

    private PayloadHasher() {
    }

    /**
     * @param request a request whose amount scale has already been validated
     * @return the hash as 64 lowercase hex characters, matching CHAR(64)
     */
    static String hash(TransferRequest request) {
        // Separated rather than concatenated: without a delimiter, accounts
        // (1, 23) and (12, 3) would produce the same byte sequence.
        String canonical = new StringBuilder()
                .append(request.fromAccountId()).append(FIELD_SEPARATOR)
                .append(request.toAccountId()).append(FIELD_SEPARATOR)
                .append(normalizedAmount(request.amount()).toPlainString()).append(FIELD_SEPARATOR)
                .append(request.currency().toUpperCase(Locale.ROOT))
                .toString();

        return sha256Hex(canonical);
    }

    /**
     * Normalizes an amount to the ledger's scale.
     *
     * setScale with no rounding mode throws if the value would lose precision,
     * which cannot happen here: the service rejects over-scale amounts before
     * hashing, so this only ever pads (50.0 -> 50.0000).
     */
    static BigDecimal normalizedAmount(BigDecimal amount) {
        return amount.setScale(LEDGER_SCALE);
    }

    private static String sha256Hex(String canonical) {
        try {
            MessageDigest digest = MessageDigest.getInstance("SHA-256");
            byte[] hashed = digest.digest(canonical.getBytes(StandardCharsets.UTF_8));
            return HexFormat.of().formatHex(hashed);
        } catch (NoSuchAlgorithmException e) {
            // SHA-256 is required of every JVM, so this is unreachable.
            throw new IllegalStateException("SHA-256 unavailable", e);
        }
    }
}
