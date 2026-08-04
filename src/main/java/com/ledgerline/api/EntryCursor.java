package com.ledgerline.api;

import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.Base64;

/**
 * Position in the entry listing, as an opaque page token.
 *
 * Holds both sort keys. Ordering on created_at alone is not stable, since
 * entries written in the same transaction share a timestamp to the microsecond:
 * a cursor carrying only the timestamp would either skip the rest of a tied
 * group or repeat it. The id breaks the tie and is unique, so (created_at, id)
 * identifies exactly one row and the page boundary is exact.
 *
 * Base64 is encoding, not secrecy -- it exists so clients treat the value as
 * opaque and do not build their own, which would couple them to the sort keys.
 */
record EntryCursor(Instant createdAt, long id) {

    private static final String SEPARATOR = ":";

    /** Thrown for anything that is not a token this class produced. */
    static class MalformedCursorException extends RuntimeException {
        MalformedCursorException(String message) {
            super(message);
        }
    }

    String encode() {
        // Epoch micros, matching the precision Postgres stores for TIMESTAMPTZ.
        long micros = createdAt.getEpochSecond() * 1_000_000L + createdAt.getNano() / 1_000L;
        String raw = micros + SEPARATOR + id;
        return Base64.getUrlEncoder().withoutPadding()
                .encodeToString(raw.getBytes(StandardCharsets.UTF_8));
    }

    /**
     * Parses a token, rejecting anything malformed.
     *
     * Every failure path throws the same exception so the handler can answer
     * 400 rather than letting a decode error surface as a 500.
     */
    static EntryCursor decode(String token) {
        String raw;
        try {
            raw = new String(Base64.getUrlDecoder().decode(token), StandardCharsets.UTF_8);
        } catch (IllegalArgumentException e) {
            throw new MalformedCursorException("cursor is not valid base64url");
        }

        String[] parts = raw.split(SEPARATOR);
        if (parts.length != 2) {
            throw new MalformedCursorException("cursor does not have the expected shape");
        }

        try {
            long micros = Long.parseLong(parts[0]);
            long id = Long.parseLong(parts[1]);
            Instant createdAt = Instant.ofEpochSecond(
                    Math.floorDiv(micros, 1_000_000L),
                    Math.floorMod(micros, 1_000_000L) * 1_000L);
            return new EntryCursor(createdAt, id);
        } catch (ArithmeticException | NumberFormatException e) {
            throw new MalformedCursorException("cursor does not contain a valid position");
        }
    }
}
