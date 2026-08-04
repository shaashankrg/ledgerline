package com.ledgerline.api;

import java.net.URI;

/**
 * Stable {@code type} URIs for RFC 9457 problem responses.
 *
 * Clients switch on these rather than parsing messages, so they are part of the
 * API contract: a value here may be added to, but changing or removing one is a
 * breaking change. They are deliberately not resolvable URLs -- RFC 9457 only
 * requires a stable identifier.
 */
public final class ErrorTypes {

    private static final String BASE = "https://ledgerline.example/problems/";

    /** Request body or header failed field-level validation. */
    public static final URI VALIDATION_FAILED = URI.create(BASE + "validation-failed");

    /** Source and destination accounts are the same. */
    public static final URI SAME_ACCOUNT = URI.create(BASE + "same-account-transfer");

    /** Declared currency does not match an account's currency. */
    public static final URI CURRENCY_MISMATCH = URI.create(BASE + "currency-mismatch");

    /** Amount carries more decimal places than the ledger stores. */
    public static final URI AMOUNT_SCALE = URI.create(BASE + "amount-scale");

    /** A referenced account does not exist. */
    public static final URI ACCOUNT_NOT_FOUND = URI.create(BASE + "account-not-found");

    /** Idempotency key was reused for a materially different request. */
    public static final URI IDEMPOTENCY_KEY_REUSE = URI.create(BASE + "idempotency-key-reuse");

    /** Request body was not well-formed JSON, or a field had the wrong type. */
    public static final URI MALFORMED_REQUEST = URI.create(BASE + "malformed-request");

    /** The addressed resource does not exist. */
    public static final URI RESOURCE_NOT_FOUND = URI.create(BASE + "resource-not-found");

    /** Pagination cursor was not a token this service issued. */
    public static final URI MALFORMED_CURSOR = URI.create(BASE + "malformed-cursor");

    /** Anything unhandled. Carries a correlation id and no detail. */
    public static final URI INTERNAL_ERROR = URI.create(BASE + "internal-error");

    private ErrorTypes() {
    }
}
