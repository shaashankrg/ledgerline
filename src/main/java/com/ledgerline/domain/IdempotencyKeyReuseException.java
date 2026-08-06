package com.ledgerline.domain;

/**
 * An idempotency key was reused for a materially different request.
 *
 * Distinct from a replay: a replay carries the same payload and returns the
 * original transaction. This means the caller reused a key for different money,
 * so honouring either interpretation would be wrong -- returning the original
 * silently drops the new transfer, and writing a new one breaks the key's
 * promise. Day 5 maps this to 422.
 */
public class IdempotencyKeyReuseException extends TransferException {

    public IdempotencyKeyReuseException(String idempotencyKey) {
        super("idempotency key " + idempotencyKey + " was already used for a different request");
    }
}
