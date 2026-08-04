package com.ledgerline.transfer;

/**
 * Base type for transfer rejections, so Day 5's HTTP layer can catch one type
 * and map subclasses to status codes.
 *
 * Unchecked because every subclass signals a caller mistake that cannot be
 * recovered from in the service, and because a checked exception thrown inside
 * a {@code @Transactional} method would still roll back the same way.
 */
public abstract class TransferException extends RuntimeException {

    protected TransferException(String message) {
        super(message);
    }
}
