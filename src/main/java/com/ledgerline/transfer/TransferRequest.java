package com.ledgerline.transfer;

import java.math.BigDecimal;

import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Positive;

/**
 * A request to move money between two accounts.
 *
 * The annotations here cover only what is expressible as a field-level rule.
 * Anything needing a database lookup or cross-field comparison -- the accounts
 * existing, their currencies agreeing, the two ids differing -- is enforced in
 * {@link TransferService}.
 */
public record TransferRequest(

        @NotBlank(message = "idempotencyKey is required")
        String idempotencyKey,

        @NotNull(message = "fromAccountId is required")
        Long fromAccountId,

        @NotNull(message = "toAccountId is required")
        Long toAccountId,

        @NotNull(message = "amount is required")
        @Positive(message = "amount must be positive")
        BigDecimal amount,

        @NotNull(message = "currency is required")
        String currency) {
}
