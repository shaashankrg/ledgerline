package com.ledgerline.api;

import java.math.BigDecimal;

import com.fasterxml.jackson.annotation.JsonFormat;

import jakarta.validation.constraints.Digits;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Positive;

/**
 * The POST /api/v1/transfers request body.
 *
 * Carries no idempotency key: that arrives in the Idempotency-Key header. The
 * body is exactly the material the payload hash is taken over, so a key can
 * never be hashed into its own lookup value.
 */
public record TransferRequestBody(

        @NotNull(message = "fromAccountId is required")
        Long fromAccountId,

        @NotNull(message = "toAccountId is required")
        Long toAccountId,

        /*
         * STRING shape, not a JSON number. A client sending 1234567.89 as a bare
         * JSON number has already handed it to a double parser before it reaches
         * us; accepting only a string keeps the decimal exact end to end.
         */
        /*
         * Scale is a shape rule, so it is checked here: NUMERIC(19,4) is a fact
         * about the message format, knowable without reading any state. Whether
         * the accounts exist and their currencies agree is not -- that needs the
         * database, and belongs to the consumer.
         */
        @NotNull(message = "amount is required")
        @Positive(message = "amount must be positive")
        @Digits(integer = 15, fraction = 4,
                message = "amount must have at most 4 decimal places")
        @JsonFormat(shape = JsonFormat.Shape.STRING)
        BigDecimal amount,

        @NotBlank(message = "currency is required")
        String currency) {
}
