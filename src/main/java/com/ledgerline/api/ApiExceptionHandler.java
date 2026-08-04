package com.ledgerline.api;

import java.net.URI;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.TreeMap;
import java.util.UUID;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpStatus;
import org.springframework.http.ProblemDetail;
import org.springframework.http.converter.HttpMessageNotReadableException;
import org.springframework.web.bind.MethodArgumentNotValidException;
import org.springframework.web.bind.MissingRequestHeaderException;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;

import com.ledgerline.transfer.AccountNotFoundException;
import com.ledgerline.transfer.AmountScaleException;
import com.ledgerline.transfer.CurrencyMismatchException;
import com.ledgerline.transfer.IdempotencyKeyReuseException;
import com.ledgerline.transfer.SameAccountTransferException;

import jakarta.validation.ConstraintViolationException;

/**
 * Maps exceptions to RFC 9457 problem responses.
 *
 * Every problem carries a stable {@code type} URI so clients can switch on the
 * kind of failure without string-matching human-readable messages.
 */
@RestControllerAdvice
class ApiExceptionHandler {

    private static final Logger log = LoggerFactory.getLogger(ApiExceptionHandler.class);

    /** Field-level failures on the request body. */
    @ExceptionHandler(MethodArgumentNotValidException.class)
    ProblemDetail handleBodyValidation(MethodArgumentNotValidException e) {
        // Sorted so the same set of violations always renders identically.
        Map<String, String> errors = new TreeMap<>();
        e.getBindingResult().getFieldErrors()
                .forEach(error -> errors.put(error.getField(), error.getDefaultMessage()));

        ProblemDetail problem = problem(HttpStatus.BAD_REQUEST, ErrorTypes.VALIDATION_FAILED,
                "Validation failed", "One or more fields are invalid.");
        problem.setProperty("errors", errors);
        return problem;
    }

    /** Field-level failures on method parameters, such as a blank header. */
    @ExceptionHandler(ConstraintViolationException.class)
    ProblemDetail handleParameterValidation(ConstraintViolationException e) {
        Map<String, String> errors = new TreeMap<>();
        e.getConstraintViolations().forEach(violation -> {
            String path = violation.getPropertyPath().toString();
            // "transfer.idempotencyKey" -> "idempotencyKey"
            String field = path.substring(path.lastIndexOf('.') + 1);
            errors.put(field, violation.getMessage());
        });

        ProblemDetail problem = problem(HttpStatus.BAD_REQUEST, ErrorTypes.VALIDATION_FAILED,
                "Validation failed", "One or more request parameters are invalid.");
        problem.setProperty("errors", errors);
        return problem;
    }

    /** The Idempotency-Key header was absent entirely. */
    @ExceptionHandler(MissingRequestHeaderException.class)
    ProblemDetail handleMissingHeader(MissingRequestHeaderException e) {
        ProblemDetail problem = problem(HttpStatus.BAD_REQUEST, ErrorTypes.VALIDATION_FAILED,
                "Validation failed", "Required header '" + e.getHeaderName() + "' is missing.");
        problem.setProperty("errors", Map.of(e.getHeaderName(), "header is required"));
        return problem;
    }

    /**
     * Body was not well-formed JSON, or a field could not be coerced.
     *
     * The parser's own message is not echoed: it can quote arbitrary request
     * content and names internal types.
     */
    @ExceptionHandler(HttpMessageNotReadableException.class)
    ProblemDetail handleMalformedBody(HttpMessageNotReadableException e) {
        log.debug("Malformed request body", e);
        return problem(HttpStatus.BAD_REQUEST, ErrorTypes.MALFORMED_REQUEST,
                "Malformed request", "The request body could not be parsed as valid JSON.");
    }

    @ExceptionHandler(SameAccountTransferException.class)
    ProblemDetail handleSameAccount(SameAccountTransferException e) {
        return problem(HttpStatus.BAD_REQUEST, ErrorTypes.SAME_ACCOUNT,
                "Invalid transfer", e.getMessage());
    }

    @ExceptionHandler(CurrencyMismatchException.class)
    ProblemDetail handleCurrencyMismatch(CurrencyMismatchException e) {
        return problem(HttpStatus.BAD_REQUEST, ErrorTypes.CURRENCY_MISMATCH,
                "Currency mismatch", e.getMessage());
    }

    @ExceptionHandler(AmountScaleException.class)
    ProblemDetail handleAmountScale(AmountScaleException e) {
        return problem(HttpStatus.BAD_REQUEST, ErrorTypes.AMOUNT_SCALE,
                "Invalid amount", e.getMessage());
    }

    /**
     * 422 rather than 400: the request is syntactically fine and the client
     * cannot fix it by reformatting -- the referenced account does not exist.
     */
    @ExceptionHandler(AccountNotFoundException.class)
    ProblemDetail handleAccountNotFound(AccountNotFoundException e) {
        return problem(HttpStatus.UNPROCESSABLE_ENTITY, ErrorTypes.ACCOUNT_NOT_FOUND,
                "Account not found", e.getMessage());
    }

    @ExceptionHandler(IdempotencyKeyReuseException.class)
    ProblemDetail handleKeyReuse(IdempotencyKeyReuseException e) {
        return problem(HttpStatus.UNPROCESSABLE_ENTITY, ErrorTypes.IDEMPOTENCY_KEY_REUSE,
                "Idempotency key reuse", e.getMessage());
    }

    /**
     * 404 rather than the transfer layer's 422: on a read the account is the
     * resource being addressed, so its absence is a missing resource rather
     * than an unprocessable request.
     */
    @ExceptionHandler(AccountReadController.AccountNotFoundInReadException.class)
    ProblemDetail handleAccountMissingOnRead(AccountReadController.AccountNotFoundInReadException e) {
        return problem(HttpStatus.NOT_FOUND, ErrorTypes.RESOURCE_NOT_FOUND,
                "Account not found", e.getMessage());
    }

    /**
     * A cursor the service did not issue is a client error, not a server one --
     * without this it would surface as a decode failure and a 500.
     */
    @ExceptionHandler(EntryCursor.MalformedCursorException.class)
    ProblemDetail handleMalformedCursor(EntryCursor.MalformedCursorException e) {
        return problem(HttpStatus.BAD_REQUEST, ErrorTypes.MALFORMED_CURSOR,
                "Malformed cursor", e.getMessage());
    }

    /**
     * Catch-all.
     *
     * The response carries a correlation id and nothing else. Exception
     * messages, stack traces, SQL, constraint names, and table names all
     * describe the internals of this service, and an unhandled failure is
     * exactly the case where an attacker learns the most from them. Full detail
     * goes to the log against the same id, so an operator can still find it.
     */
    @ExceptionHandler(Exception.class)
    ProblemDetail handleUnexpected(Exception e) {
        String correlationId = UUID.randomUUID().toString();
        log.error("Unhandled exception, correlationId={}", correlationId, e);

        ProblemDetail problem = problem(HttpStatus.INTERNAL_SERVER_ERROR, ErrorTypes.INTERNAL_ERROR,
                "Internal server error",
                "The request could not be completed. Quote the correlation id when reporting this.");
        problem.setProperty("correlationId", correlationId);
        return problem;
    }

    private static ProblemDetail problem(HttpStatus status, URI type, String title, String detail) {
        ProblemDetail problem = ProblemDetail.forStatus(status);
        problem.setType(type);
        problem.setTitle(title);
        problem.setDetail(detail);
        // Insertion-ordered so the rendered JSON is stable across responses.
        problem.setProperties(new LinkedHashMap<>());
        return problem;
    }
}
