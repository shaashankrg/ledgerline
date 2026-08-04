package com.ledgerline.api;

import java.net.URI;

import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.validation.annotation.Validated;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import com.ledgerline.transfer.TransferRequest;
import com.ledgerline.transfer.TransferResult;
import com.ledgerline.transfer.TransferService;

import jakarta.validation.Valid;
import jakarta.validation.constraints.NotBlank;

/**
 * HTTP adapter over {@link TransferService}.
 *
 * Deliberately thin: it reads the header and body, hands them to the service,
 * and shapes the result. No validation, transaction handling, or idempotency
 * logic lives here -- all of that belongs to the service, which is also what
 * makes the service usable from a non-HTTP caller later.
 */
@RestController
@RequestMapping("/api/v1/transfers")
@Validated // makes @NotBlank on the header parameter take effect
class TransferController {

    /** Signals to the client that this response replays an earlier request. */
    static final String REPLAY_HEADER = "Idempotent-Replay";

    private final TransferService transferService;

    TransferController(TransferService transferService) {
        this.transferService = transferService;
    }

    /**
     * Records a transfer, or replays one already recorded under the same key.
     *
     * A replay returns the same 201 and the same body as the original. Returning
     * a different status for a retry would force every client to branch on it,
     * which defeats the point of the request being idempotent.
     */
    @PostMapping
    ResponseEntity<TransferResponseBody> transfer(
            @RequestHeader(name = "Idempotency-Key")
            @NotBlank(message = "Idempotency-Key header must not be blank")
            String idempotencyKey,
            @Valid @RequestBody TransferRequestBody body) {

        TransferResult result = transferService.transfer(new TransferRequest(
                idempotencyKey,
                body.fromAccountId(),
                body.toAccountId(),
                body.amount(),
                body.currency()));

        TransferResponseBody responseBody = new TransferResponseBody(
                result.transactionId(),
                body.fromAccountId(),
                body.toAccountId(),
                body.amount(),
                body.currency());

        ResponseEntity.BodyBuilder response = ResponseEntity
                .created(URI.create("/api/v1/transfers/" + result.transactionId()));

        if (result.replayed()) {
            response.header(REPLAY_HEADER, "true");
        }

        return response.contentType(MediaType.APPLICATION_JSON).body(responseBody);
    }
}
