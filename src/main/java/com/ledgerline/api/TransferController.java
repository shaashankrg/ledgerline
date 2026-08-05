package com.ledgerline.api;

import java.net.URI;
import java.util.concurrent.TimeUnit;

import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.validation.annotation.Validated;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import com.ledgerline.messaging.TransactionMessage;
import com.ledgerline.messaging.TransactionProducer;

import jakarta.validation.Valid;
import jakarta.validation.constraints.NotBlank;

/**
 * Intake for transfers. Publishes to Kafka and writes nothing.
 *
 * The endpoint no longer touches the database. Writing to Postgres here and
 * publishing to Kafka would be a dual write: two systems, no shared
 * transaction, so a crash between them leaves the ledger and the topic
 * disagreeing with no way to tell which is right. Publishing only, and letting
 * the consumer be the sole writer, removes that failure entirely -- there is
 * one system of record and one path into it.
 *
 * Validation here is shape only: whether the JSON parses, the fields are
 * present, the amount is positive and fits the ledger's scale. Everything that
 * needs state -- the accounts existing, their currencies agreeing -- is checked
 * by TransferService when the consumer processes the message. That is a real
 * consequence, not an oversight: a request naming a nonexistent account is
 * accepted with 202 and fails later into the dead letter topic, because at
 * intake time there is nothing to check it against.
 */
@RestController
@RequestMapping("/api/v1/transfers")
@Validated // makes @NotBlank on the header parameter take effect
class TransferController {

    /** How long to wait for the broker to acknowledge before giving up. */
    private static final long PUBLISH_TIMEOUT_SECONDS = 10;

    private final TransactionProducer transactionProducer;

    TransferController(TransactionProducer transactionProducer) {
        this.transactionProducer = transactionProducer;
    }

    /**
     * Accepts a transfer for processing.
     *
     * Returns 202, not 201: nothing has been created yet. The ledger entries
     * appear once the consumer processes the message, so claiming creation here
     * would be a lie the client could observe by immediately reading back.
     *
     * There is no replay header any more. The producer holds no state and
     * cannot know whether this key was used before -- only the consumer, which
     * has the database, can tell a retry from a first attempt. A duplicate
     * submission publishes a second message and is deduplicated downstream, so
     * both requests get an identical 202 and the client never has to branch.
     */
    @PostMapping
    ResponseEntity<TransferResponseBody> transfer(
            @RequestHeader(name = "Idempotency-Key")
            @NotBlank(message = "Idempotency-Key header must not be blank")
            String idempotencyKey,
            @Valid @RequestBody TransferRequestBody body) {

        TransactionMessage message = new TransactionMessage(
                idempotencyKey,
                body.fromAccountId(),
                body.toAccountId(),
                body.amount(),
                body.currency());

        publish(message);

        TransferResponseBody responseBody = new TransferResponseBody(
                idempotencyKey,
                body.fromAccountId(),
                body.toAccountId(),
                body.amount(),
                body.currency());

        return ResponseEntity
                .accepted()
                .location(URI.create("/api/v1/transfers/" + idempotencyKey))
                .contentType(MediaType.APPLICATION_JSON)
                .body(responseBody);
    }

    /**
     * Waits for the broker to acknowledge before responding.
     *
     * Blocking rather than fire-and-forget: a 202 has to mean the message is
     * durably on the topic. Returning before the acknowledgement would tell the
     * client the transfer was accepted when it might still be lost, which is
     * precisely the ambiguity this design exists to remove.
     */
    private void publish(TransactionMessage message) {
        try {
            transactionProducer.publish(message).get(PUBLISH_TIMEOUT_SECONDS, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new TransferPublishException(e);
        } catch (Exception e) {
            throw new TransferPublishException(e);
        }
    }

    /**
     * The message could not be published.
     *
     * Maps to 503. Nothing was written anywhere, so the client can retry the
     * same request with the same key safely -- which is the whole reason the
     * endpoint does not write to the database first.
     */
    static class TransferPublishException extends RuntimeException {
        TransferPublishException(Throwable cause) {
            super("could not publish the transfer", cause);
        }
    }
}
