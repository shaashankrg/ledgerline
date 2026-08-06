package com.ledgerline.messaging;

import java.util.List;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;

import com.ledgerline.domain.IllegalTransitionException;
import com.ledgerline.domain.TransactionEvent;
import com.ledgerline.domain.TransferException;
import com.ledgerline.transfer.TransactionEventService;
import com.ledgerline.transfer.TransactionEventService.EventResult;

/**
 * Writes transactions from the {@code transactions} topic into the ledger.
 *
 * The listener does three things: parse, transfer, acknowledge. Validation,
 * duplicate detection, and the database transaction all belong to
 * TransferService and are not repeated here -- the consumer is a delivery
 * mechanism, and a second copy of those rules would be a second thing to keep
 * in step.
 *
 * Failure handling splits two ways:
 *
 * PERMANENT -- {@link PermanentMessageException} (unparseable payload, missing
 * field), every {@link TransferException} subclass (SameAccountTransferException,
 * AccountNotFoundException, CurrencyMismatchException, AmountScaleException,
 * IdempotencyKeyReuseException), and {@link IllegalTransitionException}. These
 * are properties of the message itself, so redelivering it produces the same
 * failure forever. Each goes to the dead letter topic and is then acknowledged,
 * which is what keeps one bad record from stalling every record behind it on
 * the partition.
 *
 * IllegalTransitionException belongs here rather than with the retryable
 * failures because a transaction's state only moves forward: an event that
 * cannot follow the current state will not become legal later. Note this is
 * distinct from TransitionConflictException, which means another writer won a
 * race and is therefore worth retrying -- it is deliberately absent from this
 * list and falls through to the transient path.
 *
 * PARKED -- an event that arrived before its authorize never reaches this
 * handler at all. TransactionEventService stores it in parked_events and
 * returns normally, so the record is acknowledged like any success. That is
 * deliberate: the event is not lost by advancing the offset, because the
 * parked table now holds it, and redelivering it would only park it again.
 * Routing these to the dead letter topic instead would discard perfectly good
 * transfers for the crime of being early, which a partitioned topic with
 * retries produces as a matter of course.
 *
 * IdempotencyKeyReuseException is permanent for a subtler reason than the
 * others: the key was already used for a different payload, and no amount of
 * retrying will make the ledger accept a second meaning for a key it has
 * already committed to.
 *
 * TRANSIENT -- anything else, principally the database being unreachable. The
 * record is left unacknowledged and the exception propagates, so the container
 * redelivers it after a backoff. Sending these to the DLT would discard a
 * transfer that was only ever going to need a second attempt.
 */
@Component
class TransactionConsumer {

    private static final Logger log = LoggerFactory.getLogger(TransactionConsumer.class);

    private final TransactionMessageParser parser;
    private final TransactionEventService eventService;
    private final DeadLetterPublisher deadLetterPublisher;

    TransactionConsumer(TransactionMessageParser parser,
            TransactionEventService eventService,
            DeadLetterPublisher deadLetterPublisher) {
        this.parser = parser;
        this.eventService = eventService;
        this.deadLetterPublisher = deadLetterPublisher;
    }

    @KafkaListener(
            topics = TransactionProducer.TOPIC,
            groupId = KafkaConsumerConfig.GROUP_ID,
            containerFactory = "transactionListenerContainerFactory",
            // Always on in production. The flag exists so a test exercising the
            // intake path alone does not have to run a broker for a consumer it
            // never uses.
            autoStartup = "${ledgerline.consumer.enabled:true}")
    void consume(ConsumerRecord<String, String> record, Acknowledgment acknowledgment) {
        try {
            List<TransactionEvent> events = parser.parse(record.value());
            EventResult result = eventService.applyAll(events);
            logOutcome(record, result);
        } catch (PermanentMessageException | TransferException | IllegalTransitionException e) {
            deadLetter(record, e);
        }
        // Acknowledged only after the ledger write returned, or after the dead
        // letter was published. A transient failure throws past this line, so
        // the offset stays put and the record comes back.
        acknowledgment.acknowledge();
    }

    private void logOutcome(ConsumerRecord<String, String> record, EventResult result) {
        if (result.parked()) {
            // Arrived before its authorize and is now stored, waiting. The
            // record is acknowledged: redelivering it would park it again and
            // achieve nothing, and the offset advancing does not lose the
            // event because parked_events holds it.
            log.debug("Parked event at offset {}, awaiting its authorize", record.offset());
        } else if (result.replayed()) {
            // A redelivery the ledger already holds. Expected under
            // at-least-once delivery, and a success rather than a problem.
            log.debug("Replayed event at offset {}, transaction now {}",
                    record.offset(), result.state());
        } else {
            log.info("Applied event at offset {}, transaction now {}",
                    record.offset(), result.state());
        }
    }

    private void deadLetter(ConsumerRecord<String, String> record, RuntimeException failure) {
        deadLetterPublisher.publish(
                record.key(), record.value(), record.topic(), record.partition(), record.offset(), failure);
    }
}
