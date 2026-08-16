package com.ledgerline.messaging;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.List;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.header.Header;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;

import com.ledgerline.domain.IllegalTransitionException;
import com.ledgerline.domain.TransactionEvent;
import com.ledgerline.domain.TransferException;
import com.ledgerline.metrics.LedgerlineMetrics;
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
    private final LedgerlineMetrics metrics;

    TransactionConsumer(TransactionMessageParser parser,
            TransactionEventService eventService,
            DeadLetterPublisher deadLetterPublisher,
            LedgerlineMetrics metrics) {
        this.parser = parser;
        this.eventService = eventService;
        this.deadLetterPublisher = deadLetterPublisher;
        this.metrics = metrics;
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
            recordEndToEndLatency(record, events);
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

    /**
     * Threshold above which a write is logged as landing during an
     * "extended catch-up window" rather than ordinary processing.
     *
     * Day 11 instrumentation (see docs/known-limitations.md's 2026-08-16
     * torn-ledger-write entry): the one confirmed data-integrity incident
     * from Day 10 landed 173s after its run's generation began, during
     * smooth/undisturbed consumer traffic with no gap around it -- not
     * adjacent to a pod kill. The investigation could not pin an exact
     * kill-to-write interval because nothing was logging publish-to-commit
     * latency at the time a write with unusually high latency actually
     * happened; this fixes that gap going forward. 5s is comfortably above
     * normal end-to-end latency under load (single-digit milliseconds per
     * the offset-application log cadence observed throughout this
     * project's chaos runs) and comfortably below "the consumer group is
     * still mid-rebalance" territory, so it flags a write that took an
     * unusually long time to arrive without being noise on every ordinary
     * message.
     */
    private static final Duration EXTENDED_CATCH_UP_THRESHOLD = Duration.ofSeconds(5);

    /**
     * Measures publish-to-commit latency using the header {@link
     * TransactionProducer} stamped at send time.
     *
     * Read here rather than in the parser: {@code applyAll} returning
     * successfully is the actual "committed" instant this timer measures --
     * reading the header before that call would measure "received", which
     * every consumer already gets for free from Kafka's own consumer lag
     * metric, and would not be this project's own claim.
     *
     * Missing or unparseable header is tolerated silently: it means the
     * message did not come from {@link TransactionProducer} (e.g. it was
     * published by a test harness or an external producer), which is not an
     * error condition for this listener, just something outside what this
     * timer can measure.
     */
    private void recordEndToEndLatency(ConsumerRecord<String, String> record, List<TransactionEvent> events) {
        Header header = record.headers().lastHeader(TransactionProducer.PRODUCED_AT_HEADER);
        if (header == null) {
            return;
        }
        try {
            long producedAtEpochMillis = Long.parseLong(new String(header.value(), StandardCharsets.UTF_8));
            Duration latency = Duration.between(Instant.ofEpochMilli(producedAtEpochMillis), Instant.now());
            metrics.recordEndToEndLatency(latency);
            if (latency.compareTo(EXTENDED_CATCH_UP_THRESHOLD) > 0) {
                // Not itself evidence of anything wrong -- most extended-
                // catch-up writes are entirely benign, exactly like the
                // vast majority of writes during Day 10's own catch-up
                // tails were. The point is only that if a torn write ever
                // recurs, whoever investigates it can grep for this line
                // near the affected transaction's externalTxnId and
                // immediately know it landed during exactly this kind of
                // window, rather than reconstructing that fact after the
                // evidence has aged out of Kubernetes' event retention (as
                // happened investigating the Day 10 incident).
                String externalTxnIds = events.stream()
                        .map(TransactionEvent::externalTxnId)
                        .distinct()
                        .reduce((a, b) -> a + "," + b)
                        .orElse("");
                log.debug("Extended catch-up window write: offset={} latency={} externalTxnIds={}",
                        record.offset(), latency, externalTxnIds);
            }
        } catch (NumberFormatException e) {
            log.debug("Unparseable {} header at offset {}, skipping latency measurement",
                    TransactionProducer.PRODUCED_AT_HEADER, record.offset());
        }
    }
}
