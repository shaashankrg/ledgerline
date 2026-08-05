package com.ledgerline.messaging;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;

import com.ledgerline.transfer.TransferException;
import com.ledgerline.transfer.TransferRequest;
import com.ledgerline.transfer.TransferResult;
import com.ledgerline.transfer.TransferService;

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
 * field) and every {@link TransferException} subclass:
 * SameAccountTransferException, AccountNotFoundException,
 * CurrencyMismatchException, AmountScaleException, and
 * IdempotencyKeyReuseException. These are properties of the message itself, so
 * redelivering it produces the same failure forever. Each goes to the dead
 * letter topic and is then acknowledged, which is what keeps one bad record
 * from stalling every record behind it on the partition.
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
    private final TransferService transferService;
    private final DeadLetterPublisher deadLetterPublisher;

    TransactionConsumer(TransactionMessageParser parser,
            TransferService transferService,
            DeadLetterPublisher deadLetterPublisher) {
        this.parser = parser;
        this.transferService = transferService;
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
            TransferRequest request = parser.parse(record.value());
            TransferResult result = transferService.transfer(request);
            logOutcome(record, result);
        } catch (PermanentMessageException | TransferException e) {
            deadLetter(record, e);
        }
        // Acknowledged only after the ledger write returned, or after the dead
        // letter was published. A transient failure throws past this line, so
        // the offset stays put and the record comes back.
        acknowledgment.acknowledge();
    }

    private void logOutcome(ConsumerRecord<String, String> record, TransferResult result) {
        if (result.replayed()) {
            // A redelivery that the ledger already holds. Expected under
            // at-least-once delivery, and a success rather than a problem.
            log.debug("Replayed transaction {} from offset {}", result.transactionId(), record.offset());
        } else {
            log.info("Recorded transaction {} from offset {}", result.transactionId(), record.offset());
        }
    }

    private void deadLetter(ConsumerRecord<String, String> record, RuntimeException failure) {
        deadLetterPublisher.publish(
                record.key(), record.value(), record.topic(), record.partition(), record.offset(), failure);
    }
}
