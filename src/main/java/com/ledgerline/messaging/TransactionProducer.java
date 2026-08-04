package com.ledgerline.messaging;

import java.util.concurrent.CompletableFuture;

import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Component;

/**
 * Publishes transactions to the {@code transactions} topic.
 *
 * Standalone for now: nothing calls this from the HTTP path, and the transfer
 * endpoint behaves exactly as it did before. Wiring it into the write path
 * raises questions this does not yet answer -- chiefly that a database commit
 * and a Kafka publish are two separate systems, so one can succeed while the
 * other fails, and deciding what that means is the work of the consumer day.
 */
@Component
public class TransactionProducer {

    static final String TOPIC = "transactions";

    private final KafkaTemplate<String, TransactionMessage> kafkaTemplate;

    TransactionProducer(KafkaTemplate<String, TransactionMessage> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    /**
     * Publishes a transaction, keyed by its transaction id.
     *
     * The key determines the partition, so every message for a given
     * transaction id lands on the same partition and stays in order relative to
     * the others sharing it. That ordering is per-partition in Kafka and does
     * not hold across partitions, which is why the key matters even though the
     * topic currently has only one.
     *
     * @return a future completing when the broker has acknowledged the record;
     *         with acks=all that means every in-sync replica holds it
     */
    public CompletableFuture<SendResult<String, TransactionMessage>> publish(TransactionMessage message) {
        return kafkaTemplate.send(TOPIC, message.transactionId(), message);
    }
}
