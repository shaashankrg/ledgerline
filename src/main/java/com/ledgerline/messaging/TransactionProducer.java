package com.ledgerline.messaging;

import java.nio.charset.StandardCharsets;
import java.util.concurrent.CompletableFuture;

import org.apache.kafka.clients.producer.ProducerRecord;
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

    /**
     * Carries the producer's wall-clock time at publish, as epoch millis, so
     * {@link TransactionConsumer} can measure end-to-end latency at DB commit.
     *
     * A header, not a payload field: the wire contract in {@link
     * TransactionMessage} is what a consumer's business logic reads, and this
     * value is purely an observability artifact that has no business meaning
     * to a consumer that isn't measuring latency.
     *
     * Single-host clock caveat: this producer and the consumer that reads the
     * header run on the same machine in this project's setup, so
     * {@code System.currentTimeMillis()} here and there are comparable without
     * any clock synchronization. That stops being true the moment producer and
     * consumer run on different hosts -- NTP drift between machines would then
     * leak directly into the reported latency, indistinguishable from real
     * processing time. Volunteering this now because it's the kind of
     * limitation a reviewer of a multi-host deployment would otherwise have to
     * find for themselves.
     */
    static final String PRODUCED_AT_HEADER = "x-produced-at-epoch-millis";

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
        ProducerRecord<String, TransactionMessage> record =
                new ProducerRecord<>(TOPIC, message.transactionId(), message);
        record.headers().add(PRODUCED_AT_HEADER,
                Long.toString(System.currentTimeMillis()).getBytes(StandardCharsets.UTF_8));
        return kafkaTemplate.send(record);
    }
}
