package com.ledgerline.messaging;

import java.nio.charset.StandardCharsets;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

/**
 * Publishes messages that can never be processed to the dead letter topic.
 *
 * The original payload is forwarded byte for byte, so whatever arrives on the
 * DLT is exactly what the consumer choked on -- a re-encoded version would hide
 * the malformation that caused the failure in the first place.
 */
@Component
class DeadLetterPublisher {

    static final String DLT_TOPIC = TransactionProducer.TOPIC + ".DLT";

    /** Carries why the message was rejected, for whoever inspects the topic. */
    static final String REASON_HEADER = "x-failure-reason";
    static final String EXCEPTION_HEADER = "x-failure-class";
    static final String ORIGINAL_TOPIC_HEADER = "x-original-topic";
    static final String ORIGINAL_PARTITION_HEADER = "x-original-partition";
    static final String ORIGINAL_OFFSET_HEADER = "x-original-offset";

    private static final Logger log = LoggerFactory.getLogger(DeadLetterPublisher.class);

    private final KafkaTemplate<String, String> deadLetterKafkaTemplate;

    DeadLetterPublisher(KafkaTemplate<String, String> deadLetterKafkaTemplate) {
        this.deadLetterKafkaTemplate = deadLetterKafkaTemplate;
    }

    /**
     * Sends the payload to the DLT and blocks until the broker acknowledges.
     *
     * Blocking is deliberate: the caller acknowledges the original record
     * immediately after this returns, and acknowledging while the dead letter
     * is still in flight risks dropping the message entirely if the publish
     * then fails. If this throws, the record is left unacknowledged and will be
     * redelivered, which is the safe direction.
     */
    void publish(String key, String payload, String topic, int partition, long offset, Throwable failure) {
        ProducerRecord<String, String> record = new ProducerRecord<>(DLT_TOPIC, key, payload);

        String reason = failure.getMessage() == null ? failure.toString() : failure.getMessage();
        record.headers().add(REASON_HEADER, reason.getBytes(StandardCharsets.UTF_8));
        record.headers().add(EXCEPTION_HEADER, causeClassOf(failure).getBytes(StandardCharsets.UTF_8));
        record.headers().add(ORIGINAL_TOPIC_HEADER, topic.getBytes(StandardCharsets.UTF_8));
        record.headers().add(ORIGINAL_PARTITION_HEADER,
                String.valueOf(partition).getBytes(StandardCharsets.UTF_8));
        record.headers().add(ORIGINAL_OFFSET_HEADER, String.valueOf(offset).getBytes(StandardCharsets.UTF_8));

        try {
            deadLetterKafkaTemplate.send(record).get(30, TimeUnit.SECONDS);
            log.warn("Sent message to {} from {}-{} offset {}: {}",
                    DLT_TOPIC, topic, partition, offset, reason);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("interrupted while publishing to " + DLT_TOPIC, e);
        } catch (Exception e) {
            throw new IllegalStateException("could not publish to " + DLT_TOPIC, e);
        }
    }

    /** The underlying cause is more useful than our own wrapper type. */
    private static String causeClassOf(Throwable failure) {
        Throwable cause = failure.getCause() == null ? failure : failure.getCause();
        return cause.getClass().getName();
    }
}
