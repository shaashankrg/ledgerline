package com.ledgerline.messaging;

import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.MicrometerConsumerListener;
import org.springframework.kafka.listener.ContainerProperties.AckMode;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.util.backoff.FixedBackOff;

import io.micrometer.core.instrument.MeterRegistry;

/**
 * Consumer wiring for the {@code transactions} topic.
 *
 * The offset is committed by the listener, after the ledger write has
 * succeeded, and never by the client on a timer.
 */
@Configuration
class KafkaConsumerConfig {

    static final String GROUP_ID = "ledgerline-transactions";

    /** Gap between redelivery attempts for transient failures. */
    private static final long RETRY_INTERVAL_MS = 1_000L;

    @Bean
    ConsumerFactory<String, String> transactionConsumerFactory(
            @Value("${spring.kafka.bootstrap-servers}") String bootstrapServers,
            MeterRegistry meterRegistry) {

        Map<String, Object> config = new HashMap<>();
        config.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        config.put(ConsumerConfig.GROUP_ID_CONFIG, GROUP_ID);

        /*
         * Auto-commit is off, and that is the load-bearing setting here.
         *
         * With enable.auto.commit=true the client commits offsets on a timer,
         * independently of whether the record was actually handled. A record
         * polled at t=0 and still mid-write at t=5s has its offset committed by
         * the timer anyway; if the process then dies, the group resumes past a
         * transfer that was never written to the ledger. The message is gone
         * with nothing to signal it, because the commit said it was done.
         *
         * Committing manually after the write inverts that: a crash before the
         * acknowledgement means the record is redelivered and processed again,
         * which is safe because TransferService is idempotent on the
         * transaction id. At-least-once plus idempotent writes is a correct
         * ledger; at-most-once is a lost transfer.
         */
        config.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);

        // A new group reads the topic from the beginning rather than skipping
        // whatever was published before it first subscribed.
        config.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        /*
         * Values arrive as raw strings and are parsed inside the listener.
         *
         * A typed JSON deserializer would fail inside the client, before any
         * code of ours runs, which makes a malformed payload hard to route to
         * the dead letter topic -- the container would see a deserialization
         * error with no record body to forward. Deserializing by hand keeps the
         * original bytes available so a poison pill can be published to the DLT
         * exactly as it arrived.
         */
        config.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        config.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);

        DefaultKafkaConsumerFactory<String, String> factory = new DefaultKafkaConsumerFactory<>(config);

        // Registers each underlying Kafka client's own JMX-sourced metrics
        // (including the per-partition records-lag gauge the consumer-lag
        // dashboard panel reads) into the app's MeterRegistry -- this
        // consumer's factory is hand-built rather than Spring Boot's
        // autoconfigured one, so the usual auto-binding does not happen
        // without wiring the listener explicitly.
        factory.addListener(new MicrometerConsumerListener<>(meterRegistry));

        return factory;
    }

    @Bean
    ConcurrentKafkaListenerContainerFactory<String, String> transactionListenerContainerFactory(
            ConsumerFactory<String, String> transactionConsumerFactory,
            @Value("${ledgerline.consumer.concurrency:1}") int concurrency) {

        ConcurrentKafkaListenerContainerFactory<String, String> factory =
                new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(transactionConsumerFactory);

        // Each unit of concurrency is a separate consumer in the group, so this
        // is how many partitions can be worked in parallel. Capped by the
        // partition count: extra consumers beyond it sit idle.
        factory.setConcurrency(concurrency);

        /*
         * MANUAL_IMMEDIATE: the commit is issued the moment acknowledge() is
         * called, rather than being queued until the end of the poll batch.
         * With plain MANUAL, a crash between the ledger write and the batch
         * boundary would redeliver work that was already acknowledged in
         * memory -- harmless here given idempotency, but it makes the offset
         * lag the truth for longer than necessary.
         */
        factory.getContainerProperties().setAckMode(AckMode.MANUAL_IMMEDIATE);

        /*
         * Only transient failures reach here -- permanent ones are caught in the
         * listener and routed to the dead letter topic. So this retries
         * indefinitely rather than giving up after a fixed count: the failures
         * that arrive here are things like the database being unreachable, and
         * discarding a valid transfer because the outage outlasted an arbitrary
         * retry budget would lose money that was only ever waiting.
         *
         * Blocking the partition is the correct behaviour while the database is
         * down. Nothing behind this record could be written either, so
         * advancing past it would only convert a delay into a loss.
         */
        factory.setCommonErrorHandler(new DefaultErrorHandler(
                new FixedBackOff(RETRY_INTERVAL_MS, FixedBackOff.UNLIMITED_ATTEMPTS)));

        return factory;
    }
}
