package com.ledgerline.messaging;

import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.support.serializer.JsonSerializer;

import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * Producer wiring for the {@code transactions} topic.
 *
 * The durability settings here are the point of this class, so they are set
 * explicitly rather than left to defaults -- a future Kafka client changing a
 * default should not silently change what "published" means for money.
 */
@Configuration
class KafkaProducerConfig {

    @Bean
    ProducerFactory<String, TransactionMessage> transactionProducerFactory(
            @Value("${spring.kafka.bootstrap-servers}") String bootstrapServers,
            @Value("${ledgerline.producer.acks:all}") String acks,
            ObjectMapper objectMapper) {

        Map<String, Object> config = new HashMap<>();
        config.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        config.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);

        /*
         * acks=all, not acks=1, by default.
         *
         * acks=1 acknowledges as soon as the partition leader has written the
         * record, before any follower has copied it. If that leader fails
         * before replication, the record is gone and the producer has already
         * been told it succeeded -- a silent loss, which for a financial
         * transaction means a transfer that the ledger believes was published
         * and that no consumer will ever see.
         *
         * acks=all waits for every in-sync replica. It costs latency, and on
         * a single-node development broker it is satisfied by the one
         * replica that exists. The setting is here so the guarantee is already
         * correct when this runs against a replicated cluster, rather than
         * being something to remember to change later.
         *
         * ledgerline.producer.acks exists for exactly one purpose: Day 10's
         * measured comparison of acks=all vs acks=1 against the real 3-broker
         * cluster, run twice with a leader killed under load, recording
         * produced-vs-persisted counts for each. Default is "all" everywhere
         * except that one temporary comparison. See docs/day10-multi-broker.md.
         */
        config.put(ProducerConfig.ACKS_CONFIG, acks);

        /*
         * Idempotent producer: the broker deduplicates records by producer id
         * and sequence number, so a retry after a network timeout appends the
         * record once rather than twice. Without it, retries turn an ambiguous
         * failure into a duplicate transaction on the topic.
         */
        config.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);
        config.put(ProducerConfig.RETRIES_CONFIG, Integer.MAX_VALUE);

        /*
         * Bounds the total time spent retrying before the send is failed.
         *
         * This now sits on the synchronous HTTP path, so it doubles as the
         * ceiling on how long a request can wait before the endpoint answers
         * 503. Kept comfortably under the controller's own publish timeout so
         * the producer is what gives up first, with a real error, rather than
         * the controller abandoning a send that might still succeed.
         */
        config.put(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, 8_000);
        config.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, 4_000);

        /*
         * Caps the wait for cluster metadata, which happens before a record is
         * ever queued and is therefore not covered by delivery.timeout.ms. An
         * unreachable broker blocks here first, so without this the send would
         * sit for the default 60s regardless of the timeouts above.
         */
        config.put(ProducerConfig.MAX_BLOCK_MS_CONFIG, 5_000);

        // Required to be <= 5 for idempotence to preserve ordering on retry.
        config.put(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, 5);

        // Built from the application's ObjectMapper so the message uses the
        // serializer declared on TransactionMessage.amount. Type headers are
        // off: the payload is a plain JSON contract, not a Java-typed one, and
        // a consumer in another language should not have to ignore them.
        JsonSerializer<TransactionMessage> valueSerializer = new JsonSerializer<>(objectMapper);
        valueSerializer.setAddTypeInfo(false);

        return new DefaultKafkaProducerFactory<>(config, new StringSerializer(), valueSerializer);
    }

    @Bean
    KafkaTemplate<String, TransactionMessage> transactionKafkaTemplate(
            ProducerFactory<String, TransactionMessage> transactionProducerFactory) {
        return new KafkaTemplate<>(transactionProducerFactory);
    }

    /**
     * Producer for the dead letter topic, sending raw strings.
     *
     * Separate from the typed template because a dead letter carries the
     * original payload verbatim, including payloads that are not valid
     * TransactionMessages at all -- routing those through a serializer that
     * expects a well-formed object is impossible by definition.
     *
     * Same durability settings: a dead letter that is silently lost leaves a
     * rejected transfer with no record anywhere.
     */
    @Bean
    KafkaTemplate<String, String> deadLetterKafkaTemplate(
            @Value("${spring.kafka.bootstrap-servers}") String bootstrapServers) {

        Map<String, Object> config = new HashMap<>();
        config.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        config.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        config.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        config.put(ProducerConfig.ACKS_CONFIG, "all");
        config.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);
        config.put(ProducerConfig.RETRIES_CONFIG, Integer.MAX_VALUE);
        config.put(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, 120_000);
        config.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, 30_000);
        config.put(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, 5);

        return new KafkaTemplate<>(new DefaultKafkaProducerFactory<>(config));
    }
}
