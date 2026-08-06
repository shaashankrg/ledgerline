package com.ledgerline.cli;

import java.math.BigDecimal;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Component;

import com.ledgerline.domain.EventType;
import com.ledgerline.messaging.TransactionMessage;
import com.ledgerline.messaging.TransactionProducer;

/**
 * Emits one transaction's lifecycle, then exits.
 *
 * Exists so a single transaction can be put through the pipeline without
 * running the load generator -- useful for a demo, and for checking by hand
 * that a change did what it claims end to end.
 *
 * Publishes rather than writing directly: the point is to exercise the real
 * path, and the real path is Kafka. Activated by the "emit" profile so it stays
 * out of the way of a normal boot.
 *
 * Usage:
 *   --spring.profiles.active=emit --from=1 --to=2 --amount=50.00
 *   optional: --events=AUTHORIZE,CAPTURE,SETTLE   (default)
 *             --txn=&lt;external id&gt;               (default: random)
 */
@Component
@Profile("emit")
public class EmitTransactionCommand implements ApplicationRunner {

    private static final Logger log = LoggerFactory.getLogger(EmitTransactionCommand.class);

    private static final List<EventType> DEFAULT_LIFECYCLE =
            List.of(EventType.AUTHORIZE, EventType.CAPTURE, EventType.SETTLE);

    private final TransactionProducer producer;
    private final ApplicationContext context;

    EmitTransactionCommand(TransactionProducer producer, ApplicationContext context) {
        this.producer = producer;
        this.context = context;
    }

    @Override
    public void run(ApplicationArguments args) throws Exception {
        String externalTxnId = optional(args, "txn").orElse(UUID.randomUUID().toString());
        long from = Long.parseLong(optional(args, "from").orElse("1"));
        long to = Long.parseLong(optional(args, "to").orElse("2"));
        BigDecimal amount = new BigDecimal(optional(args, "amount").orElse("50.00"));
        String currency = optional(args, "currency").orElse("USD");

        List<EventType> lifecycle = optional(args, "events")
                .map(EmitTransactionCommand::parseEvents)
                .orElse(DEFAULT_LIFECYCLE);

        log.info("Emitting transaction {} as {}", externalTxnId, lifecycle);

        for (EventType type : lifecycle) {
            // A distinct event id per step, deterministic from the transaction
            // and the type, so re-running the command with the same --txn
            // deduplicates rather than double-writing.
            String eventId = externalTxnId + ":" + type.name();

            producer.publish(new TransactionMessage(
                    externalTxnId, eventId, type, from, to, amount, currency))
                    .get(30, TimeUnit.SECONDS);

            log.info("  published {} ({})", type, eventId);
        }

        log.info("Done. Transaction {} emitted.", externalTxnId);

        // Otherwise the web server and the Kafka listener's threads keep the
        // JVM alive indefinitely -- this command is meant to run once and
        // return control to the shell, not to become a long-lived process.
        SpringApplication.exit(context, () -> 0);
    }

    private static List<EventType> parseEvents(String raw) {
        return Arrays.stream(raw.split(","))
                .map(String::trim)
                .filter(value -> !value.isEmpty())
                .map(value -> EventType.valueOf(value.toUpperCase(Locale.ROOT)))
                .toList();
    }

    private static Optional<String> optional(ApplicationArguments args, String name) {
        List<String> values = args.getOptionValues(name);
        return values == null || values.isEmpty()
                ? Optional.empty()
                : Optional.of(values.get(0));
    }
}
