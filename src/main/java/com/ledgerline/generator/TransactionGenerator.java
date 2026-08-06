package com.ledgerline.generator;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import com.ledgerline.domain.EventType;
import com.ledgerline.generator.FaultLedger.InjectedFault;
import com.ledgerline.messaging.TransactionMessage;
import com.ledgerline.messaging.TransactionProducer;

/**
 * Produces synthetic transaction streams, with deliberate faults, to Kafka.
 *
 * Deliberately not wired into the application's own paths: it publishes to the
 * same topic a real producer would, and the consumer cannot tell the difference.
 * That is the point -- a generator the system knows about is testing a
 * different system.
 *
 * Determinism is the property that matters most here. Every decision comes from
 * one seeded Random, consumed in a fixed order, so the same seed and config
 * produce byte-identical streams. Week 3's accuracy measurements are comparable
 * only if the stream they measured against can be reproduced exactly.
 */
@Component
public class TransactionGenerator {

    private static final Logger log = LoggerFactory.getLogger(TransactionGenerator.class);

    /** Amounts land between these, at the ledger's scale. */
    private static final BigDecimal MIN_AMOUNT = new BigDecimal("1.00");
    private static final BigDecimal AMOUNT_SPREAD = new BigDecimal("500.00");

    private final TransactionProducer producer;
    private final FaultLedger faultLedger;

    TransactionGenerator(TransactionProducer producer, FaultLedger faultLedger) {
        this.producer = producer;
        this.faultLedger = faultLedger;
    }

    /**
     * Generates and publishes a run.
     *
     * @return what was produced, for a caller that wants to assert on it
     */
    public GeneratorResult generate(GeneratorConfig config) throws Exception {
        faultLedger.startRun(config);

        // One RNG for the whole run. Every draw below advances it in a fixed
        // order, which is what makes the seed sufficient to reproduce the run.
        Random random = new Random(config.seed());

        List<String> transactionIds = new ArrayList<>();
        int publishedMessages = 0;
        int injectedFaults = 0;

        long startNanos = System.nanoTime();

        for (int i = 0; i < config.transactionCount(); i++) {
            TransactionPlan plan = planTransaction(config, random, i);
            transactionIds.add(plan.externalTxnId());

            for (InjectedFault fault : plan.faults()) {
                faultLedger.record(fault);
                injectedFaults++;
            }

            for (TransactionMessage message : plan.messages()) {
                producer.publish(message).get(30, TimeUnit.SECONDS);
                publishedMessages++;
            }

            pace(config, startNanos, i + 1);
        }

        faultLedger.finishRun(config.runId());

        log.info("Run {} produced {} transactions, {} messages, {} faults",
                config.runId(), transactionIds.size(), publishedMessages, injectedFaults);

        return new GeneratorResult(
                config.runId(), transactionIds, publishedMessages, injectedFaults);
    }

    /**
     * Decides one transaction's events and faults.
     *
     * Fault draws happen in a fixed order regardless of configured rates, so
     * adding a fault type with rate zero does not shift the stream a previous
     * seed produced. That matters: a run recorded last week must still replay
     * identically after a new fault type is added to the enum.
     */
    private TransactionPlan planTransaction(GeneratorConfig config, Random random, int index) {
        String externalTxnId = config.runId() + "-txn-" + index;

        long from = config.accountIds().get(random.nextInt(config.accountIds().size()));
        long to = from;
        while (to == from) {
            to = config.accountIds().get(random.nextInt(config.accountIds().size()));
        }

        BigDecimal amount = MIN_AMOUNT.add(
                AMOUNT_SPREAD.multiply(BigDecimal.valueOf(random.nextDouble())))
                .setScale(4, RoundingMode.HALF_UP);

        // Drawn up front, in enum order, so the sequence of draws is stable.
        boolean duplicate = random.nextDouble() < config.rateOf(FaultType.DUPLICATE_PUBLISH);
        boolean outOfOrder = random.nextDouble() < config.rateOf(FaultType.OUT_OF_ORDER);
        boolean orphan = random.nextDouble() < config.rateOf(FaultType.ORPHAN_CAPTURE);
        boolean drift = random.nextDouble() < config.rateOf(FaultType.AMOUNT_DRIFT);
        boolean missingSettlement = random.nextDouble() < config.rateOf(FaultType.MISSING_SETTLEMENT);

        int duplicateCopies = 2 + random.nextInt(4); // 2..5 inclusive

        Instant base = Instant.now().truncatedTo(ChronoUnit.MILLIS);

        List<TransactionMessage> messages = new ArrayList<>();
        List<InjectedFault> faults = new ArrayList<>();

        TransactionMessage authorize = message(
                externalTxnId, EventType.AUTHORIZE, from, to, amount);
        TransactionMessage capture = message(
                externalTxnId, EventType.CAPTURE, from, to, amount);

        // An orphan capture has no authorize at all, which subsumes any
        // ordering fault -- there is nothing for the capture to precede.
        if (orphan) {
            messages.add(capture);
            faults.add(InjectedFault.of(config.runId(), FaultType.ORPHAN_CAPTURE,
                    externalTxnId, "capture published with no authorize"));
        } else if (outOfOrder) {
            messages.add(capture);
            messages.add(authorize);
            faults.add(InjectedFault.of(config.runId(), FaultType.OUT_OF_ORDER,
                    externalTxnId, "capture published before authorize"));
        } else {
            messages.add(authorize);
            messages.add(capture);
        }

        if (duplicate) {
            // Extra copies of the capture, which is the event that writes
            // entries and therefore the one where a duplicate would hurt.
            for (int copy = 1; copy < duplicateCopies; copy++) {
                messages.add(capture);
            }
            faults.add(InjectedFault.duplicate(
                    config.runId(), externalTxnId, capture.eventId(), duplicateCopies));
        }

        // A settlement cannot follow a capture that never happens.
        boolean settles = !orphan && !missingSettlement;

        if (settles) {
            BigDecimal settledAmount = amount;

            if (drift) {
                // Drift by a small proportion, always at ledger scale, and
                // never to zero -- a zero settlement is a different fault.
                BigDecimal delta = amount
                        .multiply(BigDecimal.valueOf(0.01 + random.nextDouble() * 0.09))
                        .setScale(4, RoundingMode.HALF_UP)
                        .max(new BigDecimal("0.0001"));
                settledAmount = random.nextBoolean() ? amount.add(delta) : amount.subtract(delta);

                faults.add(InjectedFault.drift(
                        config.runId(), externalTxnId,
                        externalTxnId + ":SETTLE", amount, settledAmount));
            }

            messages.add(message(externalTxnId, EventType.SETTLE, from, to, settledAmount));
        } else if (missingSettlement && !orphan) {
            faults.add(InjectedFault.of(config.runId(), FaultType.MISSING_SETTLEMENT,
                    externalTxnId, "captured but never settled"));
        }

        return new TransactionPlan(externalTxnId, messages, faults, base);
    }

    /**
     * A message for one event.
     *
     * The event id is derived from the transaction and the type, so a duplicate
     * publish is genuinely the same event arriving twice rather than two
     * different events that happen to look alike -- which is what makes the
     * consumer's idempotency the thing under test.
     */
    private static TransactionMessage message(
            String externalTxnId, EventType type, long from, long to, BigDecimal amount) {
        return new TransactionMessage(
                externalTxnId,
                externalTxnId + ":" + type.name(),
                type,
                from,
                to,
                amount,
                "USD");
    }

    /** Holds the configured rate, if one was asked for. */
    private static void pace(GeneratorConfig config, long startNanos, int produced) {
        if (config.ratePerSecond() <= 0) {
            return;
        }

        long targetNanos = TimeUnit.SECONDS.toNanos(1) * produced / config.ratePerSecond();
        long elapsed = System.nanoTime() - startNanos;
        long sleepNanos = targetNanos - elapsed;

        if (sleepNanos > 0) {
            try {
                TimeUnit.NANOSECONDS.sleep(sleepNanos);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
    }

    private record TransactionPlan(
            String externalTxnId,
            List<TransactionMessage> messages,
            List<InjectedFault> faults,
            Instant baseTime) {
    }

    /** What a run produced. */
    public record GeneratorResult(
            String runId,
            List<String> transactionIds,
            int publishedMessages,
            int injectedFaults) {
    }
}
