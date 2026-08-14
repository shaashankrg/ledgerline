package com.ledgerline.cli;

import java.util.EnumMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Component;

import com.ledgerline.generator.FaultType;
import com.ledgerline.generator.GeneratorConfig;
import com.ledgerline.generator.TransactionGenerator;

/**
 * Publishes one deterministic, fault-injected generator run, then exits.
 *
 * The Day 8 Helm chart's generator Job entrypoint: unlike {@link
 * LoadRampCommand} (a rate ramp with no faults, built for Day 6's ceiling
 * measurement) or {@link EmitTransactionCommand} (one hand-specified
 * transaction, no faults), this is the one CLI runner that exposes {@link
 * GeneratorConfig}'s full shape -- seed, transaction count, rate, and every
 * {@link FaultType}'s rate -- as arguments, because Day 8 requires the
 * chart's {@code values.yaml} to control exactly this without anyone hand-
 * editing rendered YAML.
 *
 * Usage:
 *   --spring.profiles.active=generate --seed=42 --count=300 --rate=20
 *   [--fault.duplicatePublish=0.02] [--fault.outOfOrder=0.02]
 *   [--fault.orphanCapture=0.01] [--fault.amountDrift=0.02]
 *   [--fault.missingSettlement=0.02]
 */
@Component
@Profile("generate")
public class GenerateCommand implements ApplicationRunner {

    private static final Logger log = LoggerFactory.getLogger(GenerateCommand.class);

    private final TransactionGenerator generator;
    private final ApplicationContext context;

    GenerateCommand(TransactionGenerator generator, ApplicationContext context) {
        this.generator = generator;
        this.context = context;
    }

    @Override
    public void run(ApplicationArguments args) throws Exception {
        long seed = Long.parseLong(optional(args, "seed").orElse("42"));
        int count = Integer.parseInt(optional(args, "count").orElse("300"));
        int rate = Integer.parseInt(optional(args, "rate").orElse("0"));
        // Two real, distinct accounts -- see LoadRampCommand's identical
        // reasoning: the point is generating a stream, not account fan-out.
        List<Long> accountIds = List.of(1L, 2L);

        Map<FaultType, Double> faultRates = new EnumMap<>(FaultType.class);
        faultRates.put(FaultType.DUPLICATE_PUBLISH, faultRate(args, "duplicatePublish"));
        faultRates.put(FaultType.OUT_OF_ORDER, faultRate(args, "outOfOrder"));
        faultRates.put(FaultType.ORPHAN_CAPTURE, faultRate(args, "orphanCapture"));
        faultRates.put(FaultType.AMOUNT_DRIFT, faultRate(args, "amountDrift"));
        faultRates.put(FaultType.MISSING_SETTLEMENT, faultRate(args, "missingSettlement"));

        String runId = "gen-" + seed + "-" + System.currentTimeMillis();
        log.info("Generating run {}: seed={} count={} rate={} faultRates={}",
                runId, seed, count, rate, faultRates);

        GeneratorConfig config = new GeneratorConfig(runId, seed, count, rate, faultRates, accountIds);
        var result = generator.generate(config);

        log.info("Generation complete: runId={} publishedMessages={} injectedFaults={} publishFailures={}",
                runId, result.publishedMessages(), result.injectedFaults(), result.publishFailures());

        SpringApplication.exit(context, () -> 0);
    }

    private static double faultRate(ApplicationArguments args, String name) {
        return Double.parseDouble(optional(args, "fault." + name).orElse("0.0"));
    }

    private static Optional<String> optional(ApplicationArguments args, String name) {
        List<String> values = args.getOptionValues(name);
        return values == null || values.isEmpty()
                ? Optional.empty()
                : Optional.of(values.get(0));
    }
}
