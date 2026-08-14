package com.ledgerline.cli;

import java.util.List;
import java.util.Optional;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Component;

import com.ledgerline.generator.GeneratorConfig;
import com.ledgerline.generator.TransactionGenerator;

/**
 * Day 6's load ramp: publishes at increasing rates until told to stop, so a
 * separately-running consumer + ledger can be watched (via the Grafana
 * dashboard) for where P99 degrades or consumer lag grows unbounded.
 *
 * A one-shot CLI runner rather than a JUnit test on purpose -- what's under
 * test is the actually-running docker-compose stack (real Postgres, real
 * Kafka, the real consumer process this project's dashboard is already
 * pointed at), not an ephemeral Testcontainers instance nobody is watching a
 * dashboard against. Run alongside a live `./mvnw spring-boot:run` and
 * Grafana at localhost:3000.
 *
 * Usage:
 *   --spring.profiles.active=load --rates=50,100,200,400,800 --seconds-per-step=30
 *
 * Each step publishes at a fixed target rate (transactions/second) for
 * seconds-per-step, then moves to the next rate. One run, not two -- see the
 * Day 6 prompt's reasoning for why a second run or a connection-pool
 * ablation isn't called for here (that rigor already exists as a published
 * methodology elsewhere).
 */
@Component
@Profile("load")
public class LoadRampCommand implements ApplicationRunner {

    private static final Logger log = LoggerFactory.getLogger(LoadRampCommand.class);

    private final TransactionGenerator generator;
    private final ApplicationContext context;

    LoadRampCommand(TransactionGenerator generator, ApplicationContext context) {
        this.generator = generator;
        this.context = context;
    }

    @Override
    public void run(ApplicationArguments args) throws Exception {
        List<Integer> rates = parseRates(optional(args, "rates").orElse("50,100,200,400,800"));
        int secondsPerStep = Integer.parseInt(optional(args, "seconds-per-step").orElse("30"));
        // Two real, distinct accounts -- generator.planTransaction needs at
        // least two, and the point of this ramp is throughput, not account
        // fan-out, so more accounts would only add noise to what's being
        // measured.
        List<Long> accountIds = List.of(1L, 2L);

        log.info("Load ramp starting: rates={} secondsPerStep={}", rates, secondsPerStep);

        for (int rate : rates) {
            int transactionCount = rate * secondsPerStep;
            String runId = "load-" + rate + "rps-" + System.currentTimeMillis();

            log.info("=== step: {} tx/s target, {} transactions over {}s, runId={} ===",
                    rate, transactionCount, secondsPerStep, runId);

            long stepStartNanos = System.nanoTime();
            GeneratorConfig config = new GeneratorConfig(
                    runId, rate, transactionCount, rate,
                    new java.util.EnumMap<>(com.ledgerline.generator.FaultType.class), accountIds);
            var result = generator.generate(config);
            double actualSeconds = (System.nanoTime() - stepStartNanos) / 1_000_000_000.0;
            double actualRate = result.publishedMessages() / actualSeconds;

            log.info("=== step done: {} tx/s target -> {} messages published in {}s ({} msg/s actual), "
                            + "{} publish failures ===",
                    rate, result.publishedMessages(), String.format("%.1f", actualSeconds),
                    String.format("%.1f", actualRate), result.publishFailures());
        }

        log.info("Load ramp complete. Check the Grafana dashboard (http://localhost:3000) "
                + "and the consumer/DB logs for where things degraded.");

        SpringApplication.exit(context, () -> 0);
    }

    private static List<Integer> parseRates(String raw) {
        return java.util.Arrays.stream(raw.split(","))
                .map(String::trim)
                .filter(value -> !value.isEmpty())
                .map(Integer::parseInt)
                .toList();
    }

    private static Optional<String> optional(ApplicationArguments args, String name) {
        List<String> values = args.getOptionValues(name);
        return values == null || values.isEmpty()
                ? Optional.empty()
                : Optional.of(values.get(0));
    }
}
