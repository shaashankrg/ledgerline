package com.ledgerline.cli;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Component;

/**
 * Exists only so the {@code migrate} profile's process exits after startup
 * rather than idling forever.
 *
 * Flyway's own autoconfiguration runs migrations synchronously during
 * context refresh, before any {@link ApplicationRunner} executes -- by the
 * time {@link #run} is called here, migrations have already either
 * succeeded (and the context finished starting) or failed (and this class
 * was never reached, {@code SpringApplication} exits non-zero on its own).
 * This class's only job is to be the thing that turns "context started
 * successfully" into "process exits 0", which is what makes this profile
 * usable as a Kubernetes Job/Helm hook rather than a process that lingers
 * until something kills it.
 */
@Component
@Profile("migrate")
public class MigrateCommand implements ApplicationRunner {

    private static final Logger log = LoggerFactory.getLogger(MigrateCommand.class);

    private final ApplicationContext context;

    MigrateCommand(ApplicationContext context) {
        this.context = context;
    }

    @Override
    public void run(ApplicationArguments args) {
        log.info("Migrations applied successfully.");
        SpringApplication.exit(context, () -> 0);
    }
}
