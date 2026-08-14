package com.ledgerline;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableScheduling;

// Enables the invariant gauges' @Scheduled recomputation
// (see LedgerInvariantGauges) -- nothing else in the app uses scheduling yet.
@EnableScheduling
@SpringBootApplication
public class LedgerlineApplication {

	public static void main(String[] args) {
		SpringApplication.run(LedgerlineApplication.class, args);
	}

}
