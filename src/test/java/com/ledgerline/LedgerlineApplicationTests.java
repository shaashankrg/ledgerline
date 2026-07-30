package com.ledgerline;

import org.junit.jupiter.api.Test;

/**
 * Extends the Testcontainers base so the suite does not depend on the
 * docker-compose database being up.
 */
class LedgerlineApplicationTests extends AbstractPostgresTest {

	@Test
	void contextLoads() {
	}

}
