package com.ledgerline;

import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.PostgreSQLContainer;

/**
 * Base class for tests that need a real Postgres.
 *
 * The container is static, so one instance is shared by every subclass in the
 * run rather than being started per test class. Flyway migrates it on context
 * startup, which means tests exercise the same schema the application uses,
 * constraints included.
 *
 * A mocked datasource is deliberately not used: the property under test is that
 * Postgres and our constraints together keep the ledger balanced, and a mock
 * cannot demonstrate that.
 *
 * Note the {@code -Dapi.version} argLine in pom.xml -- without it, docker-java
 * cannot reach Docker Engine 29 and every test here fails at startup.
 */
@SpringBootTest
public abstract class AbstractPostgresTest {

    // postgres:16 matches docker-compose.yml so tests and local dev run the
    // same server version. Reusing the tag also avoids an image pull.
    private static final PostgreSQLContainer<?> POSTGRES = new PostgreSQLContainer<>("postgres:16");

    static {
        // Started once for the JVM. Testcontainers' Ryuk sidecar reaps the
        // container when the JVM exits, so there is no stop() call here.
        POSTGRES.start();
    }

    @DynamicPropertySource
    static void datasourceProperties(DynamicPropertyRegistry registry) {
        registry.add("spring.datasource.url", POSTGRES::getJdbcUrl);
        registry.add("spring.datasource.username", POSTGRES::getUsername);
        registry.add("spring.datasource.password", POSTGRES::getPassword);
    }
}
