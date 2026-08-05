package com.ledgerline.api;

import static org.assertj.core.api.Assertions.assertThat;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.UUID;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.server.LocalServerPort;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.PostgreSQLContainer;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * What the endpoint does when it cannot publish.
 *
 * The broker address points nowhere, so every publish times out. Postgres is
 * real, which is the point: the assertion is that a failed publish leaves the
 * ledger untouched, and that can only be shown against a database that could
 * have been written to.
 */
@SpringBootTest(
        webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT,
        // The consumer is irrelevant here and would only spin retrying an
        // unreachable broker for the duration of the test.
        properties = "ledgerline.consumer.enabled=false")
class TransferIntakeUnavailableTest {

    private static final PostgreSQLContainer<?> POSTGRES =
            new PostgreSQLContainer<>("postgres:16");

    static {
        POSTGRES.start();
    }

    @DynamicPropertySource
    static void containerProperties(DynamicPropertyRegistry registry) {
        // Unroutable: reserved for documentation use, so nothing answers.
        registry.add("spring.kafka.bootstrap-servers", () -> "192.0.2.1:9092");
        registry.add("spring.datasource.url", POSTGRES::getJdbcUrl);
        registry.add("spring.datasource.username", POSTGRES::getUsername);
        registry.add("spring.datasource.password", POSTGRES::getPassword);
    }

    @LocalServerPort
    private int port;

    @Autowired
    private JdbcTemplate jdbc;

    private static final ObjectMapper JSON = new ObjectMapper();
    private static final HttpClient CLIENT = HttpClient.newBuilder()
            .connectTimeout(Duration.ofSeconds(5))
            .build();

    @Test
    void publishFailureReturns503AndWritesNothing() throws Exception {
        jdbc.update("DELETE FROM ledger_entries");
        jdbc.update("DELETE FROM transactions");

        long alice = accountId("Alice Checking");
        long bob = accountId("Bob Checking");

        HttpRequest request = HttpRequest.newBuilder(
                URI.create("http://localhost:" + port + "/api/v1/transfers"))
                .header("Content-Type", "application/json")
                .header("Idempotency-Key", UUID.randomUUID().toString())
                .timeout(Duration.ofSeconds(60))
                .POST(HttpRequest.BodyPublishers.ofString("""
                        {"fromAccountId":%d,"toAccountId":%d,"amount":"50.00","currency":"USD"}"""
                        .formatted(alice, bob)))
                .build();

        HttpResponse<String> response = CLIENT.send(request, HttpResponse.BodyHandlers.ofString());

        // 503, not 500: nothing was recorded, so the client may safely retry.
        assertThat(response.statusCode()).isEqualTo(503);

        JsonNode problem = JSON.readTree(response.body());
        assertThat(problem.get("type").asText()).isEqualTo(ErrorTypes.INTAKE_UNAVAILABLE.toString());
        assertThat(problem.get("correlationId").asText()).isNotBlank();

        // No exception detail leaked to the client.
        String body = response.body().toLowerCase();
        assertThat(body)
                .doesNotContain("kafka")
                .doesNotContain("timeout")
                .doesNotContain("exception");

        assertThat(entryCount()).isZero();
        assertThat(transactionCount()).isZero();
    }

    private long accountId(String name) {
        return jdbc.queryForObject("SELECT id FROM accounts WHERE name = ?", Long.class, name);
    }

    private long entryCount() {
        return jdbc.queryForObject("SELECT count(*) FROM ledger_entries", Long.class);
    }

    private long transactionCount() {
        return jdbc.queryForObject("SELECT count(*) FROM transactions", Long.class);
    }
}
