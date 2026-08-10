package com.ledgerline.reconciliation;

import javax.sql.DataSource;

import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.jdbc.DataSourceProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;
import org.springframework.transaction.PlatformTransactionManager;

import com.zaxxer.hikari.HikariDataSource;

/**
 * A second connection to the same database, authenticated as {@code
 * recon_role} rather than the application's own datasource user.
 *
 * This is the mechanism, not just a convention, behind "the reconciliation
 * engine must never read the answer key": {@link ReconciliationService} is
 * constructed with the {@link NamedParameterJdbcTemplate} bean defined here,
 * and every statement it issues runs on a connection Postgres itself has
 * revoked all access to {@code faultlab} from (V8, reasserted by V11). A
 * query against {@code faultlab.injected_faults} written into that service
 * by mistake would fail at execution time with a permission error, the same
 * way {@code SettlementSimulatorTest.reconciliationRoleCannotReadTheAnswerKey}
 * proves it does for a raw JDBC connection.
 *
 * Points at the same host and database as the primary datasource -- only the
 * credentials differ -- so this has to be derived from {@code
 * spring.datasource.url} rather than hardcoded, since Testcontainers assigns
 * that URL a random port per test run.
 *
 * A separate {@link PlatformTransactionManager} is required alongside it:
 * Spring's {@code @Transactional} binds one manager to one {@link
 * DataSource}, and this connection is a distinct DataSource from the
 * primary one Flyway and every other repository in this codebase use.
 *
 * Introducing a second bean of type {@code DataSource} (and, transitively, a
 * second {@code PlatformTransactionManager}) risks making every *existing*,
 * unqualified {@code @Autowired DataSource} elsewhere in the app ambiguous --
 * including Flyway's own autoconfigured connection, which is how an earlier
 * version of this class broke migrations for the whole application: with two
 * DataSource beans and neither one distinguished, Flyway resolved
 * unpredictably and sometimes authenticated as recon_role before any
 * migration had granted it anything. See {@code dataSource} below for how
 * that is resolved -- Spring Boot's own autoconfigured DataSource is marked
 * {@code @Primary} explicitly, so every existing unqualified injection point
 * keeps resolving to exactly the bean it always did.
 */
@Configuration
class ReconRoleDataSourceConfig {

    static final String RECON_ROLE = "recon_role";

    /**
     * A small, deliberately bounded pool -- not the accidental second
     * full-size pool a copy-pasted primary-datasource config would produce.
     * {@link ReconciliationService} and {@link ReconciliationAudit} run one
     * batch's reconciliation at a time inside one transaction; there is no
     * concurrent fan-out on this connection today that would need more than
     * a couple of connections in flight, and an unbounded/oversized pool
     * here would just be idle capacity nothing uses. 2 leaves room for one
     * in-flight query plus the connection health checks Hikari itself opens
     * without over-provisioning.
     *
     * Originally built as a raw {@link
     * org.springframework.jdbc.datasource.DriverManagerDataSource}, which
     * opens a brand new physical connection for every single query with no
     * pooling at all -- worse than an oversized pool, not better, and not an
     * answer to "what pool size did it get" that holds up under load.
     * Replaced with Hikari to match how the primary datasource connects.
     */
    @Bean
    @ReconRoleDataSource
    DataSource reconRoleDataSource(@Value("${spring.datasource.url}") String url) {
        HikariDataSource dataSource = new HikariDataSource();
        dataSource.setJdbcUrl(url);
        dataSource.setUsername(RECON_ROLE);
        dataSource.setPassword(RECON_ROLE);
        dataSource.setPoolName("recon-role-pool");
        dataSource.setMaximumPoolSize(2);
        dataSource.setMinimumIdle(1);
        return dataSource;
    }

    @Bean
    @ReconRoleDataSource
    NamedParameterJdbcTemplate reconRoleJdbcTemplate(
            @Qualifier("reconRoleDataSource") DataSource reconRoleDataSource) {
        return new NamedParameterJdbcTemplate(reconRoleDataSource);
    }

    @Bean("reconTransactionManager")
    PlatformTransactionManager reconTransactionManager(
            @Qualifier("reconRoleDataSource") DataSource reconRoleDataSource) {
        return new DataSourceTransactionManager(reconRoleDataSource);
    }

    /**
     * Spring Boot's own datasource, built from {@code spring.datasource.*}
     * exactly as {@code DataSourceAutoConfiguration} would build it, but
     * declared here and marked {@link Primary} explicitly.
     *
     * With only one DataSource bean in the application, Spring Boot never
     * needs a primary marker -- there is nothing to disambiguate. The moment
     * {@code reconRoleDataSource} above exists as a second bean of the same
     * type, every pre-existing unqualified {@code DataSource} injection
     * point (Flyway's migration connection, every repository's
     * {@code NamedParameterJdbcTemplate}) becomes ambiguous unless one
     * candidate is marked primary. This bean is that candidate; it is
     * functionally identical to what {@code DataSourceAutoConfiguration}
     * would have produced on its own.
     */
    @Bean
    @Primary
    DataSource dataSource(DataSourceProperties properties) {
        return properties.initializeDataSourceBuilder().build();
    }

    /**
     * The primary {@code NamedParameterJdbcTemplate}, explicit for the same
     * reason {@code dataSource} above is explicit: every existing repository
     * in {@code com.ledgerline.ledger}, {@code .transfer}, and
     * {@code .settlement} constructs itself with an unqualified {@code
     * NamedParameterJdbcTemplate}, and introducing {@code
     * reconRoleJdbcTemplate} as a second bean of that type makes all of them
     * ambiguous unless one is marked primary. Built from the {@code
     * dataSource} bean above rather than left to
     * {@code JdbcTemplateAutoConfiguration}, so it is unambiguous which
     * DataSource backs it.
     */
    @Bean
    @Primary
    NamedParameterJdbcTemplate namedParameterJdbcTemplate(@Qualifier("dataSource") DataSource dataSource) {
        return new NamedParameterJdbcTemplate(dataSource);
    }
}
