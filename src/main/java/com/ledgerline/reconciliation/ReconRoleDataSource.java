package com.ledgerline.reconciliation;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

import org.springframework.beans.factory.annotation.Qualifier;

/**
 * Marks the {@code DataSource} and {@code NamedParameterJdbcTemplate} beans
 * connected as {@code recon_role}, so a constructor asking for one cannot
 * receive the application's own datasource by accident -- a plain, unmarked
 * {@code NamedParameterJdbcTemplate} injection point would compile either
 * way, and the whole point of running {@link ReconciliationService} under
 * this role is that "which connection" is not left to chance.
 */
@Qualifier
@Retention(RetentionPolicy.RUNTIME)
@Target({ElementType.FIELD, ElementType.PARAMETER, ElementType.METHOD})
public @interface ReconRoleDataSource {
}
