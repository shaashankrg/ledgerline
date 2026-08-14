package com.ledgerline.metrics;

import java.math.BigDecimal;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import com.ledgerline.ledger.LedgerQueries;
import com.ledgerline.ledger.LedgerQueries.UnbalancedAccount;

import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;

/**
 * The one correctness instrument in this project's observability layer, as
 * opposed to a performance one.
 *
 * Both gauges below are read live off cached fields that a scheduled job
 * refreshes, not computed inside the scrape handler. The underlying query is
 * O(entries), and Prometheus scrapes on its own schedule (often every few
 * seconds) independent of how expensive answering is -- computing this
 * synchronously per scrape would turn every scrape into a full ledger scan,
 * exactly the mistake the class comment on {@code /actuator/prometheus}
 * config warns about. A {@link Gauge} registered against an
 * {@link AtomicReference}/{@link AtomicLong} supplier costs nothing to read;
 * only the {@link #recompute()} job costs anything, and it runs on its own
 * clock.
 *
 * {@code ledger_invariant_delta_minor} is the global check: the sum of every
 * entry ever written, which is zero exactly when every transaction balances.
 * {@code ledger_unbalanced_accounts} is the per-account check this project's
 * "assert per item, not aggregate" lesson exists for -- two offsetting
 * errors in different accounts sum to zero and are invisible to the global
 * gauge alone, so this one counts (and, via {@link #unbalancedAccountIds()},
 * names) every account implicated by a currently-unbalanced transaction.
 */
@Component
public class LedgerInvariantGauges {

    private static final Logger log = LoggerFactory.getLogger(LedgerInvariantGauges.class);

    private final LedgerQueries ledgerQueries;

    private final AtomicReference<BigDecimal> cachedDeltaMinor = new AtomicReference<>(BigDecimal.ZERO);
    private final AtomicLong cachedUnbalancedAccountCount = new AtomicLong(0);
    private volatile List<UnbalancedAccount> cachedUnbalancedAccounts = List.of();

    LedgerInvariantGauges(LedgerQueries ledgerQueries, MeterRegistry registry) {
        this.ledgerQueries = ledgerQueries;

        Gauge.builder("ledger_invariant_delta_minor", cachedDeltaMinor, ref -> ref.get().doubleValue())
                .description("Sum of every ledger entry, in minor units. Zero when every transaction balances; "
                        + "any other value means an unbalanced write reached the table.")
                .register(registry);

        Gauge.builder("ledger_unbalanced_accounts", cachedUnbalancedAccountCount, AtomicLong::doubleValue)
                .description("Count of distinct accounts implicated by a currently-unbalanced transaction. "
                        + "Per-account, not global -- catches offsetting errors across accounts that sum to "
                        + "zero and are invisible to ledger_invariant_delta_minor alone.")
                .register(registry);
    }

    /**
     * Recomputes both cached values.
     *
     * The interval is {@code ledgerline.metrics.invariant-check-interval}
     * (default 15s) rather than hardcoded, so a sabotage test can shrink it
     * for a fast detection-latency measurement without changing production
     * behavior -- see the property override in the invariant sabotage test.
     */
    @Scheduled(fixedRateString = "${ledgerline.metrics.invariant-check-interval:15s}")
    public void recompute() {
        BigDecimal delta = ledgerQueries.invariantDeltaMinor();
        List<UnbalancedAccount> unbalanced = ledgerQueries.unbalancedAccounts();

        cachedDeltaMinor.set(delta);
        cachedUnbalancedAccounts = unbalanced;
        cachedUnbalancedAccountCount.set(unbalancedAccountIdSet(unbalanced).size());

        if (delta.compareTo(BigDecimal.ZERO) != 0 || !unbalanced.isEmpty()) {
            log.warn("Ledger invariant violated: delta={} unbalancedAccounts={}",
                    delta, unbalancedAccountIdSet(unbalanced));
        }
    }

    /** The delta gauge's current cached value, for tests that want it without scraping HTTP. */
    public BigDecimal currentDeltaMinor() {
        return cachedDeltaMinor.get();
    }

    /** Every account id currently implicated by an unbalanced transaction -- what the sabotage test names. */
    public Set<Long> unbalancedAccountIds() {
        return unbalancedAccountIdSet(cachedUnbalancedAccounts);
    }

    private static Set<Long> unbalancedAccountIdSet(List<UnbalancedAccount> rows) {
        return rows.stream().map(UnbalancedAccount::accountId)
                .collect(java.util.stream.Collectors.toCollection(java.util.LinkedHashSet::new));
    }
}
