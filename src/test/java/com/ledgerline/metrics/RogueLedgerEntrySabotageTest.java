package com.ledgerline.metrics;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import java.math.BigDecimal;
import java.time.Duration;
import java.time.Instant;
import java.util.UUID;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.jdbc.core.JdbcTemplate;

import com.ledgerline.AbstractPostgresTest;
import com.ledgerline.domain.EventType;
import com.ledgerline.domain.TransactionEvent;
import com.ledgerline.transfer.TransactionEventService;

/**
 * The one item from Days 5-6 that must not be cut or rushed: proof that the
 * ledger invariant gauges actually notice when the invariant breaks, and how
 * fast.
 *
 * The corruption technique -- inserting a single, unmatched
 * {@code ledger_entries} row directly via SQL, bypassing {@link
 * com.ledgerline.ledger.LedgerWriter} entirely -- is the same raw-SQL bypass
 * {@code LedgerInvariantTest} already uses to poke schema constraints
 * ({@code insertBareTransaction}/{@code insertRawEntry}), extended here to a
 * row that the schema's constraints do NOT reject: a single positive entry
 * against a real transaction and a real account is individually legal (it
 * satisfies the NOT NULL, FK, and nonzero-amount constraints) and only wrong
 * in that nothing balances it. That is exactly the class of defect no schema
 * constraint can catch and the invariant gauges exist for.
 *
 * The scheduled recompute interval is shortened via a property override
 * (2s instead of the 15s default) so this test measures a real, fast
 * wall-clock number rather than waiting out the production interval on every
 * run -- the interval itself is still the thing being proven reactive
 * within, not something this test weakens the production default to satisfy.
 */
@SpringBootTest(properties = "ledgerline.metrics.invariant-check-interval=2s")
class RogueLedgerEntrySabotageTest extends AbstractPostgresTest {

    @Autowired
    private TransactionEventService eventService;

    @Autowired
    private LedgerInvariantGauges gauges;

    @Autowired
    private JdbcTemplate jdbc;

    private long alice;
    private long bob;
    private long carol;

    @BeforeEach
    void setUp() {
        jdbc.update("DELETE FROM ledger_entries");
        jdbc.update("DELETE FROM transactions");
        jdbc.update("DELETE FROM transaction_states");

        alice = accountId("Alice Checking");
        bob = accountId("Bob Checking");
        carol = accountId("Carol Checking");

        // Let one recompute cycle pass so the gauges reflect a clean ledger
        // before this test starts, rather than whatever leftover state a
        // previous test's cache still holds.
        await().atMost(Duration.ofSeconds(10))
                .until(() -> gauges.currentDeltaMinor().compareTo(BigDecimal.ZERO) == 0);
    }

    @Test
    void bothGaugesStayFlatAtZeroUnderNormalLoad() {
        transfer(alice, bob, "50.00");
        transfer(bob, carol, "20.00");

        await().atMost(Duration.ofSeconds(10))
                .until(() -> gauges.unbalancedAccountIds().isEmpty());

        assertThat(gauges.currentDeltaMinor()).isEqualByComparingTo(BigDecimal.ZERO);
        assertThat(gauges.unbalancedAccountIds()).isEmpty();
    }

    @Test
    void rogueSingleSidedEntryIsDetectedByBothGaugesWithinOneScrapeInterval() {
        // A real, honest transaction first, so the corrupted one is not the
        // only row in the table and the per-account gauge has to actually
        // discriminate rather than trivially flag "the only account there is".
        transfer(alice, bob, "50.00");
        await().atMost(Duration.ofSeconds(10))
                .until(() -> gauges.currentDeltaMinor().compareTo(BigDecimal.ZERO) == 0);

        // The §9.4-style sabotage: one entry, no matching counterpart,
        // written directly via SQL -- LedgerWriter, EntryGroup's
        // balance-checking constructor, and every application-layer
        // guarantee are all bypassed. Individually legal by the schema
        // (nonzero amount, real transaction, real account); wrong only
        // because nothing offsets it.
        long transactionId = insertBareTransaction();
        BigDecimal rogueAmount = new BigDecimal("13.37");
        Instant corruptedAt = Instant.now();
        insertRawEntry(transactionId, carol, rogueAmount);

        // The actual claim Day 5 exists to make: a real, recorded number, not
        // just "detected == true".
        await().atMost(Duration.ofSeconds(10))
                .until(() -> gauges.currentDeltaMinor().compareTo(BigDecimal.ZERO) != 0);
        Duration detectionLatency = Duration.between(corruptedAt, Instant.now());

        System.out.println("[RogueLedgerEntrySabotageTest] corruption visible in "
                + "ledger_invariant_delta_minor within " + detectionLatency.toMillis() + "ms "
                + "(scrape interval under test: 2000ms)");

        // The global gauge: it moved, and by exactly the rogue amount --
        // nothing else in this test wrote an unbalanced cent. No scale
        // conversion: ledger_entries.amount is already the ledger's native
        // unit (NUMERIC(19,4)), which is what "minor" in the gauge's name
        // refers to here -- see LedgerQueries.invariantDeltaMinor's Javadoc.
        assertThat(gauges.currentDeltaMinor())
                .as("ledger_invariant_delta_minor after a single rogue %s entry", rogueAmount)
                .isEqualByComparingTo(rogueAmount);

        // The per-account gauge: not just "nonzero count", but the specific
        // account. This is the check the project's global-sum blind spot
        // (two offsetting errors in different accounts summing to zero and
        // hiding from a single aggregate) cannot pass by accident, because it
        // asserts identity, not just cardinality.
        await().atMost(Duration.ofSeconds(10))
                .until(() -> !gauges.unbalancedAccountIds().isEmpty());

        assertThat(gauges.unbalancedAccountIds())
                .as("ledger_unbalanced_accounts must name the specific corrupted account")
                .containsExactly(carol);

        // Sanity for the sabotage's own validity: alice and bob's honest
        // transfer must not have been swept up as "unbalanced" merely because
        // some other transaction on the table now is. This is the
        // per-transaction join in LedgerQueries.unbalancedAccounts doing its
        // job, not an accident of there being only one other account.
        assertThat(gauges.unbalancedAccountIds()).doesNotContain(alice, bob);
    }

    /**
     * The offsetting-errors-across-accounts case the global delta alone
     * cannot see: two single-sided entries, in different transactions, whose
     * amounts happen to cancel out system-wide while neither transaction
     * individually balances. If ledger_invariant_delta_minor were the only
     * signal, this corruption would be invisible; ledger_unbalanced_accounts
     * must still name both accounts.
     */
    @Test
    void offsettingRogueEntriesInDifferentAccountsAreStillCaughtPerAccount() {
        transfer(alice, bob, "50.00");
        await().atMost(Duration.ofSeconds(10))
                .until(() -> gauges.currentDeltaMinor().compareTo(BigDecimal.ZERO) == 0);

        long transactionA = insertBareTransaction();
        long transactionB = insertBareTransaction();
        BigDecimal amount = new BigDecimal("9.00");
        // Two different transactions, two different accounts, opposite signs
        // -- the system-wide SUM is unchanged (+9.00 and -9.00 cancel), but
        // neither transaction balances on its own.
        insertRawEntry(transactionA, carol, amount);
        insertRawEntry(transactionB, bob, amount.negate());

        await().atMost(Duration.ofSeconds(10))
                .until(() -> gauges.unbalancedAccountIds().size() >= 2);

        // The global gauge is exactly the blind spot this proves: it reads
        // zero even though the ledger is broken in two places.
        assertThat(gauges.currentDeltaMinor())
                .as("global delta cancels out even though two transactions are individually unbalanced")
                .isEqualByComparingTo(BigDecimal.ZERO);

        // The per-account gauge is what actually catches it.
        assertThat(gauges.unbalancedAccountIds())
                .as("both accounts touched by an unbalanced transaction must be named, "
                        + "even though the global sum hides the corruption")
                .contains(carol, bob);
    }

    private void transfer(long from, long to, String amount) {
        String key = UUID.randomUUID().toString();
        BigDecimal parsed = new BigDecimal(amount);
        Instant now = Instant.now();

        eventService.apply(new TransactionEvent(
                key, key + ":AUTHORIZE", EventType.AUTHORIZE, from, to, parsed, "USD", now));
        eventService.apply(new TransactionEvent(
                key, key + ":CAPTURE", EventType.CAPTURE, from, to, parsed, "USD", now));
    }

    private long accountId(String name) {
        return jdbc.queryForObject("SELECT id FROM accounts WHERE name = ?", Long.class, name);
    }

    private long insertBareTransaction() {
        return jdbc.queryForObject(
                "INSERT INTO transactions (idempotency_key) VALUES (?) RETURNING id",
                Long.class, UUID.randomUUID().toString());
    }

    /** Bypasses LedgerWriter entirely -- the same technique LedgerInvariantTest uses for schema tests. */
    private void insertRawEntry(long transactionId, long accountId, BigDecimal amount) {
        jdbc.update("INSERT INTO ledger_entries (transaction_id, account_id, amount) VALUES (?, ?, ?)",
                transactionId, accountId, amount);
    }
}
