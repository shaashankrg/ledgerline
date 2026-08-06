package com.ledgerline.domain;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;
import static org.assertj.core.api.Assertions.assertThatNullPointerException;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

import org.junit.jupiter.api.Test;

/** The invariants the domain records refuse to be constructed without. */
class DomainRecordsTest {

    @Test
    void entryGroupRejectsAnUnbalancedSet() {
        List<LedgerEntry> unbalanced = List.of(
                new LedgerEntry(1L, new BigDecimal("-50.00"), "USD"),
                new LedgerEntry(2L, new BigDecimal("40.00"), "USD"));

        assertThatExceptionOfType(IllegalArgumentException.class)
                .isThrownBy(() -> new EntryGroup(unbalanced))
                .withMessageContaining("must sum to zero")
                .withMessageContaining("-10.00");
    }

    @Test
    void entryGroupAcceptsABalancedPair() {
        EntryGroup group = new EntryGroup(List.of(
                new LedgerEntry(1L, new BigDecimal("-50.00"), "USD"),
                new LedgerEntry(2L, new BigDecimal("50.00"), "USD")));

        assertThat(group.entries()).hasSize(2);
        assertThat(group.isEmpty()).isFalse();
    }

    /** Multi-leg groups are legal as long as they balance. */
    @Test
    void entryGroupAcceptsMoreThanTwoEntriesWhenTheyBalance() {
        EntryGroup group = new EntryGroup(List.of(
                new LedgerEntry(1L, new BigDecimal("-100.00"), "USD"),
                new LedgerEntry(2L, new BigDecimal("70.00"), "USD"),
                new LedgerEntry(3L, new BigDecimal("30.00"), "USD")));

        assertThat(group.entries()).hasSize(3);
    }

    /** Differing scales must not defeat the balance check. */
    @Test
    void entryGroupComparesByValueNotScale() {
        EntryGroup group = new EntryGroup(List.of(
                new LedgerEntry(1L, new BigDecimal("-50.0000"), "USD"),
                new LedgerEntry(2L, new BigDecimal("50.00"), "USD")));

        assertThat(group.entries()).hasSize(2);
    }

    /**
     * A lone entry is rejected by the balance check rather than a separate
     * guard: one entry can only sum to zero by being zero, which LedgerEntry
     * already forbids.
     */
    @Test
    void entryGroupRejectsALoneEntry() {
        assertThatExceptionOfType(IllegalArgumentException.class)
                .isThrownBy(() -> new EntryGroup(List.of(
                        new LedgerEntry(1L, new BigDecimal("50.00"), "USD"))))
                .withMessageContaining("sum to zero");
    }

    @Test
    void emptyEntryGroupIsLegal() {
        assertThat(EntryGroup.empty().isEmpty()).isTrue();
        assertThat(EntryGroup.empty().entries()).isEmpty();
    }

    @Test
    void entryGroupIsImmutable() {
        List<LedgerEntry> source = new ArrayList<>(List.of(
                new LedgerEntry(1L, new BigDecimal("-50.00"), "USD"),
                new LedgerEntry(2L, new BigDecimal("50.00"), "USD")));

        EntryGroup group = new EntryGroup(source);
        source.clear();

        assertThat(group.entries())
                .as("the group must not be reshaped by mutating the list it was built from")
                .hasSize(2);
        assertThatExceptionOfType(UnsupportedOperationException.class)
                .isThrownBy(() -> group.entries().clear());
    }

    @Test
    void ledgerEntryRejectsZero() {
        assertThatExceptionOfType(IllegalArgumentException.class)
                .isThrownBy(() -> new LedgerEntry(1L, BigDecimal.ZERO, "USD"))
                .withMessageContaining("cannot be zero");
    }

    @Test
    void ledgerEntryReportsItsDirectionFromTheSign() {
        assertThat(new LedgerEntry(1L, new BigDecimal("-1.00"), "USD").isDebit()).isTrue();
        assertThat(new LedgerEntry(1L, new BigDecimal("1.00"), "USD").isDebit()).isFalse();
    }

    @Test
    void ledgerEntryRequiresAmountAndCurrency() {
        assertThatNullPointerException()
                .isThrownBy(() -> new LedgerEntry(1L, null, "USD"));
        assertThatNullPointerException()
                .isThrownBy(() -> new LedgerEntry(1L, BigDecimal.ONE, null));
    }

    @Test
    void transactionEventRequiresItsIdentifiers() {
        assertThatNullPointerException().isThrownBy(() -> new TransactionEvent(
                null, "evt-1", EventType.CAPTURE, 1L, 2L, BigDecimal.ONE, "USD", Instant.now()));
        assertThatNullPointerException().isThrownBy(() -> new TransactionEvent(
                "txn-1", null, EventType.CAPTURE, 1L, 2L, BigDecimal.ONE, "USD", Instant.now()));
        assertThatNullPointerException().isThrownBy(() -> new TransactionEvent(
                "txn-1", "evt-1", null, 1L, 2L, BigDecimal.ONE, "USD", Instant.now()));
    }

    /**
     * movesMoney requires all three parts, so each one missing is covered
     * separately -- a partially populated event is the shape a malformed
     * upstream message would actually arrive in.
     */
    @Test
    void transactionEventReportsWhetherItMovesMoney() {
        assertThat(eventWith(1L, 2L, BigDecimal.ONE).movesMoney()).isTrue();

        assertThat(eventWith(null, null, null).movesMoney()).isFalse();
        assertThat(eventWith(1L, 2L, null).movesMoney())
                .as("no amount means no movement").isFalse();
        assertThat(eventWith(null, 2L, BigDecimal.ONE).movesMoney())
                .as("no payer means no movement").isFalse();
        assertThat(eventWith(1L, null, BigDecimal.ONE).movesMoney())
                .as("no payee means no movement").isFalse();
    }

    private static TransactionEvent eventWith(Long from, Long to, BigDecimal amount) {
        return new TransactionEvent(
                "txn-1", "evt-1", EventType.CAPTURE, from, to, amount, "USD", Instant.now());
    }

    /** Several events legitimately share one externalTxnId; eventId separates them. */
    @Test
    void eventsOfOneTransactionShareTheTransactionIdButNotTheEventId() {
        TransactionEvent authorize = new TransactionEvent(
                "txn-1", "evt-1", EventType.AUTHORIZE, 1L, 2L, BigDecimal.ONE, "USD", Instant.now());
        TransactionEvent capture = new TransactionEvent(
                "txn-1", "evt-2", EventType.CAPTURE, 1L, 2L, BigDecimal.ONE, "USD", Instant.now());

        assertThat(authorize.externalTxnId()).isEqualTo(capture.externalTxnId());
        assertThat(authorize.eventId()).isNotEqualTo(capture.eventId());
        assertThat(authorize).isNotEqualTo(capture);
    }
}
