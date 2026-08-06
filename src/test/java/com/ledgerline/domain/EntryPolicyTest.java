package com.ledgerline.domain;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.List;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

/** What each event type writes to the ledger, and what it refuses to write. */
class EntryPolicyTest {

    private static final long ALICE = 1L;
    private static final long BOB = 2L;

    @Test
    void captureWritesTheForwardPair() {
        EntryGroup group = EntryPolicy.entriesFor(event(EventType.CAPTURE, "50.00"));

        assertThat(group.entries()).hasSize(2);
        assertThat(amountFor(group, ALICE)).isEqualByComparingTo(new BigDecimal("-50.00"));
        assertThat(amountFor(group, BOB)).isEqualByComparingTo(new BigDecimal("50.00"));
    }

    /** A refund is the same movement with the ends swapped. */
    @Test
    void refundWritesTheReversingPair() {
        EntryGroup group = EntryPolicy.entriesFor(event(EventType.REFUND, "50.00"));

        assertThat(group.entries()).hasSize(2);
        assertThat(amountFor(group, ALICE)).isEqualByComparingTo(new BigDecimal("50.00"));
        assertThat(amountFor(group, BOB)).isEqualByComparingTo(new BigDecimal("-50.00"));
    }

    /** Capture then refund nets to zero for both parties. */
    @Test
    void captureFollowedByRefundLeavesBothAccountsFlat() {
        EntryGroup captured = EntryPolicy.entriesFor(event(EventType.CAPTURE, "50.00"));
        EntryGroup refunded = EntryPolicy.entriesFor(event(EventType.REFUND, "50.00"));

        assertThat(amountFor(captured, ALICE).add(amountFor(refunded, ALICE)))
                .isEqualByComparingTo(BigDecimal.ZERO);
        assertThat(amountFor(captured, BOB).add(amountFor(refunded, BOB)))
                .isEqualByComparingTo(BigDecimal.ZERO);
    }

    @ParameterizedTest
    @EnumSource(value = EventType.class, names = { "AUTHORIZE", "SETTLE", "VOID", "EXPIRE" })
    void stateOnlyEventsWriteNothing(EventType type) {
        assertThat(EntryPolicy.entriesFor(event(type, "50.00")).isEmpty()).isTrue();
        assertThat(EntryPolicy.writesEntries(type)).isFalse();
    }

    @ParameterizedTest
    @EnumSource(value = EventType.class, names = { "CAPTURE", "REFUND" })
    void movingEventsAreReportedAsWriting(EventType type) {
        assertThat(EntryPolicy.writesEntries(type)).isTrue();
    }

    @ParameterizedTest
    @EnumSource(value = EventType.class, names = { "CAPTURE", "REFUND" })
    void movingEventWithoutAccountsIsRejected(EventType type) {
        TransactionEvent incomplete = new TransactionEvent(
                "txn-1", "evt-1", type, null, null, new BigDecimal("50.00"), "USD", Instant.now());

        assertThatExceptionOfType(IllegalArgumentException.class)
                .isThrownBy(() -> EntryPolicy.entriesFor(incomplete))
                .withMessageContaining("requires both accounts and an amount");
    }

    @ParameterizedTest
    @EnumSource(value = EventType.class, names = { "CAPTURE", "REFUND" })
    void movingEventWithoutAmountIsRejected(EventType type) {
        TransactionEvent incomplete = new TransactionEvent(
                "txn-1", "evt-1", type, ALICE, BOB, null, "USD", Instant.now());

        assertThatExceptionOfType(IllegalArgumentException.class)
                .isThrownBy(() -> EntryPolicy.entriesFor(incomplete));
    }

    /** Direction is carried by the signs, so a negative input would invert it. */
    @ParameterizedTest
    @EnumSource(value = EventType.class, names = { "CAPTURE", "REFUND" })
    void negativeAmountIsRejected(EventType type) {
        assertThatExceptionOfType(IllegalArgumentException.class)
                .isThrownBy(() -> EntryPolicy.entriesFor(event(type, "-50.00")))
                .withMessageContaining("must be positive");
    }

    @ParameterizedTest
    @EnumSource(value = EventType.class, names = { "CAPTURE", "REFUND" })
    void zeroAmountIsRejected(EventType type) {
        assertThatExceptionOfType(IllegalArgumentException.class)
                .isThrownBy(() -> EntryPolicy.entriesFor(event(type, "0.00")));
    }

    @Test
    void producedEntriesCarryTheEventCurrency() {
        EntryGroup group = EntryPolicy.entriesFor(new TransactionEvent(
                "txn-1", "evt-1", EventType.CAPTURE, ALICE, BOB,
                new BigDecimal("10.00"), "GBP", Instant.now()));

        assertThat(group.entries()).allMatch(entry -> entry.currency().equals("GBP"));
    }

    private static TransactionEvent event(EventType type, String amount) {
        return new TransactionEvent(
                "txn-1", "evt-1", type, ALICE, BOB, new BigDecimal(amount), "USD", Instant.now());
    }

    private static BigDecimal amountFor(EntryGroup group, long accountId) {
        List<LedgerEntry> matching = group.entries().stream()
                .filter(entry -> entry.accountId() == accountId)
                .toList();
        assertThat(matching).hasSize(1);
        return matching.get(0).amount();
    }
}
