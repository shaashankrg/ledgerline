package com.ledgerline.domain;

import java.math.BigDecimal;
import java.util.List;
import java.util.Objects;

/**
 * The balanced set of entries produced by applying one event.
 *
 * The invariant is enforced in the constructor, so an unbalanced group cannot
 * be constructed at all. That is deliberate: the sum-to-zero rule has been
 * checked by the schema, by a query, and by a test up to now, all of them after
 * the fact. Checking it here makes the illegal state unrepresentable, which is
 * the earliest possible point.
 *
 * An empty group is legal and meaningful -- SETTLE, VOID, and EXPIRE advance a
 * transaction without moving money, and forcing them to invent entries would be
 * worse than letting them produce none.
 */
public record EntryGroup(List<LedgerEntry> entries) {

    public EntryGroup {
        Objects.requireNonNull(entries, "entries");
        entries = List.copyOf(entries);

        BigDecimal total = entries.stream()
                .map(LedgerEntry::amount)
                .reduce(BigDecimal.ZERO, BigDecimal::add);

        // compareTo, not equals: BigDecimal equality counts scale, so 0.0000
        // would not equal ZERO despite being the same number.
        if (total.compareTo(BigDecimal.ZERO) != 0) {
            throw new IllegalArgumentException(
                    "entries must sum to zero, summed to " + total.toPlainString());
        }

        // No separate check for a lone entry: one entry can only sum to zero by
        // being zero, and LedgerEntry already refuses that, so the sum check
        // above catches it. A guard here would be a branch no input can reach.
    }

    /** No money moved. Legal for events that only advance state. */
    public static EntryGroup empty() {
        return new EntryGroup(List.of());
    }

    public boolean isEmpty() {
        return entries.isEmpty();
    }
}
