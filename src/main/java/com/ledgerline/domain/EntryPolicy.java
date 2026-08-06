package com.ledgerline.domain;

import java.math.BigDecimal;
import java.util.List;

/**
 * Which events write ledger entries, and what those entries are.
 *
 * Kept separate from the state machine because they answer different
 * questions. The machine says whether an event is allowed; this says what it
 * costs. Conflating them would mean changing the lifecycle every time the
 * accounting treatment changed, and vice versa.
 *
 * The treatment, and the reasoning:
 *
 * AUTHORIZE writes nothing. An authorization is a hold, not a movement -- the
 * payer's money has not gone anywhere, and writing entries for it would make
 * balances claim a transfer that has not happened. Holds belong in a separate
 * available-balance concept, which this ledger does not model.
 *
 * CAPTURE writes the pair. This is the first point at which the movement is
 * committed rather than reserved.
 *
 * SETTLE writes nothing. The money moved at capture; settlement is the scheme
 * confirming its batch cleared. Writing again here would double the transfer.
 *
 * REFUND writes the reversing pair, sending the amount back the way it came.
 * Deliberately new entries rather than deleting the originals: a ledger is
 * append-only, and the history of a refunded payment is that it happened and
 * was then reversed, not that it never happened.
 *
 * VOID and EXPIRE write nothing. Both end an authorization that never became a
 * movement, so there is nothing to reverse.
 */
public final class EntryPolicy {

    private EntryPolicy() {
    }

    /**
     * The entries an event produces.
     *
     * @param event an event already accepted by the state machine
     * @return the balanced group, possibly empty
     * @throws IllegalArgumentException if an event that must move money is
     *                                  missing the accounts or amount to do it
     */
    public static EntryGroup entriesFor(TransactionEvent event) {
        return switch (event.type()) {
            case CAPTURE -> movement(event, Direction.FORWARD);
            case REFUND -> movement(event, Direction.REVERSE);
            case AUTHORIZE, SETTLE, VOID, EXPIRE -> EntryGroup.empty();
        };
    }

    /** True when this event type writes to the ledger. */
    public static boolean writesEntries(EventType type) {
        return type == EventType.CAPTURE || type == EventType.REFUND;
    }

    private static EntryGroup movement(TransactionEvent event, Direction direction) {
        if (!event.movesMoney()) {
            throw new IllegalArgumentException(
                    event.type() + " requires both accounts and an amount");
        }
        if (event.amount().signum() <= 0) {
            // Direction is carried by the entry signs, not by the input, so a
            // negative amount here would silently invert the movement.
            throw new IllegalArgumentException(
                    "amount must be positive, was " + event.amount().toPlainString());
        }

        BigDecimal amount = event.amount();
        long payer = event.fromAccountId();
        long payee = event.toAccountId();

        // A refund is the same movement with the ends swapped.
        long debited = direction == Direction.FORWARD ? payer : payee;
        long credited = direction == Direction.FORWARD ? payee : payer;

        return new EntryGroup(List.of(
                new LedgerEntry(debited, amount.negate(), event.currency()),
                new LedgerEntry(credited, amount, event.currency())));
    }

    private enum Direction {
        FORWARD, REVERSE
    }
}
