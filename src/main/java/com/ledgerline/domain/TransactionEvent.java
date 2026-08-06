package com.ledgerline.domain;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.Objects;

/**
 * One thing that happened to a transaction.
 *
 * The unit the pipeline carries: a message is one event, and a transaction is
 * the sequence of events sharing an externalTxnId.
 *
 * @param externalTxnId identifies the transaction across its whole lifecycle.
 *                      Every event for one payment carries the same value, so
 *                      this is what the state machine looks state up by. It is
 *                      not an idempotency key -- five events legitimately share
 *                      one externalTxnId.
 * @param eventId       identifies this single event. Distinct per event, which
 *                      is what makes an individual message deduplicable when it
 *                      is redelivered.
 * @param type          what happened
 * @param fromAccountId payer. Null for events that move no money.
 * @param toAccountId   payee. Null for events that move no money.
 * @param amount        the movement. Null for events that move no money, which
 *                      is why it is not validated as positive here -- the state
 *                      machine decides which events require one.
 * @param currency      ISO code, upper case by convention
 * @param occurredAt    when the event happened at its source, not when it was
 *                      received. Ordering decisions must not depend on this:
 *                      a clock elsewhere is not a clock here.
 */
public record TransactionEvent(
        String externalTxnId,
        String eventId,
        EventType type,
        Long fromAccountId,
        Long toAccountId,
        BigDecimal amount,
        String currency,
        Instant occurredAt) {

    public TransactionEvent {
        Objects.requireNonNull(externalTxnId, "externalTxnId");
        Objects.requireNonNull(eventId, "eventId");
        Objects.requireNonNull(type, "type");
    }

    /** True when this event carries a money movement to write. */
    public boolean movesMoney() {
        return amount != null && fromAccountId != null && toAccountId != null;
    }
}
