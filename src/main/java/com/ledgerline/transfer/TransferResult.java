package com.ledgerline.transfer;

/**
 * Outcome of a transfer.
 *
 * @param transactionId the transaction that holds the entries -- for a replay,
 *                      the id of the original transaction, not a new one
 * @param replayed      true when this request matched an already-processed
 *                      idempotency key and therefore wrote no new entries
 */
public record TransferResult(long transactionId, boolean replayed) {
}
