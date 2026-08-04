package com.ledgerline.api;

import java.util.List;

/**
 * One page of ledger entries.
 *
 * Carries no total count deliberately: counting matching rows means a second
 * scan of the same set on every page request, and nothing in the read path
 * needs it. A client that wants "is there more" reads nextCursor.
 *
 * @param items      the page, newest first
 * @param nextCursor token for the following page, or null when this page is the
 *                   last one
 */
record EntryPageBody(List<LedgerEntryBody> items, String nextCursor) {
}
