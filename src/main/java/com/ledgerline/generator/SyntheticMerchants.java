package com.ledgerline.generator;

import java.util.List;
import java.util.Random;

/**
 * Assigns a merchant identity to a payment.
 *
 * Neither {@code transactions} (before the merchant_id column this class's
 * introduction motivated) nor the ledger's own account model carries a
 * merchant concept with useful cardinality -- there are only six accounts
 * seeded in the whole database. This class is a synthetic layer, independent
 * of the ledger account: a merchant identity is derived deterministically
 * from a seed and a caller-supplied key, using a sub-seeded {@link Random}
 * rather than a draw from the fault-selection RNG. That independence matters
 * -- threading this through the shared RNG would shift every fault
 * decision's draw position whenever this feature changed, exactly the hazard
 * {@link TransactionGenerator}'s own class comment warns against for fault
 * draws.
 *
 * The key is the payment's positional index within its run, not its
 * externalTxnId. externalTxnId embeds the run id
 * ({@code runId + "-txn-" + index}), and two runs sharing a seed but
 * different run ids -- exactly what
 * {@code TransactionGeneratorTest.sameSeedProducesAnIdenticalStream} exists
 * to verify -- must still assign the same merchant to "the payment at
 * position 11", or that test's own stated invariant ("what must match is the
 * shape and the numbers, not the label identifying which run produced them")
 * breaks. An earlier version of this class keyed on externalTxnId directly
 * and failed exactly that test.
 *
 * This looks, at a glance, like the exact hazard warned about two paragraphs
 * up -- "don't key off a position in a sequence." It is not the same thing,
 * and the distinction is worth being precise about so the next reader doesn't
 * "fix" it back to externalTxnId: {@code index} here is
 * {@link TransactionGenerator#planTransaction}'s loop counter, assigned
 * before any conditional RNG consumption happens for that transaction, so it
 * is a stable enumeration -- transaction 11 is always transaction 11 for a
 * given {@code transactionCount}, independent of which faults fired for
 * transactions 0 through 10. What the earlier paragraph warns against is
 * keying off a *draw's* position in a random sequence, which shifts when
 * upstream draws change. Those are different things that happen to both be
 * called "index." This key is only stable as long as the number and order of
 * planned transactions never depends on a config value being varied across a
 * comparison (e.g. a fault rate) -- true today, since transactionCount is
 * fixed per run and faults are decided per-transaction, not by skipping or
 * reordering transactions.
 *
 * Lives in the generator package, not settlement, and is called at payment
 * creation time (see {@link TransactionGenerator#planTransaction}) rather
 * than by the settlement simulator. That placement is the whole point: a
 * reconciliation engine reads the ledger and must never know the seed this
 * class derives from, so a merchant identity computed only inside the
 * simulator would exist on one side of the comparison and not the other --
 * useless for matching, however high its cardinality. Deriving it once, at
 * creation, and carrying it on {@code TransactionMessage} into both
 * {@code transactions.merchant_id} (via the consumer's write path) and the
 * settlement file (via {@link com.ledgerline.settlement.SettlementSimulator}
 * reading it back off the same published record it already sources from) is
 * what makes it a genuine join key instead of a value that agrees with
 * itself by construction.
 *
 * The distribution is deliberately uneven -- a handful of high-volume
 * merchants and a long tail of low-volume ones -- because a uniform spread
 * makes fuzzy matching artificially easy and would flatter accuracy numbers
 * measured against it. A real merchant population looks like this too: most
 * transaction volume concentrates in a few accounts.
 */
public final class SyntheticMerchants {

    /**
     * Merchant display names, deliberately not evenly weighted below: the
     * first few are drawn far more often than the rest. Twenty names is
     * within the 10-30 target range.
     */
    private static final List<String> NAMES = List.of(
            "Riverside Grocers",
            "Union Square Cafe",
            "Northgate Hardware",
            "Bob's Diner, Inc.",
            "Cedar Lane Pharmacy",
            "Harbor View Bakery",
            "Maple Street Books",
            "Sunrise Auto Parts",
            "Willow Creek Florist",
            "Downtown Print Shop",
            "Lakeside Hardware",
            "Old Mill Antiques",
            "Golden Gate Tailors",
            "Pinehurst Grocers",
            "Blue Ridge Coffee",
            "Elmwood Stationery",
            "Foggy Harbor Fish Market",
            "Crestview Cyclery",
            "Twin Oaks Nursery",
            "Meridian Watch Repair");

    /**
     * Cumulative selection weights matching {@link #NAMES} by index -- a
     * Zipf-like skew, most weight on the first few entries, long tail after.
     * Cumulative rather than per-item so selection is a single comparison
     * pass against one drawn value.
     */
    private static final int[] CUMULATIVE_WEIGHTS = buildCumulativeWeights();

    private SyntheticMerchants() {
    }

    /**
     * The merchant for one payment, deterministic in {@code (seed, key)}.
     *
     * A fresh {@link Random} seeded from a combination of both inputs, used
     * for exactly one draw. This is what keeps merchant assignment
     * independent of every other RNG consumer: it doesn't matter what order
     * payments are processed in, or how many other draws happened first --
     * the same key under the same seed always gets the same merchant.
     *
     * {@code key} is deliberately caller-defined rather than tied to any one
     * id format -- the generator passes a payment's positional index (see
     * the class comment for why), while a fabricated row (e.g. an
     * unknown-txn settlement fault) can pass any deterministic value and
     * still draw from exactly the same distribution as a genuine payment,
     * which is what keeps such a row indistinguishable from a real one by
     * its merchant alone.
     */
    public static String merchantFor(long seed, long key) {
        long subSeed = seed * 0x9E3779B97F4A7C15L ^ (key * 0xC2B2AE3D27D4EB4FL);
        Random random = new Random(subSeed);
        int draw = random.nextInt(CUMULATIVE_WEIGHTS[CUMULATIVE_WEIGHTS.length - 1]);
        for (int i = 0; i < CUMULATIVE_WEIGHTS.length; i++) {
            if (draw < CUMULATIVE_WEIGHTS[i]) {
                return NAMES.get(i);
            }
        }
        return NAMES.get(NAMES.size() - 1);
    }

    private static int[] buildCumulativeWeights() {
        int size = NAMES.size();
        int[] cumulative = new int[size];
        int running = 0;
        for (int i = 0; i < size; i++) {
            // Weight halves every 4 entries: entries 0-3 share the top band,
            // 4-7 the next, and so on, giving a clear high-volume head and a
            // long low-volume tail without needing a floating point curve.
            int weight = Math.max(1, 64 >> (i / 4));
            running += weight;
            cumulative[i] = running;
        }
        return cumulative;
    }
}
