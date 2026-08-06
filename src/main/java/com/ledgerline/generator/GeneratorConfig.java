package com.ledgerline.generator;

import java.util.EnumMap;
import java.util.Map;

/**
 * What a generator run should produce.
 *
 * @param runId           identifies this run's faults in the ground truth table
 * @param seed            the RNG seed. Every decision the generator makes
 *                        derives from this, so the same seed and config replay
 *                        the same stream byte for byte. Without it a run cannot
 *                        be reproduced, and an accuracy number measured against
 *                        an unreproducible stream cannot be compared to
 *                        anything.
 * @param transactionCount how many transactions to produce
 * @param ratePerSecond   target publication rate. Zero means as fast as
 *                        possible, which is what tests want -- pacing a test
 *                        run only makes it slower without making it truer.
 * @param faultRates      probability per transaction of each fault type, from
 *                        0.0 to 1.0. Independent per type, so one transaction
 *                        can draw more than one fault.
 * @param accountIds      the accounts to draw from
 */
public record GeneratorConfig(
        String runId,
        long seed,
        int transactionCount,
        int ratePerSecond,
        Map<FaultType, Double> faultRates,
        java.util.List<Long> accountIds) {

    public GeneratorConfig {
        if (transactionCount < 0) {
            throw new IllegalArgumentException("transactionCount must not be negative");
        }
        if (accountIds.size() < 2) {
            // Every transfer needs two distinct accounts, so one account cannot
            // produce a legal transaction at all.
            throw new IllegalArgumentException("at least two accounts are required");
        }

        faultRates.forEach((type, rate) -> {
            if (rate < 0.0 || rate > 1.0) {
                throw new IllegalArgumentException(
                        type + " rate must be between 0 and 1, was " + rate);
            }
        });

        // Defensive copies: a caller mutating the map afterwards would change
        // what a "reproducible" run actually reproduced.
        faultRates = new EnumMap<>(faultRates);
        accountIds = java.util.List.copyOf(accountIds);
    }

    /** A clean run: no faults, for tests that only care about volume. */
    public static GeneratorConfig clean(
            String runId, long seed, int transactionCount, java.util.List<Long> accountIds) {
        return new GeneratorConfig(
                runId, seed, transactionCount, 0, new EnumMap<>(FaultType.class), accountIds);
    }

    public double rateOf(FaultType type) {
        return faultRates.getOrDefault(type, 0.0);
    }
}
