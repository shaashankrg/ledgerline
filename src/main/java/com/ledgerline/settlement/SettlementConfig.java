package com.ledgerline.settlement;

import java.time.Clock;
import java.time.Instant;
import java.util.EnumMap;
import java.util.Map;

/**
 * What a settlement simulator run should produce.
 *
 * Mirrors {@link com.ledgerline.generator.GeneratorConfig}: every decision the
 * simulator makes derives from {@code seed} plus a fixed draw order, and every
 * timestamp derives from {@code baseInstant} plus a deterministic offset --
 * never from a wall clock -- so that the same config reproduces a
 * byte-identical file on any machine, any run.
 *
 * @param runId       identifies the generator run whose messages this batch
 *                    settles. Used only to select the input; it is not written
 *                    into the CSV.
 * @param batchId     identifies this settlement batch in {@code recon_batches}
 *                    and {@code settlement_records}.
 * @param seed        the RNG seed. Every fault decision derives from this, in a
 *                    fixed order, so the same seed replays the same file.
 * @param baseInstant the instant every settled_at is computed relative to.
 *                    Injecting a fixed instant instead of reading the wall
 *                    clock is what makes the file reproducible -- see
 *                    {@code clock} below.
 * @param faultRates  probability per eligible payment of each network fault
 *                    type, from 0.0 to 1.0.
 * @param clock       used only where the caller genuinely wants "now" (there is
 *                    no such place in the simulator itself, since every
 *                    timestamp is derived from {@code baseInstant}) -- carried
 *                    through so a test can substitute {@link Clock#fixed} and
 *                    so no code path is tempted to reach for
 *                    {@link Instant#now()} instead.
 */
public record SettlementConfig(
        String runId,
        String batchId,
        long seed,
        Instant baseInstant,
        Map<NetworkFaultType, Double> faultRates,
        Clock clock) {

    public SettlementConfig {
        faultRates.forEach((type, rate) -> {
            if (rate < 0.0 || rate > 1.0) {
                throw new IllegalArgumentException(
                        type + " rate must be between 0 and 1, was " + rate);
            }
        });

        // Defensive copy: a caller mutating the map afterwards would change
        // what a "reproducible" batch actually reproduced.
        faultRates = new EnumMap<>(faultRates);
    }

    /** A clean batch: no faults, for tests that only care about the honest view. */
    public static SettlementConfig clean(
            String runId, String batchId, long seed, Instant baseInstant, Clock clock) {
        return new SettlementConfig(
                runId, batchId, seed, baseInstant,
                new EnumMap<>(NetworkFaultType.class), clock);
    }

    public double rateOf(NetworkFaultType type) {
        return faultRates.getOrDefault(type, 0.0);
    }
}
