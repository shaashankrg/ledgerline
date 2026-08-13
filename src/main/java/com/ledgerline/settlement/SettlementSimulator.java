package com.ledgerline.settlement;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Random;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import com.ledgerline.domain.EventType;
import com.ledgerline.generator.SyntheticMerchants;
import com.ledgerline.generator.TransactionGenerator.GeneratorResult;
import com.ledgerline.messaging.TransactionMessage;
import com.ledgerline.settlement.SettlementBatchRepository.NetworkFault;

/**
 * Builds the card network's independent view of a generator run, as a CSV.
 *
 * This is the *questions*; the ledger is the *answers*. A reconciliation
 * engine, built later, compares the two.
 *
 * The file is derived from {@link GeneratorResult#messages()} -- the
 * generator's own record of what it published to Kafka -- and from nothing
 * else. Deliberately not from a ledger query: a settlement file derived from
 * the ledger would only ever be compared against itself, which finds zero
 * discrepancies always and proves nothing. Reading from the published-message
 * record instead means a bug anywhere downstream -- the consumer, the state
 * machine, the idempotency mechanism, the ledger writer -- shows up here as a
 * discrepancy, because none of those components sit between this class and
 * its input.
 *
 * Determinism is the other load-bearing property. Same seed, same config,
 * byte-identical file, on any machine, any run -- see {@link SettlementConfig}
 * and the class comments on {@link SettlementCsvWriter}. In service of that:
 * one {@link Random}, seeded once and threaded explicitly through every
 * decision below in a fixed order; every timestamp derived from
 * {@code config.baseInstant()}, never {@link Instant#now()}; a
 * {@link LinkedHashMap} wherever iteration order would otherwise affect the
 * file; single-threaded generation throughout.
 */
@Component
public class SettlementSimulator {

    private static final Logger log = LoggerFactory.getLogger(SettlementSimulator.class);

    /** Minor-unit (cent) fee, a fixed 2% of the gross, floored at 1 cent. */
    private static final BigDecimal FEE_RATE = new BigDecimal("0.02");

    /**
     * The range a late-settlement fault shifts a row's settled_at by.
     *
     * A range, not a fixed offset: every honestly-settled row lands at
     * exactly {@code baseInstant + HONEST_SETTLEMENT_LAG}, so a fixed shift
     * would mean every late-settled row landed at exactly one other instant,
     * which is a checkable arithmetic signature -- "settled_at equals X"
     * identifies the fault without ever touching the ledger. Varying the
     * magnitude closes that while keeping every shift well outside the
     * honest window, which is the property the fault is meant to model.
     */
    private static final Duration LATE_SETTLEMENT_MIN_SHIFT = Duration.ofDays(20);
    private static final Duration LATE_SETTLEMENT_MAX_SHIFT = Duration.ofDays(90);

    /** How far after capture a settlement is honestly reported. */
    private static final Duration HONEST_SETTLEMENT_LAG = Duration.ofHours(18);

    private final SettlementBatchRepository repository;

    SettlementSimulator(SettlementBatchRepository repository) {
        this.repository = repository;
    }

    /**
     * Generates a settlement batch from a generator run's published record.
     *
     * @param published what {@link com.ledgerline.generator.TransactionGenerator}
     *                  actually published -- never a ledger query.
     */
    public SettlementResult generate(SettlementConfig config, GeneratorResult published) {
        Random random = new Random(config.seed());

        List<SettlementRow> honest = honestView(config, published.messages());

        List<SettlementRow> finalRows = new ArrayList<>();
        List<NetworkFault> faults = new ArrayList<>();

        applyFaults(config, random, honest, finalRows, faults);

        String csv = SettlementCsvWriter.write(config.batchId(), finalRows);
        byte[] csvBytes = csv.getBytes(StandardCharsets.UTF_8);
        String sha256 = sha256Hex(csvBytes);

        repository.recordBatch(config, finalRows.size(), sha256, published.publishFailures());
        for (NetworkFault fault : faults) {
            repository.recordFault(fault);
        }

        log.info("Batch {} settled {} of {} captured payments, {} faults",
                config.batchId(), finalRows.size(), honest.size(), faults.size());

        return new SettlementResult(config.batchId(), csvBytes, finalRows.size(), faults.size(), sha256);
    }

    /**
     * One row per captured payment, in the order captures were published.
     *
     * Authorizations and voids produce no row -- the network only settles
     * money that actually moved. A payment captured more than once in the
     * published stream (DUPLICATE_PUBLISH, a processor-side fault about
     * message delivery) still only moved once in the real world, so it is
     * folded to a single honest row here; the network file is not where a
     * delivery-layer duplicate would show up.
     *
     * LinkedHashMap: iteration order below must be publication order, not
     * whatever order a HashMap happens to land on.
     *
     * merchant_id is read from the payment record itself
     * ({@code message.merchantId()}), never re-derived here. The generator
     * is what decides a payment's merchant, at creation, and carries it on
     * the message; the same value also reaches {@code transactions.merchant_id}
     * via the consumer's write path. Deriving it independently in both
     * places would make it agree with itself by construction -- a value the
     * later reconciliation comparison could never find fault with -- rather
     * than a genuine join key one side writes and the other reads.
     */
    private List<SettlementRow> honestView(SettlementConfig config, List<TransactionMessage> messages) {
        Map<String, SettlementRow> byTxnId = new LinkedHashMap<>();

        for (TransactionMessage message : messages) {
            if (message.eventType() != EventType.CAPTURE) {
                continue;
            }
            if (byTxnId.containsKey(message.transactionId())) {
                continue;
            }

            long grossMinor = toMinorUnits(message.amount());
            long feeMinor = feeFor(grossMinor);
            Instant settledAt = config.baseInstant().plus(HONEST_SETTLEMENT_LAG);

            byTxnId.put(message.transactionId(), new SettlementRow(
                    message.transactionId(),
                    message.merchantId(),
                    grossMinor,
                    feeMinor,
                    message.currency(),
                    settledAt));
        }

        return List.copyOf(byTxnId.values());
    }

    /**
     * Selects fault targets and applies them, in one deterministic pass.
     *
     * At most one network fault per external_txn_id: if a payment were both
     * dropped and amount-drifted, there would be no single correct answer for
     * a grader to check the detector against, and the accuracy measurement
     * built later would become ambiguous. Enforced here by removing each
     * chosen payment from the eligible pool the moment it is chosen, before
     * the next fault type's selection runs.
     *
     * NETWORK_UNKNOWN_TXN is the one fault type that adds a row rather than
     * touching an existing one, so it draws independently of the honest set
     * and is not subject to the one-fault-per-txn rule (there is no
     * txn to double up on).
     *
     * The rows built below are collected into {@code finalRows} in
     * fault-type-grouped order for convenience, then shuffled into file
     * order at the very end by {@link #shuffle}. The grouping itself must
     * never reach the file: a row's position revealing which block it came
     * from would be exactly the kind of shape-alone tell this class exists
     * to avoid -- "the last N rows are always unknown-txn" is not a property
     * a real settlement batch has, and a matcher that learned it would be
     * scored as detecting a fault it never actually reconciled.
     */
    private void applyFaults(
            SettlementConfig config, Random random,
            List<SettlementRow> honest, List<SettlementRow> finalRows, List<NetworkFault> faults) {

        // A mutable working copy, in honest order, that shrinks as payments
        // are claimed by a fault -- this is what makes "at most one fault per
        // txn" a pass over a shrinking pool rather than a post-hoc check.
        List<SettlementRow> pool = new ArrayList<>(honest);

        List<SettlementRow> dropped = select(pool, random, config.rateOf(NetworkFaultType.NETWORK_DROPPED_ROW));
        List<SettlementRow> drifted = select(pool, random, config.rateOf(NetworkFaultType.NETWORK_AMOUNT_DRIFT));
        List<SettlementRow> duplicated = select(pool, random, config.rateOf(NetworkFaultType.NETWORK_DUPLICATE_ROW));
        List<SettlementRow> lateSettled = select(pool, random, config.rateOf(NetworkFaultType.NETWORK_LATE_SETTLEMENT));
        List<SettlementRow> mangled = select(pool, random, config.rateOf(NetworkFaultType.NETWORK_MANGLED_TXN_ID));

        List<SettlementRow> built = new ArrayList<>();

        // dropped rows are deliberately never added to built below -- the
        // row's absence from the file *is* the fault.

        for (SettlementRow row : drifted) {
            SettlementRow adjusted = drift(random, row);
            built.add(adjusted);
        }

        for (SettlementRow row : duplicated) {
            built.add(row);
            built.add(row);
        }

        for (SettlementRow row : lateSettled) {
            SettlementRow shifted = row.withSettledAt(row.settledAt().plus(lateSettlementShift(random)));
            built.add(shifted);
        }

        // Only the identifier is corrupted. Amount, merchant, and settled_at
        // are carried through untouched, because a row that is wrong in more
        // than one field is not recoverable by attribute and would therefore
        // model a different fault entirely -- see NETWORK_MANGLED_TXN_ID.
        List<MangledRow> mangledRows = new ArrayList<>();
        for (SettlementRow row : mangled) {
            String corrupted = mangleTxnId(random, row.externalTxnId(), honest);
            SettlementRow written = row.withExternalTxnId(corrupted);
            mangledRows.add(new MangledRow(row.externalTxnId(), written));
            built.add(written);
        }

        // Everything left in the pool is untouched -- the honest row, verbatim.
        built.addAll(pool);

        int unknownCount = drawUnknownTxnCount(config, random, honest.size());
        List<SettlementRow> unknownRows = new ArrayList<>();
        for (int i = 0; i < unknownCount; i++) {
            SettlementRow unknown = unknownRow(config, random, i);
            unknownRows.add(unknown);
            built.add(unknown);
        }

        // Labels are derived from what was actually written above, not from
        // the selection decision -- those are different claims, and grading
        // only the emitted rows keeps them from silently diverging.
        //
        // labelUnknown takes unknownRows directly rather than re-deriving
        // membership by inspecting built's contents: the fabricated ids are
        // now shaped exactly like real ones (see unknownRow), specifically
        // so nothing about their format is a giveaway -- which means nothing
        // in this class can recover "which rows were fabricated" by pattern
        // matching either, without keeping its own reference to them.
        labelDropped(config, dropped, faults);
        labelDrifted(config, drifted, built, faults);
        labelDuplicated(config, duplicated, faults);
        labelLateSettled(config, lateSettled, built, faults);
        labelMangled(config, mangledRows, faults);
        labelUnknown(config, unknownRows, faults);

        finalRows.addAll(shuffle(config.seed(), built));
    }

    /**
     * Deterministically reorders rows into file position, breaking the
     * fault-type grouping {@link #applyFaults} built them in.
     *
     * NOT a Fisher-Yates shuffle over the list -- deliberately. An earlier
     * version did exactly that, driven by its own sub-seeded {@link Random}
     * independent of the shared {@code random} everything above draws from,
     * on the theory that independence from the shared RNG's draw position
     * would make row order stable across a fault-rate sweep. That's true for
     * a *fixed* row count, but Fisher-Yates is not stable under insertion or
     * removal: each step's random bound is the current list size, so the
     * exact same RNG sequence produces an unrelated permutation once the
     * list is a different length -- which it always is when fault rates
     * differ, since drops remove rows and duplicates/unknown-txn add them.
     * Two configs differing only in one rate would still scramble the
     * relative order of every row they have in common, defeating the
     * debugging move this exists for: generate two configs during the
     * Day 4 window sweep and diff the files to see exactly what changed.
     *
     * Sorting by a per-row key instead of shuffling the list is what
     * actually survives a changing row count. Each row's key is derived
     * from {@code (seed, externalTxnId)} -- the row's own stable identity,
     * not its position in any list -- so removing a row never changes any
     * other row's key, and the relative order of every row two configs have
     * in common is identical by construction, not by chance. Duplicated
     * rows share one externalTxnId and therefore one key; sorting is stable
     * (see {@link List#sort}'s contract), so the pair stays adjacent in
     * whatever order they were added, which is the only sane place for a
     * literal duplicate to sit.
     */
    private static List<SettlementRow> shuffle(long seed, List<SettlementRow> rows) {
        List<SettlementRow> ordered = new ArrayList<>(rows);
        ordered.sort(java.util.Comparator.comparingLong(row -> rowOrderKey(seed, row.externalTxnId())));
        return ordered;
    }

    /** A stable, deterministic sort key for one row, independent of every other row's presence or absence. */
    private static long rowOrderKey(long seed, String externalTxnId) {
        long subSeed = seed * 0x9E3779B97F4A7C15L ^ ("row-order:" + externalTxnId).hashCode();
        return new Random(subSeed).nextLong();
    }

    /**
     * Draws which rows of {@code pool} are chosen for one fault type, removing
     * them from {@code pool} so a later fault type cannot also claim them.
     *
     * The draw is a single pass in pool order -- not a random sample sized
     * after the fact -- so the sequence of RNG calls does not depend on how
     * many earlier fault types happened to claim rows, keeping the stream
     * stable for a seed even as rates change.
     */
    private List<SettlementRow> select(List<SettlementRow> pool, Random random, double rate) {
        if (rate <= 0.0) {
            return List.of();
        }
        List<SettlementRow> chosen = new ArrayList<>();
        var iterator = pool.iterator();
        while (iterator.hasNext()) {
            SettlementRow row = iterator.next();
            if (random.nextDouble() < rate) {
                chosen.add(row);
                iterator.remove();
            }
        }
        return chosen;
    }

    /**
     * Applies amount drift, recomputing the fee from the drifted gross.
     *
     * A network reporting a drifted gross reports its own fee against that
     * gross, not against a "true" figure it never had -- so recomputing here
     * is the realistic behavior, not just a defensive one. It also closes a
     * leak an earlier version of this method had: every honest row's fee is
     * exactly 2% of its own gross, so a stale fee left over from before the
     * drift would have been a fixed, checkable arithmetic signature that
     * identified a drifted row without ever comparing it to the ledger.
     */
    /**
     * Corrupts a transaction id by transposing two adjacent characters.
     *
     * Transposition rather than nulling, chosen deliberately. A null id is
     * also a legitimate shape -- settlement_records.external_txn_id is
     * nullable precisely so the loader can represent an unparseable field --
     * but it models a *parsing* failure, where the reference never survived
     * into the file at all. Transposition models a transmission or
     * transcription error, where an id is present and looks entirely
     * plausible but resolves to nothing. That is the harder case for a
     * matcher and the more common one in practice: a null is visibly absent,
     * whereas a transposed id will pass any format check and fail only at
     * lookup, which is exactly the situation pass 2 exists to handle.
     *
     * The digits are transposed, not the run-id prefix, so the result stays
     * shaped like a real id from the same run and nothing about its format
     * gives the fault away.
     *
     * The result is checked against every honest id in the run, and the
     * search continues if it collides. A transposition that happened to
     * land on another real transaction's id would silently convert this
     * fault into something else entirely: the row would exact-match the
     * wrong payment, and the answer key would claim a mangled id while the
     * engine reported a perfectly ordinary match against a different
     * transaction. Asserted rather than assumed, per the fault spec.
     */
    private String mangleTxnId(Random random, String externalTxnId, List<SettlementRow> honest) {
        Set<String> realIds = new HashSet<>();
        for (SettlementRow row : honest) {
            realIds.add(row.externalTxnId());
        }

        // Only the trailing index digits are eligible: transposing inside the
        // run-id prefix would produce an id that no longer belongs to this
        // run, which reads as unknown-txn rather than mangled.
        int lastDash = externalTxnId.lastIndexOf('-');
        String prefix = externalTxnId.substring(0, lastDash + 1);
        char[] digits = externalTxnId.substring(lastDash + 1).toCharArray();

        if (digits.length >= 2) {
            // Try each adjacent pair, starting from a seed-derived offset so
            // the choice is deterministic but not always the same position.
            int start = random.nextInt(digits.length - 1);
            for (int attempt = 0; attempt < digits.length - 1; attempt++) {
                int i = (start + attempt) % (digits.length - 1);
                if (digits[i] == digits[i + 1]) {
                    // Transposing equal characters is a no-op: the id would
                    // come out unchanged and the row would match exactly,
                    // producing no fault at all.
                    continue;
                }
                char[] swapped = digits.clone();
                char tmp = swapped[i];
                swapped[i] = swapped[i + 1];
                swapped[i + 1] = tmp;
                String candidate = prefix + new String(swapped);
                if (!realIds.contains(candidate)) {
                    return candidate;
                }
            }
        }

        // Either too short to transpose, or every transposition was a no-op
        // or collided with a real id. Append digits instead -- and check for
        // collision here too.
        //
        // A single appended digit is not safe by itself: run-x-txn-1 with a
        // 3 appended becomes run-x-txn-13, which is a real id the moment the
        // run has 13 or more transactions -- reachable in any batch above
        // roughly a dozen transactions, not an edge case. Caught by
        // mangledIdRowIsRecoveredByFuzzyMatching exact-matching payments to
        // the wrong id instead of fuzzy-recovering them, on a generated
        // batch large enough for the collision to occur; a hand-built
        // fixture never has enough ids in play to hit it, which is why this
        // needed a generated-batch test to surface at all.
        //
        // Two appended digits (00-99) shrinks the same risk by two orders of
        // magnitude but does not eliminate it in principle -- and for a
        // single-digit index (txn-0..txn-9) it is not even small: appending
        // "00".."99" to "txn-1" produces exactly txn-100..txn-199, which are
        // *all* real ids in any run of 200 or more transactions, so this loop
        // collided on every candidate deterministically rather than rarely.
        // Checked and retried anyway rather than trusting the range to be
        // wide enough, same as the transposition loop above.
        for (int suffix = 0; suffix < 100; suffix++) {
            String candidate = externalTxnId + String.format("%02d", suffix);
            if (!realIds.contains(candidate)) {
                return candidate;
            }
        }

        // Every two-digit append collided too (the txn-1 case above). Widen
        // to three digits (000-999) before giving up -- still checked and
        // retried, not trusted blindly.
        for (int suffix = 0; suffix < 1000; suffix++) {
            String candidate = externalTxnId + String.format("%03d", suffix);
            if (!realIds.contains(candidate)) {
                return candidate;
            }
        }

        // A thousand three-digit appends, a hundred two-digit appends, and
        // every adjacent transposition all collided. Only reachable with an
        // implausibly dense id space for one run; fail loudly rather than
        // silently return an id that collides after all.
        throw new IllegalStateException(
                "could not find a non-colliding mangled id for " + externalTxnId
                        + " after exhausting transposition and append candidates");
    }

    /**
     * A mangled row, paired with the id it originally carried.
     *
     * The original is kept because the answer key has to name the payment
     * that was actually corrupted -- which is unrecoverable from the written
     * row, since recovering it is precisely the matcher's job and the
     * simulator must not do it for them.
     */
    private record MangledRow(String originalTxnId, SettlementRow written) {
    }

    private void labelMangled(
            SettlementConfig config, List<MangledRow> mangledRows, List<NetworkFault> faults) {
        for (MangledRow mangledRow : mangledRows) {
            faults.add(NetworkFault.mangledTxnId(
                    config.batchId(),
                    mangledRow.originalTxnId(),
                    mangledRow.written().externalTxnId()));
        }
    }

    private SettlementRow drift(Random random, SettlementRow row) {
        long minDelta = Math.max(1, (long) (row.grossAmountMinor() * 0.01));
        long maxDelta = Math.max(minDelta, (long) (row.grossAmountMinor() * 0.10));
        long delta = minDelta + (maxDelta > minDelta ? (long) (random.nextDouble() * (maxDelta - minDelta)) : 0);
        boolean over = random.nextBoolean();
        long drifted = over ? row.grossAmountMinor() + delta : Math.max(0, row.grossAmountMinor() - delta);
        return row.withGrossAmountMinor(drifted).withFeeMinor(feeFor(drifted));
    }

    private int drawUnknownTxnCount(SettlementConfig config, Random random, int honestSize) {
        double rate = config.rateOf(NetworkFaultType.NETWORK_UNKNOWN_TXN);
        if (rate <= 0.0 || honestSize == 0) {
            return rate > 0.0 ? 1 : 0;
        }
        int count = 0;
        for (int i = 0; i < honestSize; i++) {
            if (random.nextDouble() < rate) {
                count++;
            }
        }
        return count;
    }

    /**
     * An unknown-txn row: a settlement row with no matching captured payment.
     *
     * Drawn from the exact same {@link SyntheticMerchants} distribution as
     * every honest row, keyed on this row's own fabricated transaction id --
     * not a constant sentinel. A fault must not be identifiable from the
     * artifact's shape alone: a hardcoded merchant (or amount) here would let
     * a matcher key on that value and score this fault type as "detected"
     * without reconciling anything, which would make the recall figure for
     * NETWORK_UNKNOWN_TXN measure a tell in the data rather than the engine's
     * ability to find orphan settlement rows. Same reasoning extends to the
     * amount and fee below: drawn per-row from the honest range rather than
     * fixed, so no single value is exclusive to this fault type.
     *
     * The fabricated id itself is shaped exactly like a real one --
     * {@code <runId>-txn-<n>}, the same format {@code TransactionGenerator}
     * uses -- rather than a distinctive "-unknown-N" suffix an earlier
     * version of this method used. That suffix was itself a leak: any row
     * whose id contained the literal text "unknown" was this fault type,
     * with certainty, by string inspection alone. n is drawn from well
     * outside the run's own transaction count so it can never collide with
     * a real id, without the shape of the id itself giving anything away.
     */
    private SettlementRow unknownRow(SettlementConfig config, Random random, int index) {
        // Comfortably outside any realistic transaction count, so this can
        // never collide with a real id from the same run.
        int fabricatedIndex = 1_000_000 + index * 137 + random.nextInt(1000);
        String fabricatedTxnId = config.runId() + "-txn-" + fabricatedIndex;
        String merchantId = SyntheticMerchants.merchantFor(config.seed(), fabricatedIndex);
        long grossMinor = unknownRowAmount(random);
        long feeMinor = feeFor(grossMinor);

        return new SettlementRow(
                fabricatedTxnId,
                merchantId,
                grossMinor,
                feeMinor,
                "USD",
                config.baseInstant().plus(HONEST_SETTLEMENT_LAG));
    }

    /** Same range real captured amounts are drawn from -- see TransactionGenerator's MIN_AMOUNT/AMOUNT_SPREAD. */
    private static long unknownRowAmount(Random random) {
        return 100L + (long) (random.nextDouble() * 50_000L);
    }

    /** A shift magnitude comfortably outside the honest settlement window, varying per row. */
    private static Duration lateSettlementShift(Random random) {
        long minDays = LATE_SETTLEMENT_MIN_SHIFT.toDays();
        long maxDays = LATE_SETTLEMENT_MAX_SHIFT.toDays();
        long days = minDays + (long) (random.nextDouble() * (maxDays - minDays));
        return Duration.ofDays(days);
    }

    private void labelDropped(SettlementConfig config, List<SettlementRow> dropped, List<NetworkFault> faults) {
        for (SettlementRow row : dropped) {
            faults.add(NetworkFault.of(config.batchId(), NetworkFaultType.NETWORK_DROPPED_ROW,
                    row.externalTxnId(), "captured payment never settled by the network"));
        }
    }

    private void labelDrifted(
            SettlementConfig config, List<SettlementRow> originalRows,
            List<SettlementRow> finalRows, List<NetworkFault> faults) {
        for (SettlementRow original : originalRows) {
            SettlementRow written = finalRows.stream()
                    .filter(r -> r.externalTxnId().equals(original.externalTxnId()))
                    .findFirst()
                    .orElseThrow();
            faults.add(NetworkFault.drift(
                    config.batchId(), original.externalTxnId(),
                    original.grossAmountMinor(), written.grossAmountMinor()));
        }
    }

    private void labelDuplicated(SettlementConfig config, List<SettlementRow> duplicated, List<NetworkFault> faults) {
        for (SettlementRow row : duplicated) {
            faults.add(NetworkFault.duplicate(config.batchId(), row.externalTxnId(), 2));
        }
    }

    private void labelLateSettled(
            SettlementConfig config, List<SettlementRow> originalRows,
            List<SettlementRow> finalRows, List<NetworkFault> faults) {
        for (SettlementRow original : originalRows) {
            SettlementRow written = finalRows.stream()
                    .filter(r -> r.externalTxnId().equals(original.externalTxnId()))
                    .findFirst()
                    .orElseThrow();
            faults.add(NetworkFault.lateSettlement(
                    config.batchId(), original.externalTxnId(),
                    original.settledAt(), written.settledAt()));
        }
    }

    private void labelUnknown(
            SettlementConfig config, List<SettlementRow> unknownRows, List<NetworkFault> faults) {
        for (SettlementRow row : unknownRows) {
            faults.add(NetworkFault.of(
                    config.batchId(), NetworkFaultType.NETWORK_UNKNOWN_TXN,
                    row.externalTxnId(), "settlement row with no matching captured payment"));
        }
    }

    /**
     * Converts a ledger-scale (4 decimal places) amount to minor units (2
     * decimal places, i.e. cents), the scale the settlement file reports at.
     *
     * {@link RoundingMode#HALF_UP} is applied once, explicitly, rather than
     * relying on {@link BigDecimal#longValueExact()} to reject a value that
     * doesn't fit -- ledger amounts can legitimately carry sub-cent scale
     * (NUMERIC(19,4)), and a settlement file, like a real card network, only
     * ever reports whole cents.
     */
    private static long toMinorUnits(BigDecimal amount) {
        return amount.movePointRight(2).setScale(0, RoundingMode.HALF_UP).longValueExact();
    }

    private static long feeFor(long grossMinor) {
        long fee = BigDecimal.valueOf(grossMinor).multiply(FEE_RATE)
                .setScale(0, RoundingMode.HALF_UP).longValueExact();
        return Math.max(1, fee);
    }

    private static String sha256Hex(byte[] bytes) {
        try {
            MessageDigest digest = MessageDigest.getInstance("SHA-256");
            byte[] hash = digest.digest(bytes);
            StringBuilder hex = new StringBuilder(hash.length * 2);
            for (byte b : hash) {
                hex.append(String.format(Locale.ROOT, "%02x", b));
            }
            return hex.toString();
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException("SHA-256 not available", e);
        }
    }

    /** What a settlement batch produced. */
    public record SettlementResult(
            String batchId,
            byte[] csvBytes,
            int rowCount,
            int injectedFaults,
            String fileSha256) {
    }
}
