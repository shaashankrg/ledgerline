package com.ledgerline.messaging;

import java.util.Collection;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.actuate.health.Health;
import org.springframework.boot.actuate.health.HealthIndicator;
import org.springframework.kafka.listener.ConsumerAwareRebalanceListener;
import org.springframework.stereotype.Component;

/**
 * Readiness signal driven by the actual Kafka consumer group rebalance
 * callback -- not a timer, not a guess, not "the process didn't crash so it
 * must be fine".
 *
 * <h2>Why this exists</h2>
 *
 * Liveness and readiness answer different questions. Liveness asks "is this
 * process alive, or should Kubernetes restart it?" -- the JVM being up
 * answers that, and Spring Boot's default liveness group already covers it.
 * Readiness asks "can this specific replica currently do useful work?",
 * which is a narrower and more volatile question: a live, healthy JVM can
 * still be structurally unable to process anything for seconds at a time,
 * and a Kafka consumer group rebalance is exactly that window.
 *
 * A rebalance happens whenever group membership changes -- a replica
 * scaling event, a pod restart, a rolling deploy -- and while it's in
 * progress, every consumer in the group has had its partition assignment
 * revoked and is waiting to find out what (if anything) it owns next. A
 * consumer with no assigned partitions cannot poll anything meaningful; if
 * Kubernetes keeps routing traffic to it as "ready" during that window
 * (there's no HTTP traffic here, but the same idea applies to whatever
 * depends on readiness -- e.g. a rolling update proceeding to the next pod,
 * or an operator gating on replica readiness), that's silent data-handling
 * degradation dressed up as a healthy status: the replica reports fine while
 * structurally contributing nothing.
 *
 * <h2>Why a rebalance listener, not a proxy signal</h2>
 *
 * The tempting shortcuts -- "assume ready after N seconds of uptime", "check
 * whether the container thread is alive" -- all answer a different question
 * than "does this replica currently own any partitions". Only the rebalance
 * callback itself knows that, because only it is told, by the broker, the
 * instant group membership starts changing and the instant a new assignment
 * is settled. This class is wired into {@link KafkaConsumerConfig}'s
 * container factory as exactly that callback, not polled or inferred.
 *
 * <h2>Why a count, not a boolean, with {@code ledgerline.consumer.concurrency > 1}</h2>
 *
 * {@code ConcurrentKafkaListenerContainerFactory} with concurrency N creates
 * N independent {@code KafkaMessageListenerContainer}s, each with its own
 * consumer, each rebalancing on its own schedule -- they do not all revoke
 * and re-assign in lockstep. This one Spring bean is registered as the
 * rebalance listener for every one of those N containers (see {@link
 * KafkaConsumerConfig}), so its callbacks fire concurrently from different
 * threads. A single shared boolean flipped by "whichever thread's callback
 * ran most recently" cannot distinguish "this replica holds zero partitions
 * anywhere" from "one of several threads happens to be mid-rebalance while
 * the others still hold theirs" -- the boolean's value would depend on
 * callback ordering, not on the actual state. Tracking a running total of
 * assigned partitions across every thread and reporting ready iff that total
 * is positive is correct under any interleaving: each callback only ever
 * adds or subtracts the exact partition count it was just handed, so the sum
 * is always the true count of partitions this replica currently holds,
 * regardless of which thread's rebalance is contributing at any instant.
 */
@Component
class KafkaConsumerGroupHealth implements ConsumerAwareRebalanceListener, HealthIndicator {

    private static final Logger log = LoggerFactory.getLogger(KafkaConsumerGroupHealth.class);

    /**
     * Running total of partitions currently assigned to this replica, summed
     * across every consumer thread {@code ledgerline.consumer.concurrency}
     * creates. Ready iff positive. Starts at zero: a replica that has never
     * completed its first rebalance holds no partitions yet either, and
     * should not report ready before it does.
     */
    private final AtomicInteger assignedPartitionCount = new AtomicInteger(0);

    @Override
    public void onPartitionsRevokedBeforeCommit(
            org.apache.kafka.clients.consumer.Consumer<?, ?> consumer, Collection<TopicPartition> partitions) {
        // The broker is taking these back, right now, before this consumer
        // thread has anything new to replace them with. Readiness has to
        // reflect this the instant it fires, not when a new assignment
        // eventually lands -- the gap between revocation and re-assignment
        // is precisely the window this indicator exists to make visible.
        int remaining = assignedPartitionCount.addAndGet(-partitions.size());
        log.info("Kafka consumer group rebalance started: partitions revoked {} on this thread, "
                + "{} total partitions remain assigned to this replica", partitions, remaining);
    }

    @Override
    public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
        // Called once this thread's rebalance has settled and it knows what
        // it owns. Adding rather than setting is what makes this correct
        // under concurrency > 1 -- see class Javadoc.
        int total = assignedPartitionCount.addAndGet(partitions.size());
        log.info("Kafka consumer group rebalance completed: partitions assigned {} on this thread, "
                + "{} total partitions now assigned to this replica", partitions, total);
    }

    boolean isReady() {
        return assignedPartitionCount.get() > 0;
    }

    @Override
    public Health health() {
        int count = assignedPartitionCount.get();
        return count > 0
                ? Health.up().withDetail("assignedPartitionCount", count).build()
                : Health.down().withDetail("assignedPartitionCount", count)
                        .withDetail("reason", "no partitions assigned to this replica -- rebalance in "
                                + "progress, not yet joined the group, or more replicas than partitions")
                        .build();
    }
}
