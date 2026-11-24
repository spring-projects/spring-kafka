/*
 * Copyright 2026-present the original author or authors.
 */

package org.springframework.kafka.listener;

import java.time.Duration;
import java.time.Instant;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.HashSet;
import java.util.function.BooleanSupplier;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.CooperativeStickyAssignor;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.BatchAcknowledgingConsumerAwareMessageListener;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.kafka.listener.KafkaMessageListenerContainer;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.kafka.test.EmbeddedKafkaKraftBroker;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Broker-backed tests for cooperative rebalancing and overlapping nack pauses.
 *
 * @author Jiwon Lee
 */
class CooperativeNackRebalanceTest {

    private static final String A = "t-a";
    private static final String B = "t-b";
    private static final TopicPartition TP_A = new TopicPartition(A, 0);
    private static final TopicPartition TP_B = new TopicPartition(B, 0);
    private static final Duration SLEEP = Duration.ofSeconds(30);

    private EmbeddedKafkaKraftBroker broker;
    private KafkaTemplate<String, String> producer;
    private KafkaMessageListenerContainer<String, String> c1;
    private KafkaMessageListenerContainer<String, String> c2;
    private final List<String> timeline = new CopyOnWriteArrayList<>();
    private final Set<TopicPartition> c1Assigned = ConcurrentHashMap.newKeySet();
    private final Set<TopicPartition> c2Assigned = ConcurrentHashMap.newKeySet();
    private final Map<String, AtomicInteger> deliveries = new ConcurrentHashMap<>();
    private final Map<String, AtomicLong> nackTime = new ConcurrentHashMap<>();
    private final AtomicReference<Set<TopicPartition>> pausedAtProbe = new AtomicReference<>();
    private final Set<TopicPartition> c1RevokedSinceFirstNack = ConcurrentHashMap.newKeySet();
    private volatile boolean firstNackObserved;

    @BeforeEach
    void startBroker() {
        this.broker = new EmbeddedKafkaKraftBroker(1, 1, A, B);
        this.broker.afterPropertiesSet();
        Map<String, Object> props = Map.of(
                ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, broker.getBrokersAsString(),
                ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class,
                ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class,
                ProducerConfig.ACKS_CONFIG, "all");
        this.producer = new KafkaTemplate<>(new DefaultKafkaProducerFactory<>(props));
    }

    @AfterEach
    void stop() {
        if (c1 != null) c1.stop();
        if (c2 != null) c2.stop();
        if (producer != null) producer.destroy();
        if (broker != null) broker.destroy();
        System.out.println("=== TIMELINE ===");
        timeline.forEach(System.out::println);
    }

    @Test
    void secondNackAfterCooperativeAdditionMustNotStrandOriginalPartition() throws Exception {
        runScenario(true);
    }

    @Test
    void noSecondNackMustResumeOriginalPartition() throws Exception {
        runScenario(false);
    }

    private void runScenario(boolean secondNack) throws Exception {
        String group = "nack-rebalance-" + System.nanoTime();
        this.c1 = container("c1", group, c1Assigned, true, secondNack);
        this.c2 = container("c2", group, c2Assigned, false, secondNack);

        c1.start();
        await("C1 initially owns both", Duration.ofSeconds(20),
                () -> c1Assigned.containsAll(Set.of(TP_A, TP_B)));
        c2.start();
        await("cooperative split", Duration.ofSeconds(25),
                () -> c1Assigned.size() == 1 && c2Assigned.size() == 1);
        // Follow actual assignment: either topic may be owned by C1 initially.
        String originalTopic = c1Assigned.iterator().next().topic();
        String addedTopic = c2Assigned.iterator().next().topic();
        TopicPartition originalPartition = new TopicPartition(originalTopic, 0);
        log("ORIGINAL=" + originalPartition + " ADDED=" + addedTopic);

        send(originalTopic, "fail-1");
        await("first nack", Duration.ofSeconds(15), () -> count("nack:c1:fail-1") >= 1);
        long firstNackAt = nackTime.get("fail-1").get();

        c2.stop();
        await("C1 owns both after rebalance",
                Boolean.getBoolean("nack.useBlockingZero") ? SLEEP.plusSeconds(20) : Duration.ofSeconds(15),
                () -> c1Assigned.containsAll(Set.of(TP_A, TP_B)));
        log("ADDED_PARTITION_WHILE_NACK_SLEEP assigned=" + c1Assigned);
        assertFalse(c1RevokedSinceFirstNack.contains(originalPartition),
                "Original partition was revoked during rebalance; not the cooperative retention scenario");
        send(addedTopic, secondNack ? "fail-2" : "ok-b");
        if (secondNack) {
            boolean expectOverlappingNacks = Boolean.parseBoolean(
                    System.getProperty("nack.expectOverlappingNacks", "false"));
            await("second nack", expectOverlappingNacks ? Duration.ofSeconds(15) : SLEEP.plusSeconds(20),
                    () -> count("nack:c1:fail-2") >= 1);
            long delta = TimeUnit.NANOSECONDS.toMillis(nackTime.get("fail-2").get() - firstNackAt);
            log("SECOND_NACK_DELAY_MS=" + delta + " expectedOverlap=" + expectOverlappingNacks);
            if (expectOverlappingNacks) {
                assertTrue(delta >= 0 && delta < SLEEP.toMillis(),
                        "The original path must overlap; actual delta ms=" + delta);
            }
            else {
                assertTrue(delta >= SLEEP.toMillis(),
                        "U2 must block the second nack during first sleep; actual delta ms=" + delta);
            }
        }
        send(originalTopic, "ok-1");

        await("new partition continues processing", Duration.ofSeconds(2 * SLEEP.toSeconds() + 15),
                () -> count("success:c1:" + (secondNack ? "fail-2" : "ok-b")) >= 1);
        // Do not inspect the pause set before the expected wake. In the control
        // case, the new partition is active while the original nack is still sleeping.
        long latestNackAt = secondNack ? nackTime.get("fail-2").get() : firstNackAt;
        await("nack wake deadline has elapsed", SLEEP.plusSeconds(5),
                () -> System.nanoTime() >= latestNackAt + SLEEP.toNanos()
                        + Duration.ofSeconds(2).toNanos());
        // Read the actual consumer pause set on its own consumer thread.
        send(addedTopic, "probe-b");
        await("probe delivered", Duration.ofSeconds(15), () -> pausedAtProbe.get() != null);
        log("PROBE_PAUSED=" + pausedAtProbe.get());
        assertFalse(pausedAtProbe.get().contains(originalPartition),
                "Original partition remains paused after nack wake; evidence of state loss");
        await("original partition resumes", Duration.ofSeconds(15),
                () -> count("success:c1:ok-1") >= 1);
        assertTrue(count("success:c1:fail-1") >= 1, "Original partition was not redelivered");
    }

    private KafkaMessageListenerContainer<String, String> container(
            String id, String group, Set<TopicPartition> assignment,
            boolean nackFirst, boolean nackSecond) {
        Map<String, Object> consumerProps = Map.of(
                ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, broker.getBrokersAsString(),
                ConsumerConfig.GROUP_ID_CONFIG, group,
                ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class,
                ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class,
                ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest",
                ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false,
                ConsumerConfig.PARTITION_ASSIGNMENT_STRATEGY_CONFIG, CooperativeStickyAssignor.class.getName(),
                ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 10);
        ContainerProperties props = new ContainerProperties(A, B);
        props.setGroupId(group);
        props.setAckMode(ContainerProperties.AckMode.valueOf(
                System.getProperty("nack.ackMode", "MANUAL_IMMEDIATE")));
        props.setPollTimeout(250);
        props.setConsumerRebalanceListener(new org.springframework.kafka.listener.ConsumerAwareRebalanceListener() {
            @Override
            public void onPartitionsRevokedBeforeCommit(Consumer<?, ?> consumer,
                    Collection<TopicPartition> partitions) {
                assignment.removeAll(partitions);
                if ("c1".equals(id) && firstNackObserved) {
                    c1RevokedSinceFirstNack.addAll(partitions);
                }
                log(id + " revoked=" + partitions + " retained=" + assignment);
            }
            @Override
            public void onPartitionsAssigned(Consumer<?, ?> consumer,
                    Collection<TopicPartition> partitions) {
                assignment.addAll(partitions);
                log(id + " assigned=" + partitions + " total=" + assignment
                        + " paused=" + consumer.paused());
            }
        });
        props.setMessageListener((BatchAcknowledgingConsumerAwareMessageListener<String, String>)
                (records, acknowledgment, consumer) -> handle(id, records, acknowledgment, consumer,
                        nackFirst, nackSecond));
        KafkaMessageListenerContainer<String, String> container = new KafkaMessageListenerContainer<>(
                new DefaultKafkaConsumerFactory<String, String>(consumerProps), props);
        container.setBeanName(id);
        return container;
    }

    private void handle(String id, List<ConsumerRecord<String, String>> records,
            Acknowledgment ack, Consumer<?, ?> consumer, boolean nackFirst, boolean nackSecond) {
        log(id + " poll=" + records.stream().map(r -> r.topic() + "-" + r.partition()
                + "@" + r.offset() + "=" + r.value()).toList()
                + " paused=" + consumer.paused());
        for (int i = 0; i < records.size(); i++) {
            String value = records.get(i).value();
            int attempt = deliveries.computeIfAbsent(id + ":" + value,
                    ignored -> new AtomicInteger()).incrementAndGet();
            if (attempt == 1 && ((nackFirst && "fail-1".equals(value))
                    || (nackSecond && "fail-2".equals(value)))) {
                nackTime.computeIfAbsent(value, ignored -> new AtomicLong()).set(System.nanoTime());
                if ("c1".equals(id) && "fail-1".equals(value)) {
                    firstNackObserved = true;
                }
                log("nack:" + id + ":" + value + " index=" + i
                        + " pausedBeforeNack=" + consumer.paused());
                if (Boolean.getBoolean("nack.useBlockingZero")) {
                    // W1: delay on the consumer thread, then issue zero-duration nack.
                    // The rebalance must wait for the listener to return.
                    try {
                        Thread.sleep(SLEEP.toMillis());
                    }
                    catch (InterruptedException ex) {
                        Thread.currentThread().interrupt();
                        throw new IllegalStateException("W1 sleep interrupted", ex);
                    }
                    log("W1_ZERO_NACK_CALL:" + id + ":" + value);
                    ack.nack(i, Duration.ZERO);
                }
                else {
                    ack.nack(i, SLEEP);
                }
                return;
            }
            if ("probe-b".equals(value) && "c1".equals(id)) {
                pausedAtProbe.set(new HashSet<>(consumer.paused()));
            }
            log("success:" + id + ":" + value + " attempt=" + attempt);
        }
        ack.acknowledge();
    }

    private void send(String topic, String value) throws Exception {
        producer.send(topic, value).get(10, TimeUnit.SECONDS);
        log("sent:" + topic + ":" + value);
    }

    private long count(String prefix) {
        return timeline.stream().filter(s -> s.contains(" " + prefix)).count();
    }

    private void log(String msg) {
        timeline.add(Instant.now() + " " + msg);
    }

    private void await(String description, Duration timeout, BooleanSupplier condition)
            throws InterruptedException {
        long deadline = System.nanoTime() + timeout.toNanos();
        while (System.nanoTime() < deadline) {
            if (condition.getAsBoolean()) return;
            Thread.sleep(50);
        }
        fail("Timed out: " + description + " (see timeline)");
    }
}
