/*
 * Copyright 2026 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.springframework.kafka.listener;

import java.time.Duration;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.Test;

import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.listener.ContainerProperties.AckMode;
import org.springframework.kafka.support.Acknowledgment;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.BDDMockito.given;
import static org.mockito.BDDMockito.willAnswer;
import static org.mockito.Mockito.mock;

/**
 * Deterministic consumer-rebalance simulation for overlapping batch nack sleeps.
 * Uses a Mockito Consumer so no external broker or wall-clock rebalance races.
 */
class NackRebalanceOwnershipTests {

    @SuppressWarnings("unchecked")
    @Test
    void cooperativeAssignmentMustNotStrandOriginalNackPausedPartition() throws Exception {
        ConsumerFactory<Integer, String> factory = mock(ConsumerFactory.class);
        Consumer<Integer, String> consumer = mock(Consumer.class);
        given(factory.createConsumer(eq("nack-mock"), eq("nack-mock-client"), isNull(), any()))
                .willReturn(consumer);
        given(factory.getConfigurationProperties()).willReturn(new HashMap<>());

        TopicPartition a = new TopicPartition("nack-a", 0);
        TopicPartition b = new TopicPartition("nack-b", 0);
        Set<TopicPartition> assigned = ConcurrentHashMap.newKeySet();
        Set<TopicPartition> paused = ConcurrentHashMap.newKeySet();
        AtomicReference<ConsumerRebalanceListener> rebalance = new AtomicReference<>();
        CountDownLatch completedB = new CountDownLatch(1);
        AtomicReference<Set<TopicPartition>> pausedAfterBRecovery = new AtomicReference<>();
        AtomicInteger pollIndex = new AtomicInteger();
        AtomicInteger firstBDelivery = new AtomicInteger();

        given(consumer.assignment()).willReturn(assigned);
        given(consumer.paused()).willAnswer(inv -> Set.copyOf(paused));
        willAnswer(inv -> {
            paused.addAll(inv.getArgument(0));
            return null;
        }).given(consumer).pause(any(Collection.class));
        willAnswer(inv -> {
            paused.removeAll(inv.getArgument(0));
            return null;
        }).given(consumer).resume(any(Collection.class));
        willAnswer(inv -> {
            rebalance.set(inv.getArgument(1));
            return null;
        }).given(consumer).subscribe(any(Collection.class), any(ConsumerRebalanceListener.class));

        given(consumer.poll(any(Duration.class))).willAnswer(inv -> {
            Thread.sleep(20);
            int index = pollIndex.getAndIncrement();
            if (index == 0) {
                assigned.add(a);
                rebalance.get().onPartitionsAssigned(List.of(a));
                return records(a, "a-fail");
            }
            if (index == 1) {
                // Cooperative assignment adds B but retains A during the first nack.
                assigned.add(b);
                rebalance.get().onPartitionsAssigned(List.of(b));
            }
            if (assigned.contains(b) && !paused.contains(b) && firstBDelivery.get() < 2) {
                firstBDelivery.incrementAndGet();
                return records(b, "b-fail");
            }
            return emptyRecords();
        });

        ContainerProperties properties = new ContainerProperties("nack-a", "nack-b");
        properties.setGroupId("nack-mock");
        properties.setClientId("nack-mock-client");
        properties.setAckMode(AckMode.MANUAL_IMMEDIATE);
        properties.setPollTimeout(20);
        properties.setMessageListener((BatchAcknowledgingMessageListener<Integer, String>) (batch, ack) -> {
            ConsumerRecord<Integer, String> record = batch.get(0);
            if ("a-fail".equals(record.value())) {
                ack.nack(0, Duration.ofMillis(700));
            }
            else if ("b-fail".equals(record.value())) {
                if (firstBDelivery.get() == 1) {
                    ack.nack(0, Duration.ofMillis(700));
                }
                else {
                    pausedAfterBRecovery.set(Set.copyOf(paused));
                    ack.acknowledge();
                    completedB.countDown();
                }
            }
        });

        KafkaMessageListenerContainer<Integer, String> container =
                new KafkaMessageListenerContainer<>(factory, properties);
        try {
            container.start();
            assertThat(completedB.await(5, TimeUnit.SECONDS))
                    .as("B should be redelivered after its nack wake")
                    .isTrue();
            assertThat(pausedAfterBRecovery.get())
                    .as("A should no longer be paused after both nack wake deadlines")
                    .doesNotContain(a);
        }
        finally {
            container.stop();
        }
    }

    private static ConsumerRecords<Integer, String> records(TopicPartition partition, String value) {
        return new ConsumerRecords<>(
                Map.of(partition, List.of(new ConsumerRecord<>(
                        partition.topic(), partition.partition(), 0L, 1, value))),
                Map.of());
    }

    private static ConsumerRecords<Integer, String> emptyRecords() {
        return new ConsumerRecords<>(Map.of(), Map.of());
    }
}
