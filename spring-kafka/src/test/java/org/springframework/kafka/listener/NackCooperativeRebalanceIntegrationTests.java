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
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.CooperativeStickyAssignor;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.Test;

import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.kafka.test.condition.EmbeddedKafkaCondition;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * Broker-backed coverage for a cooperative assignment during an active nack sleep.
 *
 * <p>Unlike the mock-consumer test, this checks the actual group assignment,
 * fetch behaviour and redelivery. The test deliberately uses a long nack
 * window because broker-driven cooperative rebalances are asynchronous.
 */
@EmbeddedKafka(partitions = 1, topics = { NackCooperativeRebalanceIntegrationTests.TOPIC_A,
        NackCooperativeRebalanceIntegrationTests.TOPIC_B })
class NackCooperativeRebalanceIntegrationTests {

    static final String TOPIC_A = "nack-cooperative-a";

    static final String TOPIC_B = "nack-cooperative-b";

    private static final Duration NACK_SLEEP = Duration.ofSeconds(20);

    @Test
    void newPartitionMustNotDeliverRecordsDuringExistingNackSleep() throws Exception {
        EmbeddedKafkaBroker broker = EmbeddedKafkaCondition.getBroker();
        String group = "nack-cooperative-" + System.nanoTime();
        Map<String, Object> consumerProperties = new HashMap<>();
        consumerProperties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, broker.getBrokersAsString());
        consumerProperties.put(ConsumerConfig.GROUP_ID_CONFIG, group);
        consumerProperties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        consumerProperties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        consumerProperties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        consumerProperties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        consumerProperties.put(ConsumerConfig.PARTITION_ASSIGNMENT_STRATEGY_CONFIG,
                CooperativeStickyAssignor.class.getName());

        Map<String, Object> producerProperties = new HashMap<>();
        producerProperties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, broker.getBrokersAsString());
        producerProperties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        producerProperties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);

        DefaultKafkaProducerFactory<String, String> producerFactory =
                new DefaultKafkaProducerFactory<>(producerProperties);
        KafkaTemplate<String, String> template = new KafkaTemplate<>(producerFactory);
        Set<TopicPartition> c1Assignment = ConcurrentHashMap.newKeySet();
        Set<TopicPartition> c2Assignment = ConcurrentHashMap.newKeySet();
        Set<TopicPartition> c1RevokedDuringNack = ConcurrentHashMap.newKeySet();
        AtomicReference<TopicPartition> original = new AtomicReference<>();
        AtomicReference<TopicPartition> added = new AtomicReference<>();
        AtomicLong firstNackNanos = new AtomicLong();
        AtomicInteger originalDeliveries = new AtomicInteger();
        CountDownLatch firstNack = new CountDownLatch(1);
        CountDownLatch originalRedelivered = new CountDownLatch(1);
        CountDownLatch addedDelivered = new CountDownLatch(1);

        KafkaMessageListenerContainer<String, String> c1 = container(
                consumerProperties, group, "c1", c1Assignment, c1RevokedDuringNack,
                () -> firstNack.getCount() == 0);
        KafkaMessageListenerContainer<String, String> c2 = container(
                consumerProperties, group, "c2", c2Assignment, ConcurrentHashMap.newKeySet(), () -> false);

        c1.getContainerProperties().setMessageListener(
                (BatchAcknowledgingMessageListener<String, String>) (records, ack) -> {
                    for (ConsumerRecord<String, String> record : records) {
                        TopicPartition partition = new TopicPartition(record.topic(), record.partition());
                        if (partition.equals(original.get()) && "first-failure".equals(record.value())) {
                            if (originalDeliveries.incrementAndGet() == 1) {
                                firstNackNanos.set(System.nanoTime());
                                ack.nack(0, NACK_SLEEP);
                                firstNack.countDown();
                                return;
                            }
                            originalRedelivered.countDown();
                        }
                        if (partition.equals(added.get()) && "new-partition-record".equals(record.value())) {
                            addedDelivered.countDown();
                        }
                    }
                    ack.acknowledge();
                });
        c2.getContainerProperties().setMessageListener(
                (BatchAcknowledgingMessageListener<String, String>) (records, ack) -> ack.acknowledge());

        try {
            c1.start();
            await().atMost(Duration.ofSeconds(20))
                    .untilAsserted(() -> assertThat(c1Assignment).hasSize(2));
            c2.start();
            await().atMost(Duration.ofSeconds(25)).untilAsserted(() -> {
                assertThat(c1Assignment).hasSize(1);
                assertThat(c2Assignment).hasSize(1);
            });
            original.set(c1Assignment.iterator().next());
            added.set(c2Assignment.iterator().next());

            template.send(original.get().topic(), "first-failure").get(10, TimeUnit.SECONDS);
            assertThat(firstNack.await(15, TimeUnit.SECONDS)).isTrue();

            c2.stop();
            await().atMost(Duration.ofSeconds(12))
                    .untilAsserted(() -> assertThat(c1Assignment).containsExactlyInAnyOrder(original.get(), added.get()));
            assertThat(c1RevokedDuringNack)
                    .as("The original partition must be retained during cooperative assignment")
                    .doesNotContain(original.get());

            assertThat(System.nanoTime())
                    .as("The added partition must be assigned before the first nack expires")
                    .isLessThan(firstNackNanos.get() + NACK_SLEEP.toNanos() - Duration.ofSeconds(2).toNanos());

            template.send(added.get().topic(), "new-partition-record").get(10, TimeUnit.SECONDS);
            assertThat(addedDelivered.await(1, TimeUnit.SECONDS))
                    .as("A cooperative addition must not bypass an active nack pause")
                    .isFalse();

            assertThat(originalRedelivered.await(30, TimeUnit.SECONDS))
                    .as("The original nack-paused partition must resume")
                    .isTrue();
            assertThat(addedDelivered.await(10, TimeUnit.SECONDS))
                    .as("The newly assigned partition must resume after the nack sleep")
                    .isTrue();
        }
        finally {
            c1.stop();
            c2.stop();
            template.destroy();
            producerFactory.destroy();
        }
    }

    private static KafkaMessageListenerContainer<String, String> container(
            Map<String, Object> properties, String group, String id, Set<TopicPartition> assignment,
            Set<TopicPartition> revokedDuringNack, java.util.function.BooleanSupplier nackStarted) {

        ContainerProperties containerProperties = new ContainerProperties(TOPIC_A, TOPIC_B);
        containerProperties.setGroupId(group);
        containerProperties.setAckMode(ContainerProperties.AckMode.MANUAL_IMMEDIATE);
        containerProperties.setPollTimeout(250);
        containerProperties.setConsumerRebalanceListener(new ConsumerAwareRebalanceListener() {

            @Override
            public void onPartitionsRevokedBeforeCommit(Consumer<?, ?> consumer,
                    Collection<TopicPartition> revoked) {
                assignment.removeAll(revoked);
                if (nackStarted.getAsBoolean()) {
                    revokedDuringNack.addAll(revoked);
                }
            }

            @Override
            public void onPartitionsAssigned(Consumer<?, ?> consumer,
                    Collection<TopicPartition> partitions) {
                assignment.addAll(partitions);
            }

        });
        KafkaMessageListenerContainer<String, String> container = new KafkaMessageListenerContainer<>(
                new DefaultKafkaConsumerFactory<>(properties), containerProperties);
        container.setBeanName(id);
        return container;
    }

}
