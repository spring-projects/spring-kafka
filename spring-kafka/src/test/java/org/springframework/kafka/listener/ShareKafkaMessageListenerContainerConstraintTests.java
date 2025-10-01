/*
 * Copyright 2025-present the original author or authors.
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

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AlterConfigOp;
import org.apache.kafka.clients.admin.ConfigEntry;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ShareConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.jspecify.annotations.Nullable;
import org.junit.jupiter.api.Test;

import org.springframework.kafka.core.DefaultShareConsumerFactory;
import org.springframework.kafka.support.ShareAcknowledgment;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.context.EmbeddedKafka;

import static org.assertj.core.api.Assertions.assertThat;

@EmbeddedKafka(
		topics = {
				"share-constraint-basic-test",
				"share-constraint-partial-test",
				"share-constraint-timeout-test",
				"share-constraint-concurrent-test"
		},
		partitions = 1,
		brokerProperties = {
				"share.coordinator.state.topic.replication.factor=1",
				"share.coordinator.state.topic.min.isr=1"
		}
)
class ShareKafkaMessageListenerContainerConstraintTests {

	@Test
	void shouldBlockSubsequentPollsUntilAllRecordsAcknowledged(EmbeddedKafkaBroker broker) throws Exception {
		String topic = "share-constraint-basic-test";
		String groupId = "share-constraint-basic-group";
		String bootstrapServers = broker.getBrokersAsString();

		setShareAutoOffsetResetEarliest(bootstrapServers, groupId);

		// Produce first batch
		produceTestRecords(bootstrapServers, topic, 3);

		Map<String, Object> consumerProps = createConsumerProps(bootstrapServers, groupId, true);
		DefaultShareConsumerFactory<String, String> factory = new DefaultShareConsumerFactory<>(consumerProps);

		ContainerProperties containerProps = new ContainerProperties(topic);
		containerProps.setExplicitShareAcknowledgment(true);

		CountDownLatch firstBatchLatch = new CountDownLatch(3);
		CountDownLatch secondBatchLatch = new CountDownLatch(2);
		List<ShareAcknowledgment> firstBatchAcks = Collections.synchronizedList(new ArrayList<>());
		AtomicInteger totalProcessed = new AtomicInteger();

		containerProps.setMessageListener(new AcknowledgingShareConsumerAwareMessageListener<String, String>() {
			@Override
			public void onShareRecord(ConsumerRecord<String, String> record,
					@Nullable ShareAcknowledgment acknowledgment, ShareConsumer<?, ?> consumer) {

				int count = totalProcessed.incrementAndGet();

				if (count <= 3) {
					// First batch - collect acknowledgments but don't acknowledge
					firstBatchAcks.add(acknowledgment);
					firstBatchLatch.countDown();
				}
				else {
					// Second batch - should only happen after first batch acknowledged
					acknowledgment.acknowledge();
					secondBatchLatch.countDown();
				}
			}
		});

		ShareKafkaMessageListenerContainer<String, String> container =
				new ShareKafkaMessageListenerContainer<>(factory, containerProps);
		container.setBeanName("constraintBasicTestContainer");
		container.start();

		try {
			// Wait for first batch
			assertThat(firstBatchLatch.await(15, TimeUnit.SECONDS)).isTrue();
			assertThat(firstBatchAcks).hasSize(3);

			// Produce second batch while first is pending
			produceTestRecords(bootstrapServers, topic, 2);

			// Wait and verify second batch is NOT processed yet
			Thread.sleep(3000);
			assertThat(totalProcessed.get()).isEqualTo(3);
			assertThat(secondBatchLatch.getCount()).isEqualTo(2);

			// Acknowledge first batch
			for (ShareAcknowledgment ack : firstBatchAcks) {
				ack.acknowledge();
			}

			// Now second batch should be processed
			assertThat(secondBatchLatch.await(15, TimeUnit.SECONDS)).isTrue();
			assertThat(totalProcessed.get()).isEqualTo(5);
		}
		finally {
			container.stop();
		}
	}

	@Test
	void shouldHandlePartialAcknowledgmentCorrectly(EmbeddedKafkaBroker broker) throws Exception {
		String topic = "share-constraint-partial-test";
		String groupId = "share-constraint-partial-group";
		String bootstrapServers = broker.getBrokersAsString();

		setShareAutoOffsetResetEarliest(bootstrapServers, groupId);
		produceTestRecords(bootstrapServers, topic, 4);

		Map<String, Object> consumerProps = createConsumerProps(bootstrapServers, groupId, true);
		DefaultShareConsumerFactory<String, String> factory = new DefaultShareConsumerFactory<>(consumerProps);

		ContainerProperties containerProps = new ContainerProperties(topic);
		containerProps.setExplicitShareAcknowledgment(true);

		CountDownLatch batchLatch = new CountDownLatch(4);
		CountDownLatch nextPollLatch = new CountDownLatch(1);
		List<ShareAcknowledgment> batchAcks = Collections.synchronizedList(new ArrayList<>());
		AtomicInteger totalProcessed = new AtomicInteger();

		containerProps.setMessageListener(new AcknowledgingShareConsumerAwareMessageListener<String, String>() {
			@Override
			public void onShareRecord(ConsumerRecord<String, String> record,
					@Nullable ShareAcknowledgment acknowledgment, ShareConsumer<?, ?> consumer) {

				int count = totalProcessed.incrementAndGet();

				if (count <= 4) {
					batchAcks.add(acknowledgment);
					batchLatch.countDown();
				}
				else {
					// This should only happen after all previous records acknowledged
					acknowledgment.acknowledge();
					nextPollLatch.countDown();
				}
			}
		});

		ShareKafkaMessageListenerContainer<String, String> container =
				new ShareKafkaMessageListenerContainer<>(factory, containerProps);
		container.setBeanName("constraintPartialTestContainer");
		container.start();

		try {
			// Wait for batch to be processed
			assertThat(batchLatch.await(15, TimeUnit.SECONDS)).isTrue();
			assertThat(batchAcks).hasSize(4);

			// Acknowledge only first 3 records
			for (int i = 0; i < 3; i++) {
				batchAcks.get(i).acknowledge();
			}

			// Produce more records
			produceTestRecords(bootstrapServers, topic, 1);

			// Should not process new records while one is still pending
			Thread.sleep(3000);
			assertThat(totalProcessed.get()).isEqualTo(4);

			// Acknowledge the last pending record
			batchAcks.get(3).acknowledge();

			// Now should process new records
			assertThat(nextPollLatch.await(15, TimeUnit.SECONDS)).isTrue();
			assertThat(totalProcessed.get()).isEqualTo(5);

		}
		finally {
			container.stop();
		}
	}

	@Test
	void shouldHandleConcurrentAcknowledgmentAttempts(EmbeddedKafkaBroker broker) throws Exception {
		String topic = "share-constraint-concurrent-test";
		String groupId = "share-constraint-concurrent-group";
		String bootstrapServers = broker.getBrokersAsString();

		setShareAutoOffsetResetEarliest(bootstrapServers, groupId);
		produceTestRecords(bootstrapServers, topic, 1);

		Map<String, Object> consumerProps = createConsumerProps(bootstrapServers, groupId, true);
		DefaultShareConsumerFactory<String, String> factory = new DefaultShareConsumerFactory<>(consumerProps);

		ContainerProperties containerProps = new ContainerProperties(topic);
		containerProps.setExplicitShareAcknowledgment(true);

		CountDownLatch processedLatch = new CountDownLatch(1);
		AtomicReference<ShareAcknowledgment> ackRef = new AtomicReference<>();
		AtomicInteger successfulAcks = new AtomicInteger();
		AtomicInteger failedAcks = new AtomicInteger();

		containerProps.setMessageListener(new AcknowledgingShareConsumerAwareMessageListener<String, String>() {
			@Override
			public void onShareRecord(ConsumerRecord<String, String> record,
					@Nullable ShareAcknowledgment acknowledgment, ShareConsumer<?, ?> consumer) {
				ackRef.set(acknowledgment);
				processedLatch.countDown();
			}
		});

		ShareKafkaMessageListenerContainer<String, String> container =
				new ShareKafkaMessageListenerContainer<>(factory, containerProps);
		container.setBeanName("constraintConcurrentTestContainer");
		container.start();

		try {
			// Wait for record to be processed
			assertThat(processedLatch.await(10, TimeUnit.SECONDS)).isTrue();
			ShareAcknowledgment ack = ackRef.get();
			assertThat(ack).isNotNull();

			// Try to acknowledge the same record concurrently from multiple threads
			int numThreads = 10;
			ExecutorService executor = Executors.newFixedThreadPool(numThreads);
			CountDownLatch threadLatch = new CountDownLatch(numThreads);

			for (int i = 0; i < numThreads; i++) {
				executor.submit(() -> {
					try {
						ack.acknowledge();
						successfulAcks.incrementAndGet();
					}
					catch (IllegalStateException e) {
						failedAcks.incrementAndGet();
					}
					finally {
						threadLatch.countDown();
					}
				});
			}

			assertThat(threadLatch.await(10, TimeUnit.SECONDS)).isTrue();
			executor.shutdown();

			// Only one acknowledgment should succeed
			assertThat(successfulAcks.get()).isEqualTo(1);
			assertThat(failedAcks.get()).isEqualTo(numThreads - 1);
			// Check internal state through reflection since isAcknowledged() is no longer public
			assertThat(isAcknowledgedInternal(ack)).isTrue();

		}
		finally {
			container.stop();
		}
	}

	// Utility methods
	private static Map<String, Object> createConsumerProps(String bootstrapServers, String groupId, boolean explicit) {
		Map<String, Object> props = new HashMap<>();
		props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
		props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
		props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
		props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
		if (explicit) {
			props.put("share.acknowledgement.mode", "explicit");
		}
		return props;
	}

	private static void produceTestRecords(String bootstrapServers, String topic, int count) throws Exception {
		try (var producer = createProducer(bootstrapServers)) {
			for (int i = 0; i < count; i++) {
				producer.send(new ProducerRecord<>(topic, "key" + i, "value" + i)).get();
			}
		}
	}

	private static KafkaProducer<String, String> createProducer(String bootstrapServers) {
		Map<String, Object> producerProps = new HashMap<>();
		producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
		producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
		producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
		return new KafkaProducer<>(producerProps);
	}

	private static void setShareAutoOffsetResetEarliest(String bootstrapServers, String groupId) throws Exception {
		Map<String, Object> adminProperties = new HashMap<>();
		adminProperties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
		ConfigEntry entry = new ConfigEntry("share.auto.offset.reset", "earliest");
		AlterConfigOp op = new AlterConfigOp(entry, AlterConfigOp.OpType.SET);
		Map<ConfigResource, Collection<AlterConfigOp>> configs = Map.of(
				new ConfigResource(ConfigResource.Type.GROUP, groupId), List.of(op));
		try (Admin admin = Admin.create(adminProperties)) {
			admin.incrementalAlterConfigs(configs).all().get();
		}
	}

	/**
	 * Helper method to access internal acknowledgment state for testing.
	 * Since isAcknowledged() was removed from the public interface, we use reflection.
	 */
	private static boolean isAcknowledgedInternal(ShareAcknowledgment ack) {
		try {
			java.lang.reflect.Method method = ack.getClass().getDeclaredMethod("isAcknowledged");
			method.setAccessible(true);
			return (Boolean) method.invoke(ack);
		}
		catch (Exception e) {
			throw new RuntimeException("Failed to access internal acknowledgment state", e);
		}
	}
}
