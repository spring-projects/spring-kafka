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
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AlterConfigOp;
import org.apache.kafka.clients.admin.ConfigEntry;
import org.apache.kafka.clients.consumer.AcknowledgeType;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ShareConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.awaitility.Awaitility;
import org.jspecify.annotations.Nullable;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentMatchers;

import org.springframework.beans.DirectFieldAccessor;
import org.springframework.core.log.LogAccessor;
import org.springframework.kafka.core.DefaultShareConsumerFactory;
import org.springframework.kafka.support.ShareAcknowledgment;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.kafka.test.utils.KafkaTestUtils;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;

@EmbeddedKafka(
		topics = {
				"share-listener-integration-test",
				"share-container-explicit-test",
				"share-container-implicit-test",
				"share-container-constraint-test",
				"share-container-partial-test",
				"share-container-concurrent-test",
				"share-container-error-test",
				"share-container-mixed-ack-test",
				"share-container-lifecycle-test"
		},
		partitions = 1,
		brokerProperties = {
				"share.coordinator.state.topic.replication.factor=1",
				"share.coordinator.state.topic.min.isr=1"
		}
)
class ShareKafkaMessageListenerContainerIntegrationTests {

	@Test
	void integrationTestShareKafkaMessageListenerContainer(EmbeddedKafkaBroker broker) throws Exception {
		final String topic = "share-listener-integration-test";
		final String groupId = "shareListenerGroup";
		String bootstrapServers = broker.getBrokersAsString();

		// Produce a record
		var producerProps = new java.util.Properties();
		producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
		producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
		producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
		try (var producer = new KafkaProducer<String, String>(producerProps)) {
			producer.send(new ProducerRecord<>(topic, "key", "integration-test-value")).get();
		}

		setShareAutoOffsetResetEarliest(bootstrapServers, groupId);

		var consumerProps = new java.util.HashMap<String, Object>();
		consumerProps.put("bootstrap.servers", bootstrapServers);
		consumerProps.put("key.deserializer", StringDeserializer.class);
		consumerProps.put("value.deserializer", StringDeserializer.class);
		consumerProps.put("group.id", groupId);

		DefaultShareConsumerFactory<String, String> consumerFactory = new DefaultShareConsumerFactory<>(consumerProps);
		ContainerProperties containerProps = new ContainerProperties(topic);
		CountDownLatch latch = new CountDownLatch(1);
		AtomicReference<String> received = new AtomicReference<>();
		containerProps.setMessageListener((MessageListener<String, String>) record -> {
			received.set(record.value());
			latch.countDown();
		});

		ShareKafkaMessageListenerContainer<String, String> container =
				new ShareKafkaMessageListenerContainer<>(consumerFactory, containerProps);
		container.setBeanName("integrationTestShareKafkaMessageListenerContainer");
		container.start();

		try {
			assertThat(latch.await(10, java.util.concurrent.TimeUnit.SECONDS)
					&& "integration-test-value".equals(received.get()))
					.as("Message should be received and have expected value")
					.isTrue();
		}
		finally {
			container.stop();
		}
	}

	@Test
	void shouldSupportExplicitAcknowledgmentMode(EmbeddedKafkaBroker broker) throws Exception {
		String topic = "share-container-explicit-test";
		String groupId = "share-container-explicit-group";
		String bootstrapServers = broker.getBrokersAsString();

		setShareAutoOffsetResetEarliest(bootstrapServers, groupId);
		produceTestRecords(bootstrapServers, topic, 3);

		Map<String, Object> consumerProps = createConsumerProps(bootstrapServers, groupId, true);
		DefaultShareConsumerFactory<String, String> factory = new DefaultShareConsumerFactory<>(consumerProps);

		ContainerProperties containerProps = new ContainerProperties(topic);
		containerProps.setExplicitShareAcknowledgment(true);

		CountDownLatch latch = new CountDownLatch(3);
		List<String> received = Collections.synchronizedList(new ArrayList<>());
		List<ShareAcknowledgment> acknowledgments = Collections.synchronizedList(new ArrayList<>());

		containerProps.setMessageListener((AcknowledgingShareConsumerAwareMessageListener<String, String>) (
				record, acknowledgment, consumer) -> {
			received.add(record.value());
			acknowledgments.add(acknowledgment);

			// Explicitly acknowledge the record
			if (acknowledgment != null) {
				acknowledgment.acknowledge(); // ACCEPT
			}

			latch.countDown();
		});

		ShareKafkaMessageListenerContainer<String, String> container =
				new ShareKafkaMessageListenerContainer<>(factory, containerProps);
		container.setBeanName("explicitAckTestContainer");
		container.start();

		try {
			assertThat(latch.await(30, TimeUnit.SECONDS)).isTrue();
			assertThat(received).hasSize(3);
			assertThat(acknowledgments).hasSize(3)
					.allMatch(Objects::nonNull)
					.allMatch(ShareKafkaMessageListenerContainerIntegrationTests::isAcknowledgedInternal)
					.allMatch(ack -> AcknowledgeType.ACCEPT.equals(getAcknowledgmentTypeInternal(ack)));
		}
		finally {
			container.stop();
		}
	}

	@Test
	void shouldSupportImplicitAcknowledgmentMode(EmbeddedKafkaBroker broker) throws Exception {
		String topic = "share-container-implicit-test";
		String groupId = "share-container-implicit-group";
		String bootstrapServers = broker.getBrokersAsString();

		setShareAutoOffsetResetEarliest(bootstrapServers, groupId);
		produceTestRecords(bootstrapServers, topic, 3);

		Map<String, Object> consumerProps = createConsumerProps(bootstrapServers, groupId, false);
		DefaultShareConsumerFactory<String, String> factory = new DefaultShareConsumerFactory<>(consumerProps);

		ContainerProperties containerProps = new ContainerProperties(topic);
		// Default is implicit mode

		CountDownLatch latch = new CountDownLatch(3);
		List<String> received = Collections.synchronizedList(new ArrayList<>());

		containerProps.setMessageListener((AcknowledgingShareConsumerAwareMessageListener<String, String>) (
				record, acknowledgment, consumer) -> {
			received.add(record.value());

			// In implicit mode, acknowledgment should be null
			assertThat(acknowledgment).isNull();

			latch.countDown();
		});

		ShareKafkaMessageListenerContainer<String, String> container =
				new ShareKafkaMessageListenerContainer<>(factory, containerProps);
		container.setBeanName("implicitAckTestContainer");
		container.start();

		try {
			assertThat(latch.await(30, TimeUnit.SECONDS)).isTrue();
			assertThat(received).hasSize(3);
		}
		finally {
			container.stop();
		}
	}

	@Test
	void shouldEnforceExplicitAcknowledgmentConstraints(EmbeddedKafkaBroker broker) throws Exception {
		String topic = "share-container-constraint-test";
		String groupId = "share-container-constraint-group";
		String bootstrapServers = broker.getBrokersAsString();

		setShareAutoOffsetResetEarliest(bootstrapServers, groupId);
		produceTestRecords(bootstrapServers, topic, 3);

		Map<String, Object> consumerProps = createConsumerProps(bootstrapServers, groupId, true);
		DefaultShareConsumerFactory<String, String> factory = new DefaultShareConsumerFactory<>(consumerProps);

		ContainerProperties containerProps = new ContainerProperties(topic);
		containerProps.setExplicitShareAcknowledgment(true);

		CountDownLatch firstBatchLatch = new CountDownLatch(3);
		CountDownLatch secondBatchLatch = new CountDownLatch(3);
		AtomicInteger processedCount = new AtomicInteger();
		List<ShareAcknowledgment> pendingAcks = Collections.synchronizedList(new ArrayList<>());

		containerProps.setMessageListener((AcknowledgingShareConsumerAwareMessageListener<String, String>) (
				record, acknowledgment, consumer) -> {

			int count = processedCount.incrementAndGet();

			if (count <= 3) {
				// First batch - collect acknowledgments but don't acknowledge yet
				pendingAcks.add(acknowledgment);
				firstBatchLatch.countDown();
			}
			else {
				// Second batch - should only happen after first batch is acknowledged
				acknowledgment.acknowledge();
				secondBatchLatch.countDown();
			}
		});

		ShareKafkaMessageListenerContainer<String, String> container =
				new ShareKafkaMessageListenerContainer<>(factory, containerProps);
		container.setBeanName("constraintTestContainer");
		container.start();

		// Access the first consumer from the consumers list
		LogAccessor logAccessor = spy(KafkaTestUtils.getPropertyValue(container, "consumers[0].logger",
				LogAccessor.class));

		DirectFieldAccessor accessor = new DirectFieldAccessor(container);
		accessor.setPropertyValue("consumers[0].logger", logAccessor);

		try {
			// Wait for first batch to be processed
			assertThat(firstBatchLatch.await(15, TimeUnit.SECONDS)).isTrue();
			assertThat(pendingAcks).hasSize(3);

			// Produce more records for second batch while first is pending
			produceTestRecords(bootstrapServers, topic, 3);

			// Wait for the next poll to be blocked since no explicit acknowledgment has been made yet.
			// this.logger.trace(() -> "Poll blocked waiting for " + this.pendingAcknowledgments.size() +
			//									" acknowledgments");
			Awaitility.await().atMost(15, TimeUnit.SECONDS).untilAsserted(() ->
				verify(logAccessor, atLeastOnce()).trace(ArgumentMatchers.<Supplier<CharSequence>>any())
			);

			assertThat(processedCount.get()).isEqualTo(3);

			// Acknowledge first batch
			for (ShareAcknowledgment ack : pendingAcks) {
				ack.acknowledge();
			}

			// Now second batch should be processed
			assertThat(secondBatchLatch.await(15, TimeUnit.SECONDS)).isTrue();
			assertThat(processedCount.get()).isEqualTo(6);

		}
		finally {
			container.stop();
		}
	}

	@Test
	void shouldHandlePartialAcknowledgmentCorrectly(EmbeddedKafkaBroker broker) throws Exception {
		String topic = "share-container-partial-test";
		String groupId = "share-container-partial-group";
		String bootstrapServers = broker.getBrokersAsString();

		setShareAutoOffsetResetEarliest(bootstrapServers, groupId);

		Map<String, Object> consumerProps = createConsumerProps(bootstrapServers, groupId, true);
		DefaultShareConsumerFactory<String, String> factory = new DefaultShareConsumerFactory<>(consumerProps);

		ContainerProperties containerProps = new ContainerProperties(topic);
		containerProps.setExplicitShareAcknowledgment(true);

		CountDownLatch batchLatch = new CountDownLatch(4);
		CountDownLatch nextPollLatch = new CountDownLatch(1);
		List<ShareAcknowledgment> batchAcks = Collections.synchronizedList(new ArrayList<>());
		AtomicInteger totalProcessed = new AtomicInteger();

		containerProps.setMessageListener((AcknowledgingShareConsumerAwareMessageListener<String, String>) (
				record, acknowledgment, consumer) -> {

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
		});

		ShareKafkaMessageListenerContainer<String, String> container =
				new ShareKafkaMessageListenerContainer<>(factory, containerProps);
		container.setBeanName("partialAckTestContainer");
		container.start();

		// Access the first consumer from the consumers list
		LogAccessor logAccessor = spy(KafkaTestUtils.getPropertyValue(container, "consumers[0].logger",
				LogAccessor.class));

		DirectFieldAccessor accessor = new DirectFieldAccessor(container);
		accessor.setPropertyValue("consumers[0].logger", logAccessor);

		produceTestRecords(bootstrapServers, topic, 4);

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

			// Wait for the next poll to be blocked since one acknowledgment is still pending.
			// this.logger.trace(() -> "Poll blocked waiting for " + this.pendingAcknowledgments.size() +
			//									" acknowledgments");
			Awaitility.await().atMost(15, TimeUnit.SECONDS).untilAsserted(() ->
				verify(logAccessor, atLeastOnce()).trace(ArgumentMatchers.<Supplier<CharSequence>>any())
			);

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
	void shouldHandleProcessingErrorsInExplicitMode(EmbeddedKafkaBroker broker) throws Exception {
		String topic = "share-container-error-test";
		String groupId = "share-container-error-group";
		String bootstrapServers = broker.getBrokersAsString();

		setShareAutoOffsetResetEarliest(bootstrapServers, groupId);
		produceTestRecords(bootstrapServers, topic, 5);

		Map<String, Object> consumerProps = createConsumerProps(bootstrapServers, groupId, true);
		DefaultShareConsumerFactory<String, String> factory = new DefaultShareConsumerFactory<>(consumerProps);

		ContainerProperties containerProps = new ContainerProperties(topic);
		containerProps.setExplicitShareAcknowledgment(true);

		CountDownLatch latch = new CountDownLatch(5);
		AtomicInteger errorCount = new AtomicInteger();
		AtomicInteger successCount = new AtomicInteger();

		containerProps.setMessageListener((AcknowledgingShareConsumerAwareMessageListener<String, String>) (
				record, acknowledgment, consumer) -> {

			// Simulate error for every 3rd record
			if (record.value().endsWith("2")) { // value2
				errorCount.incrementAndGet();
				latch.countDown();
				throw new RuntimeException("Simulated processing error");
			}
			else {
				successCount.incrementAndGet();
				acknowledgment.acknowledge();
			}

			latch.countDown();
		});

		ShareKafkaMessageListenerContainer<String, String> container =
				new ShareKafkaMessageListenerContainer<>(factory, containerProps);
		container.setBeanName("errorTestContainer");
		container.start();

		try {
			assertThat(latch.await(30, TimeUnit.SECONDS)).isTrue();
			assertThat(errorCount.get()).isEqualTo(1);
			assertThat(successCount.get()).isEqualTo(4);
		}
		finally {
			container.stop();
		}
	}

	@Test
	void shouldSupportMixedAcknowledgmentTypes(EmbeddedKafkaBroker broker) throws Exception {
		String topic = "share-container-mixed-ack-test";
		String groupId = "share-container-mixed-ack-group";
		String bootstrapServers = broker.getBrokersAsString();

		setShareAutoOffsetResetEarliest(bootstrapServers, groupId);

		// Produce test records with different keys to identify them
		try (var producer = createProducer(bootstrapServers)) {
			producer.send(new ProducerRecord<>(topic, "accept", "accept-value")).get();
			producer.send(new ProducerRecord<>(topic, "release", "release-value")).get();
			producer.send(new ProducerRecord<>(topic, "reject", "reject-value")).get();
		}

		Map<String, Object> consumerProps = createConsumerProps(bootstrapServers, groupId, true);
		DefaultShareConsumerFactory<String, String> factory = new DefaultShareConsumerFactory<>(consumerProps);

		ContainerProperties containerProps = new ContainerProperties(topic);
		containerProps.setExplicitShareAcknowledgment(true);

		CountDownLatch firstRoundLatch = new CountDownLatch(3);
		CountDownLatch redeliveryLatch = new CountDownLatch(1);
		Map<String, AcknowledgeType> ackTypes = new ConcurrentHashMap<>();

		containerProps.setMessageListener((AcknowledgingShareConsumerAwareMessageListener<String, String>) (
				record, acknowledgment, consumer) -> {

			String key = record.key();

			if ("accept".equals(key)) {
				acknowledgment.acknowledge();
				ackTypes.put(key, AcknowledgeType.ACCEPT);
				firstRoundLatch.countDown();
			}
			else if ("release".equals(key)) {
				if (!ackTypes.containsKey("release-redelivered")) {
					// First delivery - release it
					acknowledgment.release();
					ackTypes.put("release-redelivered", AcknowledgeType.RELEASE);
					firstRoundLatch.countDown();
				}
				else {
					// Redelivered - accept it
					acknowledgment.acknowledge();
					ackTypes.put(key, AcknowledgeType.ACCEPT);
					redeliveryLatch.countDown();
				}
			}
			else if ("reject".equals(key)) {
				acknowledgment.reject();
				ackTypes.put(key, AcknowledgeType.REJECT);
				firstRoundLatch.countDown();
			}
		});

		ShareKafkaMessageListenerContainer<String, String> container =
				new ShareKafkaMessageListenerContainer<>(factory, containerProps);
		container.setBeanName("mixedAckTestContainer");
		container.start();

		try {
			// Wait for first round of processing
			assertThat(firstRoundLatch.await(15, TimeUnit.SECONDS)).isTrue();
			assertThat(ackTypes.get("accept")).isEqualTo(AcknowledgeType.ACCEPT);
			assertThat(ackTypes.get("reject")).isEqualTo(AcknowledgeType.REJECT);

			// Wait for redelivery of released record
			assertThat(redeliveryLatch.await(15, TimeUnit.SECONDS)).isTrue();
			assertThat(ackTypes.get("release")).isEqualTo(AcknowledgeType.ACCEPT); // what got released, was accepted eventually.
			assertThat(ackTypes.get("release-redelivered")).isEqualTo(AcknowledgeType.RELEASE);

		}
		finally {
			container.stop();
		}
	}

	@Test
	void shouldSupportDifferentListenerTypes(EmbeddedKafkaBroker broker) throws Exception {
		String topic = "share-container-implicit-test";
		String groupId = "share-container-listener-types-group";
		String bootstrapServers = broker.getBrokersAsString();

		setShareAutoOffsetResetEarliest(bootstrapServers, groupId);
		produceTestRecords(bootstrapServers, topic, 1);

		Map<String, Object> consumerProps = createConsumerProps(bootstrapServers, groupId, false);
		DefaultShareConsumerFactory<String, String> factory = new DefaultShareConsumerFactory<>(consumerProps);

		// Test 1: Basic MessageListener
		testBasicMessageListener(factory, topic, bootstrapServers, groupId + "-basic");

		// Test 2: ShareConsumerAwareMessageListener
		testShareConsumerAwareListener(factory, topic, bootstrapServers, groupId + "-aware");

		// Test 3: AcknowledgingShareConsumerAwareMessageListener in implicit mode
		testAckListenerInImplicitMode(factory, topic, bootstrapServers, groupId + "-ack-implicit");
	}

	@Test
	void shouldHandleContainerLifecycle(EmbeddedKafkaBroker broker) throws Exception {
		String topic = "share-container-lifecycle-test";
		String groupId = "share-container-lifecycle-group";
		String bootstrapServers = broker.getBrokersAsString();

		setShareAutoOffsetResetEarliest(bootstrapServers, groupId);

		Map<String, Object> consumerProps = createConsumerProps(bootstrapServers, groupId, true);
		DefaultShareConsumerFactory<String, String> factory = new DefaultShareConsumerFactory<>(consumerProps);

		ContainerProperties containerProps = new ContainerProperties(topic);
		containerProps.setExplicitShareAcknowledgment(true);

		CountDownLatch firstProcessingLatch = new CountDownLatch(1);
		CountDownLatch secondProcessingLatch = new CountDownLatch(1);
		AtomicInteger callCount = new AtomicInteger(0);

		containerProps.setMessageListener((AcknowledgingShareConsumerAwareMessageListener<String, String>) (
				record, acknowledgment, consumer) -> {
			int count = callCount.incrementAndGet();
			if (count == 1) {
				firstProcessingLatch.countDown();
			}
			else if (count == 2) {
				secondProcessingLatch.countDown();
			}
			acknowledgment.acknowledge();
		});

		ShareKafkaMessageListenerContainer<String, String> container =
				new ShareKafkaMessageListenerContainer<>(factory, containerProps);
		container.setBeanName("lifecycleTestContainer");

		assertThat(container.isRunning()).isFalse();

		container.start();
		assertThat(container.isRunning()).isTrue();

		produceTestRecords(bootstrapServers, topic, 1);
		assertThat(firstProcessingLatch.await(10, TimeUnit.SECONDS)).isTrue();

		container.stop();
		assertThat(container.isRunning()).isFalse();

		container.start();
		assertThat(container.isRunning()).isTrue();

		produceTestRecords(bootstrapServers, topic, 1);
		assertThat(secondProcessingLatch.await(10, TimeUnit.SECONDS)).isTrue();

		container.stop();
	}

	@Test
	void shouldHandleConcurrentAcknowledgmentAttempts(EmbeddedKafkaBroker broker) throws Exception {
		String topic = "share-container-concurrent-test";
		String groupId = "share-container-concurrent-group";
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

		containerProps.setMessageListener((AcknowledgingShareConsumerAwareMessageListener<String, String>) (
				record, acknowledgment, consumer) -> {
			ackRef.set(acknowledgment);
			processedLatch.countDown();
		});

		ShareKafkaMessageListenerContainer<String, String> container =
				new ShareKafkaMessageListenerContainer<>(factory, containerProps);
		container.setBeanName("concurrentAckTestContainer");
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

	private static void testBasicMessageListener(DefaultShareConsumerFactory<String, String> factory,
			String topic, String bootstrapServers, String groupId) throws Exception {

		setShareAutoOffsetResetEarliest(bootstrapServers, groupId);
		produceTestRecords(bootstrapServers, topic, 1);

		ContainerProperties containerProps = new ContainerProperties(topic);
		CountDownLatch latch = new CountDownLatch(1);

		containerProps.setMessageListener((MessageListener<String, String>) record -> {
			assertThat(record).isNotNull();
			latch.countDown();
		});

		ShareKafkaMessageListenerContainer<String, String> container =
				new ShareKafkaMessageListenerContainer<>(factory, containerProps);
		container.setBeanName("basicListenerTest");
		container.start();

		try {
			assertThat(latch.await(10, TimeUnit.SECONDS)).isTrue();
		}
		finally {
			container.stop();
		}
	}

	private static void testShareConsumerAwareListener(DefaultShareConsumerFactory<String, String> factory,
			String topic, String bootstrapServers, String groupId) throws Exception {

		setShareAutoOffsetResetEarliest(bootstrapServers, groupId);
		produceTestRecords(bootstrapServers, topic, 1);

		ContainerProperties containerProps = new ContainerProperties(topic);
		CountDownLatch latch = new CountDownLatch(1);
		AtomicReference<ShareConsumer<?, ?>> consumerRef = new AtomicReference<>();

		containerProps.setMessageListener(new AcknowledgingShareConsumerAwareMessageListener<String, String>() {
			@Override
			public void onShareRecord(ConsumerRecord<String, String> record,
					@Nullable ShareAcknowledgment acknowledgment, ShareConsumer<?, ?> consumer) {
				assertThat(record).isNotNull();
				assertThat(consumer).isNotNull();
				// In implicit mode, acknowledgment should be null
				assertThat(acknowledgment).isNull();
				consumerRef.set(consumer);
				latch.countDown();
			}
		});

		ShareKafkaMessageListenerContainer<String, String> container =
				new ShareKafkaMessageListenerContainer<>(factory, containerProps);
		container.setBeanName("consumerAwareListenerTest");
		container.start();

		try {
			assertThat(latch.await(10, TimeUnit.SECONDS)).isTrue();
			assertThat(consumerRef.get()).isNotNull();
		}
		finally {
			container.stop();
		}
	}

	private static void testAckListenerInImplicitMode(DefaultShareConsumerFactory<String, String> factory,
			String topic, String bootstrapServers, String groupId) throws Exception {

		setShareAutoOffsetResetEarliest(bootstrapServers, groupId);
		produceTestRecords(bootstrapServers, topic, 1);

		ContainerProperties containerProps = new ContainerProperties(topic);
		// Implicit mode (default)
		CountDownLatch latch = new CountDownLatch(1);

		containerProps.setMessageListener((AcknowledgingShareConsumerAwareMessageListener<String, String>) (
				record, acknowledgment, consumer) -> {
			assertThat(record).isNotNull();
			assertThat(consumer).isNotNull();
			// In implicit mode, acknowledgment should be null
			assertThat(acknowledgment).isNull();
			latch.countDown();
		});

		ShareKafkaMessageListenerContainer<String, String> container =
				new ShareKafkaMessageListenerContainer<>(factory, containerProps);
		container.setBeanName("ackListenerImplicitTest");
		container.start();

		try {
			assertThat(latch.await(10, TimeUnit.SECONDS)).isTrue();
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

	/**
	 * Helper method to access internal acknowledgment type for testing.
	 */
	private static AcknowledgeType getAcknowledgmentTypeInternal(ShareAcknowledgment ack) {
		try {
			java.lang.reflect.Method method = ack.getClass().getDeclaredMethod("getAcknowledgmentType");
			method.setAccessible(true);
			return (AcknowledgeType) method.invoke(ack);
		}
		catch (Exception e) {
			throw new RuntimeException("Failed to access internal acknowledgment type", e);
		}
	}

	// ==================== Concurrency Integration Tests ====================

	@Test
	void shouldProcessRecordsWithMultipleConsumerThreads(EmbeddedKafkaBroker broker) throws Exception {
		String topic = "share-container-concurrency-basic-test";
		String groupId = "share-container-concurrency-basic-group";
		String bootstrapServers = broker.getBrokersAsString();

		// Create topic with multiple partitions to allow better distribution
		broker.addTopics(topic);

		setShareAutoOffsetResetEarliest(bootstrapServers, groupId);

		// Produce more records than consumers to ensure distribution
		int numRecords = 30;
		int concurrency = 3;
		produceTestRecords(bootstrapServers, topic, numRecords);

		Map<String, Object> consumerProps = createConsumerProps(bootstrapServers, groupId, false);
		DefaultShareConsumerFactory<String, String> factory = new DefaultShareConsumerFactory<>(consumerProps);

		ContainerProperties containerProps = new ContainerProperties(topic);
		CountDownLatch latch = new CountDownLatch(numRecords);
		List<String> receivedValues = Collections.synchronizedList(new ArrayList<>());

		containerProps.setMessageListener((MessageListener<String, String>) record -> {
			receivedValues.add(record.value());
			latch.countDown();
		});

		ShareKafkaMessageListenerContainer<String, String> container =
				new ShareKafkaMessageListenerContainer<>(factory, containerProps);
		container.setBeanName("concurrencyBasicTest");
		container.setConcurrency(concurrency);

		container.start();

		try {
			assertThat(latch.await(30, TimeUnit.SECONDS))
					.as("All %d records should be processed", numRecords)
					.isTrue();

			assertThat(receivedValues).hasSize(numRecords);
		}
		finally {
			container.stop();
		}
	}

	@Test
	void shouldAggregateMetricsFromMultipleConsumers(EmbeddedKafkaBroker broker) throws Exception {
		String topic = "share-container-concurrency-metrics-test";
		String groupId = "share-container-concurrency-metrics-group";
		String bootstrapServers = broker.getBrokersAsString();

		broker.addTopics(topic);
		setShareAutoOffsetResetEarliest(bootstrapServers, groupId);

		int concurrency = 4;
		Map<String, Object> consumerProps = createConsumerProps(bootstrapServers, groupId, false);
		DefaultShareConsumerFactory<String, String> factory = new DefaultShareConsumerFactory<>(consumerProps);

		ContainerProperties containerProps = new ContainerProperties(topic);
		containerProps.setMessageListener((MessageListener<String, String>) record -> {
			// Simple consumer
		});

		ShareKafkaMessageListenerContainer<String, String> container =
				new ShareKafkaMessageListenerContainer<>(factory, containerProps);
		container.setBeanName("concurrencyMetricsTest");
		container.setConcurrency(concurrency);
		container.start();

		try {
			// Wait for all consumers to initialize and register metrics
			Awaitility.await()
					.atMost(10, TimeUnit.SECONDS)
					.untilAsserted(() -> {
						var metrics = container.metrics();
						assertThat(metrics)
								.as("Metrics should be available from all %d consumers", concurrency)
								.hasSize(concurrency);
					});

			var metrics = container.metrics();

			// Verify each consumer has a unique client ID
			assertThat(metrics.keySet())
					.as("Each consumer should have unique client ID")
					.hasSize(concurrency);

			// Verify client IDs follow the pattern: beanName-0, beanName-1, etc.
			for (String clientId : metrics.keySet()) {
				assertThat(clientId)
						.as("Client ID should contain bean name")
						.contains("concurrencyMetricsTest");
			}
		}
		finally {
			container.stop();
		}
	}

	@Test
	void shouldHandleConcurrencyWithExplicitAcknowledgment(EmbeddedKafkaBroker broker) throws Exception {
		String topic = "share-container-concurrency-explicit-test";
		String groupId = "share-container-concurrency-explicit-group";
		String bootstrapServers = broker.getBrokersAsString();

		broker.addTopics(topic);
		setShareAutoOffsetResetEarliest(bootstrapServers, groupId);

		int numRecords = 15;
		int concurrency = 3;
		produceTestRecords(bootstrapServers, topic, numRecords);

		Map<String, Object> consumerProps = createConsumerProps(bootstrapServers, groupId, true);
		DefaultShareConsumerFactory<String, String> factory = new DefaultShareConsumerFactory<>(consumerProps);

		ContainerProperties containerProps = new ContainerProperties(topic);
		containerProps.setExplicitShareAcknowledgment(true);

		CountDownLatch latch = new CountDownLatch(numRecords);
		AtomicInteger acceptCount = new AtomicInteger();
		AtomicInteger rejectCount = new AtomicInteger();

		containerProps.setMessageListener((AcknowledgingShareConsumerAwareMessageListener<String, String>) (
				record, acknowledgment, consumer) -> {
			// Reject every 5th record, accept others
			int recordNum = Integer.parseInt(record.value().substring(5)); // "value0" -> 0
			if (recordNum % 5 == 0) {
				acknowledgment.reject();
				rejectCount.incrementAndGet();
			}
			else {
				acknowledgment.acknowledge();
				acceptCount.incrementAndGet();
			}
			latch.countDown();
		});

		ShareKafkaMessageListenerContainer<String, String> container =
				new ShareKafkaMessageListenerContainer<>(factory, containerProps);
		container.setBeanName("concurrencyExplicitTest");
		container.setConcurrency(concurrency);
		container.start();

		try {
			assertThat(latch.await(30, TimeUnit.SECONDS))
					.as("All records should be processed with explicit acknowledgment")
					.isTrue();

			assertThat(acceptCount.get() + rejectCount.get())
					.as("Total acknowledgments should equal number of records")
					.isEqualTo(numRecords);

			assertThat(rejectCount.get())
					.as("Expected number of rejections")
					.isEqualTo(3); // Records 0, 5, 10
		}
		finally {
			container.stop();
		}
	}

	@Test
	void shouldStopAllConsumerThreadsGracefully(EmbeddedKafkaBroker broker) throws Exception {
		String topic = "share-container-concurrency-lifecycle-test";
		String groupId = "share-container-concurrency-lifecycle-group";
		String bootstrapServers = broker.getBrokersAsString();

		broker.addTopics(topic);
		setShareAutoOffsetResetEarliest(bootstrapServers, groupId);

		int concurrency = 5;
		Map<String, Object> consumerProps = createConsumerProps(bootstrapServers, groupId, false);
		DefaultShareConsumerFactory<String, String> factory = new DefaultShareConsumerFactory<>(consumerProps);

		ContainerProperties containerProps = new ContainerProperties(topic);
		AtomicInteger processedCount = new AtomicInteger();

		containerProps.setMessageListener((MessageListener<String, String>) record -> {
			processedCount.incrementAndGet();
		});

		ShareKafkaMessageListenerContainer<String, String> container =
				new ShareKafkaMessageListenerContainer<>(factory, containerProps);
		container.setBeanName("concurrencyLifecycleTest");
		container.setConcurrency(concurrency);

		// Verify initial state
		assertThat(container.isRunning()).isFalse();
		assertThat(container.metrics()).isEmpty();

		// Start container
		container.start();
		assertThat(container.isRunning()).isTrue();

		// Wait for consumers to initialize
		Awaitility.await()
				.atMost(10, TimeUnit.SECONDS)
				.untilAsserted(() -> assertThat(container.metrics()).hasSize(concurrency));

		// Produce some records
		produceTestRecords(bootstrapServers, topic, 10);

		// Give some time for processing
		Awaitility.await()
				.atMost(10, TimeUnit.SECONDS)
				.untilAsserted(() -> assertThat(processedCount.get()).isGreaterThan(0));

		int processedBeforeStop = processedCount.get();

		// Stop the container
		container.stop();
		assertThat(container.isRunning()).isFalse();

		// Verify metrics are cleared after stop
		Awaitility.await()
				.atMost(5, TimeUnit.SECONDS)
				.untilAsserted(() -> assertThat(container.metrics()).isEmpty());

		// Verify container can be restarted
		container.start();
		assertThat(container.isRunning()).isTrue();

		// Verify consumers are recreated
		Awaitility.await()
				.atMost(10, TimeUnit.SECONDS)
				.untilAsserted(() -> assertThat(container.metrics()).hasSize(concurrency));

		// Produce more records
		produceTestRecords(bootstrapServers, topic, 5);

		// Verify processing continues after restart
		Awaitility.await()
				.atMost(10, TimeUnit.SECONDS)
				.untilAsserted(() -> assertThat(processedCount.get()).isGreaterThan(processedBeforeStop));

		// Final stop
		container.stop();
		assertThat(container.isRunning()).isFalse();
	}

}
