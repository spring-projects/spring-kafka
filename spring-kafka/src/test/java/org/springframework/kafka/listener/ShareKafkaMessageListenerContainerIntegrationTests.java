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
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

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
import org.jspecify.annotations.Nullable;
import org.junit.jupiter.api.Test;

import org.springframework.kafka.core.DefaultShareConsumerFactory;
import org.springframework.kafka.support.ShareAcknowledgment;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.context.EmbeddedKafka;

import static org.assertj.core.api.Assertions.assertThat;

@EmbeddedKafka(
		topics = {
				"share-listener-integration-test",
				"share-container-explicit-test",
				"share-container-implicit-test",
				"share-container-constraint-test",
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
		containerProps.setShareAcknowledgmentMode(ContainerProperties.ShareAcknowledgmentMode.EXPLICIT);

		CountDownLatch latch = new CountDownLatch(3);
		List<String> received = Collections.synchronizedList(new ArrayList<>());
		List<ShareAcknowledgment> acknowledgments = Collections.synchronizedList(new ArrayList<>());

		containerProps.setMessageListener(new AcknowledgingShareConsumerAwareMessageListener<String, String>() {
			@Override
			public void onShareRecord(ConsumerRecord<String, String> record,
					@Nullable ShareAcknowledgment acknowledgment, ShareConsumer<?, ?> consumer) {
				received.add(record.value());
				acknowledgments.add(acknowledgment);

				// Explicitly acknowledge the record
				if (acknowledgment != null) {
					acknowledgment.acknowledge(); // ACCEPT
				}

				latch.countDown();
			}
		});

		ShareKafkaMessageListenerContainer<String, String> container =
				new ShareKafkaMessageListenerContainer<>(factory, containerProps);
		container.setBeanName("explicitAckTestContainer");
		container.start();

		try {
			assertThat(latch.await(30, TimeUnit.SECONDS)).isTrue();
			assertThat(received).hasSize(3);
			assertThat(acknowledgments).hasSize(3);
			assertThat(acknowledgments).allMatch(Objects::nonNull);
			assertThat(acknowledgments).allMatch(ack -> ack.isAcknowledged());
			assertThat(acknowledgments).allMatch(ack -> ack.getAcknowledgmentType() == AcknowledgeType.ACCEPT);
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

		containerProps.setMessageListener(new AcknowledgingShareConsumerAwareMessageListener<String, String>() {
			@Override
			public void onShareRecord(ConsumerRecord<String, String> record,
					@Nullable ShareAcknowledgment acknowledgment, ShareConsumer<?, ?> consumer) {
				received.add(record.value());

				// In implicit mode, acknowledgment should be null
				assertThat(acknowledgment).isNull();

				latch.countDown();
			}
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
		containerProps.setShareAcknowledgmentMode(ContainerProperties.ShareAcknowledgmentMode.EXPLICIT);

		CountDownLatch firstBatchLatch = new CountDownLatch(3);
		CountDownLatch secondBatchLatch = new CountDownLatch(3);
		AtomicInteger processedCount = new AtomicInteger();
		List<ShareAcknowledgment> pendingAcks = Collections.synchronizedList(new ArrayList<>());

		containerProps.setMessageListener(new AcknowledgingShareConsumerAwareMessageListener<String, String>() {
			@Override
			public void onShareRecord(ConsumerRecord<String, String> record,
					@Nullable ShareAcknowledgment acknowledgment, ShareConsumer<?, ?> consumer) {

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
			}
		});

		ShareKafkaMessageListenerContainer<String, String> container =
				new ShareKafkaMessageListenerContainer<>(factory, containerProps);
		container.setBeanName("constraintTestContainer");
		container.start();

		try {
			// Wait for first batch to be processed
			assertThat(firstBatchLatch.await(15, TimeUnit.SECONDS)).isTrue();
			assertThat(pendingAcks).hasSize(3);

			// Wait a bit to ensure no more records are processed while acknowledgments are pending
			Thread.sleep(2000);
			assertThat(processedCount.get()).isEqualTo(3);

			// Acknowledge first batch
			for (ShareAcknowledgment ack : pendingAcks) {
				ack.acknowledge();
			}

			// Produce more records for second batch
			produceTestRecords(bootstrapServers, topic, 3);

			// Now second batch should be processed
			assertThat(secondBatchLatch.await(15, TimeUnit.SECONDS)).isTrue();
			assertThat(processedCount.get()).isEqualTo(6);

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
		containerProps.setShareAcknowledgmentMode(ContainerProperties.ShareAcknowledgmentMode.EXPLICIT);

		CountDownLatch latch = new CountDownLatch(5);
		AtomicInteger errorCount = new AtomicInteger();
		AtomicInteger successCount = new AtomicInteger();

		containerProps.setMessageListener(new AcknowledgingShareConsumerAwareMessageListener<String, String>() {
			@Override
			public void onShareRecord(ConsumerRecord<String, String> record,
					@Nullable ShareAcknowledgment acknowledgment, ShareConsumer<?, ?> consumer) {

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
			}
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
		containerProps.setShareAcknowledgmentMode(ContainerProperties.ShareAcknowledgmentMode.EXPLICIT);

		CountDownLatch firstRoundLatch = new CountDownLatch(3);
		CountDownLatch redeliveryLatch = new CountDownLatch(1);
		Map<String, AcknowledgeType> ackTypes = new ConcurrentHashMap<>();

		containerProps.setMessageListener(new AcknowledgingShareConsumerAwareMessageListener<String, String>() {
			@Override
			public void onShareRecord(ConsumerRecord<String, String> record,
					@Nullable ShareAcknowledgment acknowledgment, ShareConsumer<?, ?> consumer) {

				String key = record.key();

				if ("accept".equals(key)) {
					acknowledgment.acknowledge(AcknowledgeType.ACCEPT);
					ackTypes.put(key, AcknowledgeType.ACCEPT);
					firstRoundLatch.countDown();
				}
				else if ("release".equals(key)) {
					if (!ackTypes.containsKey("release-redelivered")) {
						// First delivery - release it
						acknowledgment.acknowledge(AcknowledgeType.RELEASE);
						ackTypes.put("release-redelivered", AcknowledgeType.RELEASE);
						firstRoundLatch.countDown();
					}
					else {
						// Redelivered - accept it
						acknowledgment.acknowledge(AcknowledgeType.ACCEPT);
						ackTypes.put(key, AcknowledgeType.ACCEPT);
						redeliveryLatch.countDown();
					}
				}
				else if ("reject".equals(key)) {
					acknowledgment.acknowledge(AcknowledgeType.REJECT);
					ackTypes.put(key, AcknowledgeType.REJECT);
					firstRoundLatch.countDown();
				}
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
		containerProps.setShareAcknowledgmentMode(ContainerProperties.ShareAcknowledgmentMode.EXPLICIT);

		AtomicBoolean listenerCalled = new AtomicBoolean(false);
		containerProps.setMessageListener(new AcknowledgingShareConsumerAwareMessageListener<String, String>() {
			@Override
			public void onShareRecord(ConsumerRecord<String, String> record,
					@Nullable ShareAcknowledgment acknowledgment, ShareConsumer<?, ?> consumer) {
				listenerCalled.set(true);
				acknowledgment.acknowledge();
			}
		});

		ShareKafkaMessageListenerContainer<String, String> container =
				new ShareKafkaMessageListenerContainer<>(factory, containerProps);
		container.setBeanName("lifecycleTestContainer");

		// Test initial state
		assertThat(container.isRunning()).isFalse();

		// Test start
		container.start();
		assertThat(container.isRunning()).isTrue();

		// Test processing
		produceTestRecords(bootstrapServers, topic, 1);
		Thread.sleep(3000); // Give time for processing
		assertThat(listenerCalled.get()).isTrue();

		// Test stop
		container.stop();
		assertThat(container.isRunning()).isFalse();

		// Test restart
		listenerCalled.set(false);
		container.start();
		assertThat(container.isRunning()).isTrue();

		produceTestRecords(bootstrapServers, topic, 1);
		Thread.sleep(3000);
		assertThat(listenerCalled.get()).isTrue();

		container.stop();
	}

	private void testBasicMessageListener(DefaultShareConsumerFactory<String, String> factory,
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

	private void testShareConsumerAwareListener(DefaultShareConsumerFactory<String, String> factory,
			String topic, String bootstrapServers, String groupId) throws Exception {

		setShareAutoOffsetResetEarliest(bootstrapServers, groupId);
		produceTestRecords(bootstrapServers, topic, 1);

		ContainerProperties containerProps = new ContainerProperties(topic);
		CountDownLatch latch = new CountDownLatch(1);
		AtomicReference<ShareConsumer<?, ?>> consumerRef = new AtomicReference<>();

		containerProps.setMessageListener(new ShareConsumerAwareMessageListener<String, String>() {
			@Override
			public void onShareRecord(ConsumerRecord<String, String> record, ShareConsumer<?, ?> consumer) {
				assertThat(record).isNotNull();
				assertThat(consumer).isNotNull();
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

	private void testAckListenerInImplicitMode(DefaultShareConsumerFactory<String, String> factory,
			String topic, String bootstrapServers, String groupId) throws Exception {

		setShareAutoOffsetResetEarliest(bootstrapServers, groupId);
		produceTestRecords(bootstrapServers, topic, 1);

		ContainerProperties containerProps = new ContainerProperties(topic);
		// Implicit mode (default)
		CountDownLatch latch = new CountDownLatch(1);

		containerProps.setMessageListener(new AcknowledgingShareConsumerAwareMessageListener<String, String>() {
			@Override
			public void onShareRecord(ConsumerRecord<String, String> record,
					@Nullable ShareAcknowledgment acknowledgment, ShareConsumer<?, ?> consumer) {
				assertThat(record).isNotNull();
				assertThat(consumer).isNotNull();
				// In implicit mode, acknowledgment should be null
				assertThat(acknowledgment).isNull();
				latch.countDown();
			}
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
	private Map<String, Object> createConsumerProps(String bootstrapServers, String groupId, boolean explicit) {
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

	private void produceTestRecords(String bootstrapServers, String topic, int count) throws Exception {
		try (var producer = createProducer(bootstrapServers)) {
			for (int i = 0; i < count; i++) {
				producer.send(new ProducerRecord<>(topic, "key" + i, "value" + i)).get();
			}
		}
	}

	private KafkaProducer<String, String> createProducer(String bootstrapServers) {
		Map<String, Object> producerProps = new HashMap<>();
		producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
		producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
		producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
		return new KafkaProducer<>(producerProps);
	}

	private void setShareAutoOffsetResetEarliest(String bootstrapServers, String groupId) throws Exception {
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
}
