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

import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AlterConfigOp;
import org.apache.kafka.clients.admin.ConfigEntry;
import org.apache.kafka.clients.consumer.AcknowledgeType;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ShareConsumer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.jspecify.annotations.Nullable;
import org.junit.jupiter.api.Test;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.config.ShareKafkaListenerContainerFactory;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.DefaultShareConsumerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.core.ShareConsumerFactory;
import org.springframework.kafka.support.ShareAcknowledgment;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.junit.jupiter.SpringJUnitConfig;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Enhanced integration tests for @KafkaListener with share consumer acknowledgment features.
 *
 * @author Soby Chacko
 * @since 4.0
 */
@SpringJUnitConfig
@DirtiesContext
@EmbeddedKafka(topics = {
		"share-listener-basic-test",
		"share-listener-explicit-ack-test",
		"share-listener-consumer-aware-test",
		"share-listener-ack-consumer-aware-test",
		"share-listener-mixed-ack-test",
		"share-listener-error-handling-test",
		"share-listener-factory-props-test"
},
		brokerProperties = {
				"share.coordinator.state.topic.replication.factor=1",
				"share.coordinator.state.topic.min.isr=1"
		})
class ShareKafkaListenerIntegrationTests {

	@Autowired
	EmbeddedKafkaBroker broker;

	@Autowired
	KafkaTemplate<String, String> kafkaTemplate;

	@Test
	void shouldSupportBasicShareKafkaListener() throws Exception {
		final String topic = "share-listener-basic-test";
		final String groupId = "share-listener-basic-group";
		setShareAutoOffsetResetEarliest(this.broker.getBrokersAsString(), groupId);

		// Send test message
		kafkaTemplate.send(topic, "basic-test-message");

		// Wait for processing
		assertThat(BasicTestListener.latch.await(10, TimeUnit.SECONDS)).isTrue();
		assertThat(BasicTestListener.received.get()).isEqualTo("basic-test-message");
	}

	@Test
	void shouldSupportExplicitAcknowledgmentWithShareAcknowledgment() throws Exception {
		final String topic = "share-listener-explicit-ack-test";
		final String groupId = "share-explicit-ack-group";
		setShareAutoOffsetResetEarliest(this.broker.getBrokersAsString(), groupId);

		// Send test messages
		kafkaTemplate.send(topic, "accept", "accept-message");
		kafkaTemplate.send(topic, "release", "release-message");
		kafkaTemplate.send(topic, "reject", "reject-message");

		// Wait for processing
		assertThat(ExplicitAckTestListener.latch.await(15, TimeUnit.SECONDS)).isTrue();
		assertThat(ExplicitAckTestListener.redeliveryLatch.await(15, TimeUnit.SECONDS)).isTrue();

		// Verify acknowledgment types were used correctly
		assertThat(ExplicitAckTestListener.acknowledgmentTypes).containsKey("accept");
		assertThat(ExplicitAckTestListener.acknowledgmentTypes).containsKey("reject");
		assertThat(ExplicitAckTestListener.acknowledgmentTypes.get("accept")).isEqualTo(AcknowledgeType.ACCEPT);
		assertThat(ExplicitAckTestListener.acknowledgmentTypes.get("reject")).isEqualTo(AcknowledgeType.REJECT);

		// The release message should have been redelivered and then accepted
		assertThat(ExplicitAckTestListener.redeliveredAndAccepted.get()).isTrue();
	}

	@Test
	void shouldSupportShareConsumerAwareListener() throws Exception {
		final String topic = "share-listener-consumer-aware-test";
		final String groupId = "share-consumer-aware-group";
		setShareAutoOffsetResetEarliest(this.broker.getBrokersAsString(), groupId);

		// Send test message
		kafkaTemplate.send(topic, "consumer-aware-message");

		// Wait for processing
		assertThat(ShareConsumerAwareTestListener.latch.await(10, TimeUnit.SECONDS)).isTrue();
		assertThat(ShareConsumerAwareTestListener.received.get()).isEqualTo("consumer-aware-message");
		assertThat(ShareConsumerAwareTestListener.consumerReceived.get()).isNotNull();
	}

	@Test
	void shouldSupportAcknowledgingShareConsumerAwareListener() throws Exception {
		final String topic = "share-listener-ack-consumer-aware-test";
		final String groupId = "share-ack-consumer-aware-group";
		setShareAutoOffsetResetEarliest(this.broker.getBrokersAsString(), groupId);

		// Send test message
		kafkaTemplate.send(topic, "ack-consumer-aware-message");

		// Wait for processing
		assertThat(AckShareConsumerAwareTestListener.latch.await(10, TimeUnit.SECONDS)).isTrue();
		assertThat(AckShareConsumerAwareTestListener.received.get()).isEqualTo("ack-consumer-aware-message");
		assertThat(AckShareConsumerAwareTestListener.consumerReceived.get()).isNotNull();
		assertThat(AckShareConsumerAwareTestListener.acknowledgmentReceived.get()).isNotNull();
		assertThat(isAcknowledgedInternal(AckShareConsumerAwareTestListener.acknowledgmentReceived.get())).isTrue();
	}

	@Test
	void shouldHandleMixedAcknowledgmentScenarios() throws Exception {
		final String topic = "share-listener-mixed-ack-test";
		final String groupId = "share-mixed-ack-group";
		setShareAutoOffsetResetEarliest(this.broker.getBrokersAsString(), groupId);

		// Send multiple test messages with different scenarios
		kafkaTemplate.send(topic, "success1", "success-message-1");
		kafkaTemplate.send(topic, "success2", "success-message-2");
		kafkaTemplate.send(topic, "retry", "retry-message");

		// Wait for processing
		assertThat(MixedAckTestListener.processedLatch.await(15, TimeUnit.SECONDS)).isTrue();
		assertThat(MixedAckTestListener.retryLatch.await(15, TimeUnit.SECONDS)).isTrue();

		// Verify correct processing
		assertThat(MixedAckTestListener.processedCount.get()).isEqualTo(4); // 3 original + 1 retry
		assertThat(MixedAckTestListener.successCount.get()).isEqualTo(3); // 2 success + 1 retry success
		assertThat(MixedAckTestListener.retryCount.get()).isEqualTo(1);
	}

	@Test
	void shouldHandleProcessingErrorsCorrectly() throws Exception {
		final String topic = "share-listener-error-handling-test";
		final String groupId = "share-error-handling-group";
		setShareAutoOffsetResetEarliest(this.broker.getBrokersAsString(), groupId);

		// Send messages that will trigger errors
		kafkaTemplate.send(topic, "success", "success-message");
		kafkaTemplate.send(topic, "error", "error-message");
		kafkaTemplate.send(topic, "success2", "success-message-2");

		// Wait for processing
		assertThat(ErrorHandlingTestListener.latch.await(10, TimeUnit.SECONDS)).isTrue();

		// Verify error handling
		assertThat(ErrorHandlingTestListener.successCount.get()).isEqualTo(2);
		assertThat(ErrorHandlingTestListener.errorCount.get()).isEqualTo(1);
	}

	@Test
	void shouldSupportExplicitAcknowledgmentViaFactoryContainerProperties() throws Exception {
		final String topic = "share-listener-factory-props-test";
		final String groupId = "share-factory-props-group";
		setShareAutoOffsetResetEarliest(this.broker.getBrokersAsString(), groupId);

		// Send test message
		kafkaTemplate.send(topic, "factory-test", "factory-props-message");

		// Wait for processing
		assertThat(FactoryPropsTestListener.latch.await(10, TimeUnit.SECONDS)).isTrue();
		assertThat(FactoryPropsTestListener.received.get()).isEqualTo("factory-props-message");
		assertThat(FactoryPropsTestListener.acknowledgmentReceived.get()).isNotNull();
		assertThat(isAcknowledgedInternal(FactoryPropsTestListener.acknowledgmentReceived.get())).isTrue();
	}

	/**
	 * Sets the share.auto.offset.reset group config to earliest for the given groupId.
	 */
	private static void setShareAutoOffsetResetEarliest(String bootstrapServers, String groupId) throws Exception {
		Map<String, Object> adminProperties = new HashMap<>();
		adminProperties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
		try (Admin admin = Admin.create(adminProperties)) {
			ConfigResource configResource = new ConfigResource(ConfigResource.Type.GROUP, groupId);
			ConfigEntry configEntry = new ConfigEntry("share.auto.offset.reset", "earliest");
			Map<ConfigResource, Collection<AlterConfigOp>> configs = Map.of(configResource,
					List.of(new AlterConfigOp(configEntry, AlterConfigOp.OpType.SET)));
			admin.incrementalAlterConfigs(configs).all().get();
		}
	}

	/**
	 * Helper method to access internal acknowledgment state for testing.
	 */
	private boolean isAcknowledgedInternal(ShareAcknowledgment ack) {
		try {
			java.lang.reflect.Method method = ack.getClass().getDeclaredMethod("isAcknowledged");
			method.setAccessible(true);
			return (Boolean) method.invoke(ack);
		}
		catch (Exception e) {
			throw new RuntimeException("Failed to access internal acknowledgment state", e);
		}
	}

	@Configuration
	@EnableKafka
	static class TestConfig {

		@Bean
		public ShareConsumerFactory<String, String> shareConsumerFactory(EmbeddedKafkaBroker broker) {
			Map<String, Object> configs = new HashMap<>();
			configs.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, broker.getBrokersAsString());
			configs.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
			configs.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
			return new DefaultShareConsumerFactory<>(configs);
		}

		@Bean
		public ShareConsumerFactory<String, String> explicitShareConsumerFactory(EmbeddedKafkaBroker broker) {
			Map<String, Object> configs = new HashMap<>();
			configs.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, broker.getBrokersAsString());
			configs.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
			configs.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
			configs.put("share.acknowledgement.mode", "explicit");
			return new DefaultShareConsumerFactory<>(configs);
		}

		@Bean
		public ShareKafkaListenerContainerFactory<String, String> shareKafkaListenerContainerFactory(
				ShareConsumerFactory<String, String> shareConsumerFactory) {
			return new ShareKafkaListenerContainerFactory<>(shareConsumerFactory);
		}

		@Bean
		public ShareKafkaListenerContainerFactory<String, String> explicitShareKafkaListenerContainerFactory(
				ShareConsumerFactory<String, String> explicitShareConsumerFactory) {
			return new ShareKafkaListenerContainerFactory<>(explicitShareConsumerFactory);
		}

		@Bean
		public ShareKafkaListenerContainerFactory<String, String> factoryPropsShareKafkaListenerContainerFactory(
				ShareConsumerFactory<String, String> shareConsumerFactory) {
			ShareKafkaListenerContainerFactory<String, String> factory =
					new ShareKafkaListenerContainerFactory<>(shareConsumerFactory);
			// Configure explicit acknowledgment via factory's container properties
			factory.getContainerProperties().setExplicitShareAcknowledgment(true);
			return factory;
		}

		@Bean
		public ProducerFactory<String, String> producerFactory(EmbeddedKafkaBroker broker) {
			Map<String, Object> props = new HashMap<>();
			props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, broker.getBrokersAsString());
			props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
			props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
			return new DefaultKafkaProducerFactory<>(props);
		}

		@Bean
		public KafkaTemplate<String, String> kafkaTemplate(ProducerFactory<String, String> producerFactory) {
			return new KafkaTemplate<>(producerFactory);
		}

		// Test listeners as beans
		@Bean
		public BasicTestListener basicTestListener() {
			return new BasicTestListener();
		}

		@Bean
		public ExplicitAckTestListener explicitAckTestListener() {
			return new ExplicitAckTestListener();
		}

		@Bean
		public ShareConsumerAwareTestListener shareConsumerAwareTestListener() {
			return new ShareConsumerAwareTestListener();
		}

		@Bean
		public AckShareConsumerAwareTestListener ackShareConsumerAwareTestListener() {
			return new AckShareConsumerAwareTestListener();
		}

		@Bean
		public MixedAckTestListener mixedAckTestListener() {
			return new MixedAckTestListener();
		}

		@Bean
		public ErrorHandlingTestListener errorHandlingTestListener() {
			return new ErrorHandlingTestListener();
		}

		@Bean
		public FactoryPropsTestListener factoryPropsTestListener() {
			return new FactoryPropsTestListener();
		}
	}

	// Test listener classes
	static class BasicTestListener {

		static final CountDownLatch latch = new CountDownLatch(1);

		static final AtomicReference<String> received = new AtomicReference<>();

		@KafkaListener(topics = "share-listener-basic-test",
				groupId = "share-listener-basic-group",
				containerFactory = "shareKafkaListenerContainerFactory")
		public void listen(ConsumerRecord<String, String> record) {
			received.set(record.value());
			latch.countDown();
		}

	}

	static class ExplicitAckTestListener {

		static final CountDownLatch latch = new CountDownLatch(3);

		static final CountDownLatch redeliveryLatch = new CountDownLatch(1);

		static final Map<String, AcknowledgeType> acknowledgmentTypes = new HashMap<>();

		static final AtomicReference<Boolean> redeliveredAndAccepted = new AtomicReference<>(false);

		@KafkaListener(topics = "share-listener-explicit-ack-test",
				groupId = "share-explicit-ack-group",
				containerFactory = "explicitShareKafkaListenerContainerFactory")
		public void listen(ConsumerRecord<String, String> record, ShareAcknowledgment acknowledgment,
				ShareConsumer<?, ?> consumer) {
			String key = record.key();

			if ("accept".equals(key)) {
				acknowledgment.acknowledge();
				acknowledgmentTypes.put(key, AcknowledgeType.ACCEPT);
				latch.countDown();
			}
			else if ("release".equals(key)) {
				if (!acknowledgmentTypes.containsKey("release-attempted")) {
					// First attempt - release it
					acknowledgment.release();
					acknowledgmentTypes.put("release-attempted", AcknowledgeType.RELEASE);
					latch.countDown();
				}
				else {
					// Redelivered - accept it
					acknowledgment.acknowledge();
					redeliveredAndAccepted.set(true);
					redeliveryLatch.countDown();
				}
			}
			else if ("reject".equals(key)) {
				acknowledgment.reject();
				acknowledgmentTypes.put(key, AcknowledgeType.REJECT);
				latch.countDown();
			}
		}
	}

	static class ShareConsumerAwareTestListener {

		static final CountDownLatch latch = new CountDownLatch(1);

		static final AtomicReference<String> received = new AtomicReference<>();

		static final AtomicReference<ShareConsumer<?, ?>> consumerReceived = new AtomicReference<>();

		@KafkaListener(topics = "share-listener-consumer-aware-test",
				groupId = "share-consumer-aware-group",
				containerFactory = "shareKafkaListenerContainerFactory")
		public void listen(ConsumerRecord<String, String> record, ShareConsumer<?, ?> consumer) {
			received.set(record.value());
			consumerReceived.set(consumer);
			latch.countDown();
		}
	}

	static class AckShareConsumerAwareTestListener {

		static final CountDownLatch latch = new CountDownLatch(1);

		static final AtomicReference<String> received = new AtomicReference<>();

		static final AtomicReference<ShareConsumer<?, ?>> consumerReceived = new AtomicReference<>();

		static final AtomicReference<ShareAcknowledgment> acknowledgmentReceived = new AtomicReference<>();

		@KafkaListener(topics = "share-listener-ack-consumer-aware-test",
				groupId = "share-ack-consumer-aware-group",
				containerFactory = "explicitShareKafkaListenerContainerFactory")
		public void listen(ConsumerRecord<String, String> record,
				@Nullable ShareAcknowledgment acknowledgment, ShareConsumer<?, ?> consumer) {
			received.set(record.value());
			consumerReceived.set(consumer);
			acknowledgmentReceived.set(acknowledgment);
			if (acknowledgment != null) {
				acknowledgment.acknowledge(); // ACCEPT
			}
			latch.countDown();
		}
	}

	static class MixedAckTestListener {

		static final CountDownLatch processedLatch = new CountDownLatch(3);

		static final CountDownLatch retryLatch = new CountDownLatch(1);

		static final AtomicInteger processedCount = new AtomicInteger();

		static final AtomicInteger successCount = new AtomicInteger();

		static final AtomicInteger retryCount = new AtomicInteger();

		@KafkaListener(topics = "share-listener-mixed-ack-test",
				groupId = "share-mixed-ack-group",
				containerFactory = "explicitShareKafkaListenerContainerFactory")
		public void listen(ConsumerRecord<String, String> record, ShareAcknowledgment acknowledgment) {
			String key = record.key();
			int count = processedCount.incrementAndGet();

			if ("retry".equals(key)) {
				if (retryCount.get() == 0) {
					// First attempt - release for retry
					retryCount.incrementAndGet();
					acknowledgment.release();
					processedLatch.countDown();
				}
				else {
					// Retry attempt - accept
					successCount.incrementAndGet();
					acknowledgment.acknowledge();
					retryLatch.countDown();
				}
			}
			else {
				// Success messages
				successCount.incrementAndGet();
				acknowledgment.acknowledge();
				processedLatch.countDown();
			}
		}
	}

	static class ErrorHandlingTestListener {

		static final CountDownLatch latch = new CountDownLatch(3);

		static final AtomicInteger successCount = new AtomicInteger();

		static final AtomicInteger errorCount = new AtomicInteger();

		@KafkaListener(topics = "share-listener-error-handling-test",
				groupId = "share-error-handling-group",
				containerFactory = "explicitShareKafkaListenerContainerFactory")
		public void listen(ConsumerRecord<String, String> record, ShareAcknowledgment acknowledgment) {
			String key = record.key();
			if ("error".equals(key)) {
				errorCount.incrementAndGet();
				latch.countDown();
				// Let the error propagate - container should auto-reject
				throw new RuntimeException("Simulated processing error");
			}
			else {
				successCount.incrementAndGet();
				acknowledgment.acknowledge();
				latch.countDown();
			}
		}
	}

	static class FactoryPropsTestListener {

		static final CountDownLatch latch = new CountDownLatch(1);

		static final AtomicReference<String> received = new AtomicReference<>();

		static final AtomicReference<ShareAcknowledgment> acknowledgmentReceived = new AtomicReference<>();

		@KafkaListener(topics = "share-listener-factory-props-test",
				groupId = "share-factory-props-group",
				containerFactory = "factoryPropsShareKafkaListenerContainerFactory")
		public void listen(ConsumerRecord<String, String> record, @Nullable ShareAcknowledgment acknowledgment) {
			received.set(record.value());
			acknowledgmentReceived.set(acknowledgment);
			if (acknowledgment != null) {
				acknowledgment.acknowledge(); // ACCEPT
			}
			latch.countDown();
		}
	}

}
