/*
 * Copyright 2019-2024 the original author or authors.
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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.Test;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaAdmin;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.junit.jupiter.SpringJUnitConfig;
import org.springframework.util.backoff.FixedBackOff;

/**
 * @author Sanghyeok An
 * @since 3.3.0
 */

@SpringJUnitConfig
@DirtiesContext
@EmbeddedKafka
class DeliveryAttemptAwareRetryListenerIntegrationTest {

	static final String MAIN_TOPIC_CONTAINER_FACTORY0 = "kafkaListenerContainerFactory0";

	static final String TEST_TOPIC0 = "myBatchDeliveryAttemptTopic0";

	static final int MAX_ATTEMPT_COUNT0 = 3;

	static final CountDownLatch latch0 = new CountDownLatch(MAX_ATTEMPT_COUNT0 + 1);

	static final String MAIN_TOPIC_CONTAINER_FACTORY1 = "kafkaListenerContainerFactory1";

	static final String TEST_TOPIC1 = "myBatchDeliveryAttemptTopic1";

	static final int MAX_ATTEMPT_COUNT1 = 10;

	static final CountDownLatch latch1 = new CountDownLatch(MAX_ATTEMPT_COUNT1 + 1);

	@Autowired
	private KafkaTemplate<String, String> kafkaTemplate;

	@Test
	void should_have_delivery_attempt_header_in_each_consumer_record(@Autowired TestTopicListener0 listener) {

		// Given
		String msg1 = "1";
		String msg2 = "2";
		String msg3 = "3";

		// When
		kafkaTemplate.send(TEST_TOPIC0, msg1);
		kafkaTemplate.send(TEST_TOPIC0, msg2);
		kafkaTemplate.send(TEST_TOPIC0, msg3);

		// Then
		assertThat(awaitLatch(latch0)).isTrue();

		Map<Integer, Integer> deliveryAttemptCountMap = convertToMap(listener.receivedHeaders);

		for (int attemptCnt = 1; attemptCnt <= MAX_ATTEMPT_COUNT0; attemptCnt++) {
			assertThat(deliveryAttemptCountMap.get(attemptCnt)).isEqualTo(3);
		}
	}

	@Test
	void should_have_delivery_attempt_header_in_each_consumer_record_with_more_bigger_max_attempt(@Autowired TestTopicListener1 listener) {
		// Given
		String msg1 = "1";
		String msg2 = "2";
		String msg3 = "3";

		// When
		kafkaTemplate.send(TEST_TOPIC1, msg1);
		kafkaTemplate.send(TEST_TOPIC1, msg2);
		kafkaTemplate.send(TEST_TOPIC1, msg3);

		// Then
		assertThat(awaitLatch(latch1)).isTrue();

		Map<Integer, Integer> deliveryAttemptCountMap = convertToMap(listener.receivedHeaders);

		for (int attemptCnt = 1; attemptCnt <= MAX_ATTEMPT_COUNT1; attemptCnt++) {
			assertThat(deliveryAttemptCountMap.get(attemptCnt)).isEqualTo(3);
		}
	}

	private Map<Integer, Integer> convertToMap(List<Header> headers) {
		Map<Integer, Integer> map = new HashMap<>();
		for (Header header : headers) {
			int attemptCount = ByteBuffer.wrap(header.value()).getInt();
			Integer cnt = map.getOrDefault(attemptCount, 0);
			map.put(attemptCount, cnt + 1);
		}
		return map;
	}

	private boolean awaitLatch(CountDownLatch latch) {
		try {
			return latch.await(60, TimeUnit.SECONDS);
		}
		catch (Exception e) {
			fail(e.getMessage());
			throw new RuntimeException(e);
		}
	}

	static class TestTopicListener0 {
		final List<Header> receivedHeaders = new ArrayList<>();

		@KafkaListener(
				topics = TEST_TOPIC0,
				containerFactory = MAIN_TOPIC_CONTAINER_FACTORY0,
				batch = "true")
		public void listen(List<ConsumerRecord<?, ?>> records) {
			latch0.countDown();
			for (ConsumerRecord<?, ?> record : records) {
				Iterable<Header> headers = record.headers().headers(KafkaHeaders.DELIVERY_ATTEMPT);
				for (Header header : headers) {
					receivedHeaders.add(header);
				}
			}
			throw new RuntimeException("Failed.");
		}
	}

	static class TestTopicListener1 {
		final List<Header> receivedHeaders = new ArrayList<>();

		@KafkaListener(
				topics = TEST_TOPIC1,
				containerFactory = MAIN_TOPIC_CONTAINER_FACTORY1,
				batch = "true")
		public void listen(List<ConsumerRecord<?, ?>> records) {
			latch1.countDown();
			for (ConsumerRecord<?, ?> record : records) {
				Iterable<Header> headers = record.headers().headers(KafkaHeaders.DELIVERY_ATTEMPT);
				for (Header header : headers) {
					receivedHeaders.add(header);
				}
			}
			throw new RuntimeException("Failed.");
		}
	}

	@Configuration
	static class TestConfiguration {

		@Bean
		TestTopicListener0 testTopicListener0() {
			return new TestTopicListener0();
		}

		@Bean
		TestTopicListener1 testTopicListener1() {
			return new TestTopicListener1();
		}
	}

	@Configuration
	static class KafkaProducerConfig {

		@Autowired
		EmbeddedKafkaBroker broker;

		@Bean
		ProducerFactory<String, String> producerFactory() {
			Map<String, Object> configProps = new HashMap<>();
			configProps.put(
					ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,
					this.broker.getBrokersAsString());
			configProps.put(
					ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
					StringSerializer.class);
			configProps.put(
					ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
					StringSerializer.class);
			return new DefaultKafkaProducerFactory<>(configProps);
		}

		@Bean("customKafkaTemplate")
		KafkaTemplate<String, String> kafkaTemplate() {
			return new KafkaTemplate<>(producerFactory());
		}
	}

	@EnableKafka
	@Configuration
	static class KafkaConsumerConfig {

		@Autowired
		EmbeddedKafkaBroker broker;

		@Bean
		KafkaAdmin kafkaAdmin() {
			Map<String, Object> configs = new HashMap<>();
			configs.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, this.broker.getBrokersAsString());
			return new KafkaAdmin(configs);
		}

		@Bean
		ConsumerFactory<String, String> consumerFactory() {
			Map<String, Object> props = new HashMap<>();
			props.put(
					ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,
					this.broker.getBrokersAsString());
			props.put(
					ConsumerConfig.GROUP_ID_CONFIG,
					"groupId");
			props.put(
					ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
					StringDeserializer.class);
			props.put(
					ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
					StringDeserializer.class);
			props.put(
					ConsumerConfig.ALLOW_AUTO_CREATE_TOPICS_CONFIG, false);
			props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

			return new DefaultKafkaConsumerFactory<>(props);
		}

		@Bean
		ConcurrentKafkaListenerContainerFactory<String, String>
		kafkaListenerContainerFactory0(ConsumerFactory<String, String> consumerFactory) {

			final FixedBackOff fixedBackOff = new FixedBackOff(1000L, MAX_ATTEMPT_COUNT0);
			DefaultErrorHandler errorHandler = new DefaultErrorHandler(fixedBackOff);
			errorHandler.setRetryListeners(new DeliveryAttemptAwareRetryListener());

			ConcurrentKafkaListenerContainerFactory<String, String> factory = new ConcurrentKafkaListenerContainerFactory<>();
			factory.setConsumerFactory(consumerFactory);
			factory.setCommonErrorHandler(errorHandler);


			final ContainerProperties containerProperties = factory.getContainerProperties();
			containerProperties.setDeliveryAttemptHeader(true);

			return factory;
		}

		@Bean
		ConcurrentKafkaListenerContainerFactory<String, String>
		kafkaListenerContainerFactory1(ConsumerFactory<String, String> consumerFactory) {

			final FixedBackOff fixedBackOff = new FixedBackOff(1000L, MAX_ATTEMPT_COUNT1);
			DefaultErrorHandler errorHandler = new DefaultErrorHandler(fixedBackOff);
			errorHandler.setRetryListeners(new DeliveryAttemptAwareRetryListener());

			ConcurrentKafkaListenerContainerFactory<String, String> factory = new ConcurrentKafkaListenerContainerFactory<>();
			factory.setConsumerFactory(consumerFactory);
			factory.setCommonErrorHandler(errorHandler);


			final ContainerProperties containerProperties = factory.getContainerProperties();
			containerProperties.setDeliveryAttemptHeader(true);

			return factory;

		}

	}

}
