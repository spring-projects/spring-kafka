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

package org.springframework.kafka.retrytopic;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.BackOff;
import org.springframework.kafka.annotation.DltHandler;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.annotation.RetryableTopic;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaAdmin;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.kafka.test.utils.KafkaTestUtils;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.scheduling.TaskScheduler;
import org.springframework.scheduling.concurrent.ThreadPoolTaskScheduler;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.junit.jupiter.SpringJUnitConfig;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;

/**
 * @author Hyunggeol Lee
 * @since 4.0
 */
@SpringJUnitConfig
@DirtiesContext
@EmbeddedKafka
public class ReusableRetryTopicMultipleDltIntegrationTests {

	private static final Logger logger = LoggerFactory.getLogger(ReusableRetryTopicMultipleDltIntegrationTests.class);

	public static final String SINGLE_DLT_TOPIC = "reusableRetryWithSingleDlt";

	public static final String MULTI_DLT_TOPIC = "reusableRetryWithMultiDlt";

	public static final String NO_DLT_TOPIC = "reusableRetryWithNoDlt";

	@Autowired
	private KafkaTemplate<String, String> kafkaTemplate;

	@Autowired
	private KafkaListenerEndpointRegistry registry;

	@Autowired
	private CountDownLatchContainer latchContainer;

	@Test
	void shouldStartContextWithReusableRetryTopicAndMultipleDlts() {
		assertThat(registry.getListenerContainer("multiDltListenerId")).isNotNull();
		assertThat(registry.getListenerContainer("multiDltListenerId").isRunning()).isTrue();

		kafkaTemplate.send(MULTI_DLT_TOPIC, "Testing multiple DLTs with custom exception");
		assertThat(awaitLatch(latchContainer.multiDltLatch)).isTrue();
		assertThat(latchContainer.multiDltInvocations.get()).isEqualTo(3);
	}

	@Test
	void shouldMaintainBackwardCompatibilityWithSingleDlt() {
		assertThat(registry.getListenerContainer("singleDltListenerId")).isNotNull();
		assertThat(registry.getListenerContainer("singleDltListenerId").isRunning()).isTrue();

		kafkaTemplate.send(SINGLE_DLT_TOPIC, "Testing single DLT");
		assertThat(awaitLatch(latchContainer.singleDltLatch)).isTrue();
		assertThat(latchContainer.singleDltInvocations.get()).isEqualTo(3);
	}

	@Test
	void shouldWorkWithReusableRetryTopicAndNoDlt() {
		assertThat(registry.getListenerContainer("noDltListenerId")).isNotNull();
		assertThat(registry.getListenerContainer("noDltListenerId").isRunning()).isTrue();

		kafkaTemplate.send(NO_DLT_TOPIC, "Testing reusable retry with no DLT");
		assertThat(awaitLatch(latchContainer.noDltProcessed)).isTrue();
		assertThat(latchContainer.noDltInvocations.get()).isEqualTo(3);
	}

	private boolean awaitLatch(CountDownLatch latch) {
		try {
			return latch.await(30, TimeUnit.SECONDS);
		}
		catch (InterruptedException e) {
			fail(e.getMessage());
			Thread.currentThread().interrupt();
			throw new RuntimeException(e);
		}
	}

	static class SingleDltListener {

		@Autowired
		CountDownLatchContainer container;

		@RetryableTopic(
				attempts = "3",
				backOff = @BackOff(50),
				sameIntervalTopicReuseStrategy = SameIntervalTopicReuseStrategy.SINGLE_TOPIC
		)
		@KafkaListener(id = "singleDltListenerId", topics = SINGLE_DLT_TOPIC)
		public void listen(String message, @Header(KafkaHeaders.RECEIVED_TOPIC) String receivedTopic) {
			logger.debug("Single DLT listener: message {} received in topic {}", message, receivedTopic);
			container.singleDltInvocations.incrementAndGet();
			throw new RuntimeException("Test exception for single DLT");
		}

		@DltHandler
		public void handleDlt(Object message) {
			logger.debug("Single DLT handler received message");
			container.singleDltLatch.countDown();
		}
	}

	static class MultiDltListener {

		@Autowired
		CountDownLatchContainer container;

		@RetryableTopic(
				attempts = "3",
				backOff = @BackOff(50),
				sameIntervalTopicReuseStrategy = SameIntervalTopicReuseStrategy.SINGLE_TOPIC,
				exceptionBasedDltRouting = {
						@ExceptionBasedDltDestination(suffix = "-custom", exceptions = { CustomRetryException.class }),
						@ExceptionBasedDltDestination(suffix = "-validation", exceptions = { ValidationRetryException.class })
				}
		)
		@KafkaListener(id = "multiDltListenerId", topics = MULTI_DLT_TOPIC)
		public void listen(String message, @Header(KafkaHeaders.RECEIVED_TOPIC) String receivedTopic) {
			logger.debug("Multi DLT listener: message {} received in topic {}", message, receivedTopic);
			container.multiDltInvocations.incrementAndGet();

			if (message.contains("custom exception")) {
				throw new CustomRetryException("Testing custom DLT routing");
			}
			else if (message.contains("validation exception")) {
				throw new ValidationRetryException("Testing validation DLT routing");
			}
			else {
				throw new RuntimeException("Testing default DLT routing");
			}
		}

		@DltHandler
		public void handleDlt(Object message) {
			logger.debug("Multi DLT handler received message");
			container.multiDltLatch.countDown();
		}
	}

	static class NoDltListener {

		@Autowired
		CountDownLatchContainer container;

		@RetryableTopic(
				attempts = "3",
				backOff = @BackOff(50),
				dltStrategy = DltStrategy.NO_DLT,
				sameIntervalTopicReuseStrategy = SameIntervalTopicReuseStrategy.SINGLE_TOPIC
		)
		@KafkaListener(id = "noDltListenerId", topics = NO_DLT_TOPIC)
		public void listen(String message, @Header(KafkaHeaders.RECEIVED_TOPIC) String receivedTopic) {
			logger.debug("No DLT listener: message {} received in topic {}", message, receivedTopic);
			container.noDltInvocations.incrementAndGet();
			if (container.noDltInvocations.get() == 3) {
				container.noDltProcessed.countDown();
			}
			throw new RuntimeException("Test exception for no DLT");
		}

		@DltHandler
		public void shouldNotBeInvoked() {
			fail("DLT handler should not be invoked when dltStrategy = NO_DLT");
		}
	}

	static class CountDownLatchContainer {

		final CountDownLatch singleDltLatch = new CountDownLatch(1);

		final CountDownLatch multiDltLatch = new CountDownLatch(1);

		final CountDownLatch noDltProcessed = new CountDownLatch(1);

		final AtomicInteger singleDltInvocations = new AtomicInteger(0);

		final AtomicInteger multiDltInvocations = new AtomicInteger(0);

		final AtomicInteger noDltInvocations = new AtomicInteger(0);
	}

	@SuppressWarnings("serial")
	public static class CustomRetryException extends RuntimeException {
		public CustomRetryException(String msg) {
			super(msg);
		}
	}

	@SuppressWarnings("serial")
	public static class ValidationRetryException extends RuntimeException {
		public ValidationRetryException(String msg) {
			super(msg);
		}
	}

	@Configuration
	static class RetryTopicConfigurations extends RetryTopicConfigurationSupport {

		@Bean
		public SingleDltListener singleDltListener() {
			return new SingleDltListener();
		}

		@Bean
		public MultiDltListener multiDltListener() {
			return new MultiDltListener();
		}

		@Bean
		public NoDltListener noDltListener() {
			return new NoDltListener();
		}

		@Bean
		CountDownLatchContainer latchContainer() {
			return new CountDownLatchContainer();
		}

		@Bean
		TaskScheduler taskScheduler() {
			return new ThreadPoolTaskScheduler();
		}
	}

	@Configuration
	public static class KafkaProducerConfig {

		@Autowired
		EmbeddedKafkaBroker broker;

		@Bean
		public ProducerFactory<String, String> producerFactory() {
			Map<String, Object> configProps = KafkaTestUtils.producerProps(broker);
			configProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
			configProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
			return new DefaultKafkaProducerFactory<>(configProps);
		}

		@Bean
		public KafkaTemplate<String, String> kafkaTemplate() {
			return new KafkaTemplate<>(producerFactory());
		}
	}

	@EnableKafka
	@Configuration
	public static class KafkaConsumerConfig {

		@Autowired
		EmbeddedKafkaBroker broker;

		@Bean
		public KafkaAdmin kafkaAdmin() {
			Map<String, Object> configs = new HashMap<>();
			configs.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, broker.getBrokersAsString());
			return new KafkaAdmin(configs);
		}

		@Bean
		public ConsumerFactory<String, String> consumerFactory() {
			Map<String, Object> props = KafkaTestUtils.consumerProps(broker.getBrokersAsString(), "test-group");
			props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
			props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
			props.put(ConsumerConfig.ALLOW_AUTO_CREATE_TOPICS_CONFIG, false);
			props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
			return new DefaultKafkaConsumerFactory<>(props);
		}

		@Bean
		public ConcurrentKafkaListenerContainerFactory<String, String> kafkaListenerContainerFactory(
				ConsumerFactory<String, String> consumerFactory) {

			ConcurrentKafkaListenerContainerFactory<String, String> factory =
					new ConcurrentKafkaListenerContainerFactory<>();
			factory.setConsumerFactory(consumerFactory);
			factory.setConcurrency(1);
			return factory;
		}
	}
}
