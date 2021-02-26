/*
 * Copyright 2018-2021 the original author or authors.
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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;

import java.time.Clock;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

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
import org.springframework.kafka.annotation.DltHandler;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.annotation.RetryableTopic;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaAdmin;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.kafka.listener.KafkaConsumerBackoffManager;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.retry.annotation.Backoff;
import org.springframework.stereotype.Component;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.context.junit.jupiter.SpringJUnitConfig;


/**
 * @author Tomaz Fernandes
 * @author Gary Russell
 * @since 2.7
 */
@SpringJUnitConfig
@DirtiesContext
@EmbeddedKafka(topics = { RetryableTopicIntegrationTests.FIRST_TOPIC,
		RetryableTopicIntegrationTests.SECOND_TOPIC,
		RetryableTopicIntegrationTests.THIRD_TOPIC,
		RetryableTopicIntegrationTests.FOURTH_TOPIC }, partitions = 1)
@TestPropertySource(properties = "five.attempts=5")
public class RetryableTopicIntegrationTests {

	private static final Logger logger = LoggerFactory.getLogger(RetryableTopicIntegrationTests.class);

	public final static String FIRST_TOPIC = "myRetryTopic1";

	public final static String SECOND_TOPIC = "myRetryTopic2";

	public final static String THIRD_TOPIC = "myRetryTopic3";

	public final static String FOURTH_TOPIC = "myRetryTopic4";

	public final static String NOT_RETRYABLE_EXCEPTION_TOPIC = "noRetryTopic";

	private final static String MAIN_TOPIC_CONTAINER_FACTORY = "kafkaListenerContainerFactory";

	@Autowired
	private KafkaTemplate<String, String> kafkaTemplate;

	@Autowired
	private CountDownLatchContainer latchContainer;

	@Test
	void shouldRetryFirstTopic() {
		logger.debug("Sending message to topic " + FIRST_TOPIC);
		kafkaTemplate.send(FIRST_TOPIC, "Testing topic 1");
		assertThat(awaitLatch(latchContainer.countDownLatch1)).isTrue();
		assertThat(awaitLatch(latchContainer.customDltCountdownLatch)).isTrue();
	}

	@Test
	void shouldRetrySecondTopic() {
		logger.debug("Sending message to topic " + SECOND_TOPIC);
		kafkaTemplate.send(SECOND_TOPIC, "Testing topic 2");
		assertThat(awaitLatch(latchContainer.countDownLatch2)).isTrue();
		assertThat(awaitLatch(latchContainer.customDltCountdownLatch)).isTrue();
	}

	@Test
	void shouldRetryThirdTopicWithTimeout() {
		logger.debug("Sending message to topic " + THIRD_TOPIC);
		kafkaTemplate.send(THIRD_TOPIC, "Testing topic 3");
		assertThat(awaitLatch(latchContainer.countDownLatch3)).isTrue();
		assertThat(awaitLatch(latchContainer.countDownLatchDltOne)).isTrue();
	}

	@Test
	void shouldRetryFourthTopicWithNoDlt() {
		logger.debug("Sending message to topic " + FOURTH_TOPIC);
		kafkaTemplate.send(FOURTH_TOPIC, "Testing topic 4");
		assertThat(awaitLatch(latchContainer.countDownLatch4)).isTrue();
	}

	@Test
	public void shouldGoStraightToDlt() {
		logger.debug("Sending message to topic " + NOT_RETRYABLE_EXCEPTION_TOPIC);
		kafkaTemplate.send(NOT_RETRYABLE_EXCEPTION_TOPIC, "Testing topic with annotation 1");
		assertThat(awaitLatch(latchContainer.countDownLatchNoRetry)).isTrue();
		assertThat(awaitLatch(latchContainer.countDownLatchDltTwo)).isTrue();
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

	@Component
	static class FirstTopicListener {

		@Autowired
		CountDownLatchContainer container;

		@KafkaListener(id = "firstTopicId", topics = FIRST_TOPIC, containerFactory = MAIN_TOPIC_CONTAINER_FACTORY)
		public void listen(String message) {
			logger.debug("Message {} received in topic {}", message, FIRST_TOPIC);
			container.countDownLatch1.countDown();
			throw new RuntimeException("Woooops... in topic " + FIRST_TOPIC);
		}
	}

	@Component
	static class SecondTopicListener {

		@Autowired
		CountDownLatchContainer container;

		@KafkaListener(topics = SECOND_TOPIC, containerFactory = MAIN_TOPIC_CONTAINER_FACTORY)
		public void listenAgain(String message) {
			logger.debug("Message {} received in topic {} ", message, SECOND_TOPIC);
			container.countDownLatch2.countDown();
			throw new IllegalStateException("Another woooops... " + SECOND_TOPIC);
		}
	}

	@Component
	static class ThirdTopicListener {

		@Autowired
		CountDownLatchContainer container;

		@RetryableTopic(attempts = "${five.attempts}",
				backoff = @Backoff(delay = 70, maxDelay = 120, multiplier = 1.5),
				numPartitions = "#{3}",
				timeout = "${missing.property:2000}",
				include = MyRetryException.class, kafkaTemplate = "kafkaTemplate")
		@KafkaListener(id = "thirdTopicId", topics = THIRD_TOPIC, containerFactory = MAIN_TOPIC_CONTAINER_FACTORY)
		public void listenWithAnnotation(String message) {
			container.countDownLatch3.countDown();
			logger.debug("========================== Message {} received in annotated topic {} ", message, THIRD_TOPIC);
			throw new MyRetryException("Annotated woooops... " + THIRD_TOPIC);
		}

		@DltHandler
		public void annotatedDltMethod(Object message) {
			logger.debug("Received message in annotated Dlt method");
			container.countDownLatchDltOne.countDown();
		}
	}

	@Component
	static class FourthTopicListener {

		@Autowired
		CountDownLatchContainer container;

		@RetryableTopic(dltStrategy = DltStrategy.NO_DLT, attempts = "4", backoff = @Backoff(50),
				kafkaTemplate = "kafkaTemplate")
		@KafkaListener(topics = FOURTH_TOPIC, containerFactory = MAIN_TOPIC_CONTAINER_FACTORY)
		public void listenNoDlt(String message) {
			logger.debug("Message {} received in topic {} ", message, FOURTH_TOPIC);
			container.countDownLatch4.countDown();
			throw new IllegalStateException("Another woooops... " + FOURTH_TOPIC);
		}

		@DltHandler
		public void shouldNotGetHere() {
			fail("Dlt should not be processed!");
		}
	}

	@Component
	static class NoRetryTopicListener {

		@Autowired
		CountDownLatchContainer container;

		@RetryableTopic(attempts = "3", numPartitions = "3", exclude = MyDontRetryException.class,
				backoff = @Backoff(delay = 50, maxDelay = 100, multiplier = 3),
				traversingCauses = "true", kafkaTemplate = "kafkaTemplate")
		@KafkaListener(topics = NOT_RETRYABLE_EXCEPTION_TOPIC, containerFactory = MAIN_TOPIC_CONTAINER_FACTORY)
		public void listenWithAnnotation2(String message) {
			container.countDownLatchNoRetry.countDown();
			logger.info("Message {} received in second annotated topic {} ", message, NOT_RETRYABLE_EXCEPTION_TOPIC);
			throw new MyDontRetryException("Annotated second woooops... " + NOT_RETRYABLE_EXCEPTION_TOPIC);
		}

		@DltHandler
		public void annotatedDltMethod(Object message) {
			logger.info("Received message in annotated Dlt method!");
			container.countDownLatchDltTwo.countDown();
		}
	}

	@Component
	static class CountDownLatchContainer {

		CountDownLatch countDownLatch1 = new CountDownLatch(5);
		CountDownLatch countDownLatch2 = new CountDownLatch(3);
		CountDownLatch countDownLatch3 = new CountDownLatch(3);
		CountDownLatch countDownLatch4 = new CountDownLatch(4);
		CountDownLatch countDownLatchNoRetry = new CountDownLatch(1);
		CountDownLatch countDownLatchDltOne = new CountDownLatch(1);
		CountDownLatch countDownLatchDltTwo = new CountDownLatch(1);
		CountDownLatch customDltCountdownLatch = new CountDownLatch(1);

	}

	@Component
	static class MyCustomDltProcessor {

		@Autowired
		KafkaTemplate<String, String> kafkaTemplate;

		@Autowired
		CountDownLatchContainer container;

		public void processDltMessage(Object message) {
			container.customDltCountdownLatch.countDown();
			logger.info("Received message in custom dlt!");
			logger.info("Just showing I have an injected kafkaTemplate! " + kafkaTemplate);
			throw new RuntimeException("Dlt Error!");
		}
	}

	@SuppressWarnings("serial")
	public static class MyRetryException extends RuntimeException {
		public MyRetryException(String msg) {
			super(msg);
		}
	}

	@SuppressWarnings("serial")
	public static class MyDontRetryException extends RuntimeException {
		public MyDontRetryException(String msg) {
			super(msg);
		}
	}

	@Configuration
	static class RetryTopicConfigurations {

		private static final String DLT_METHOD_NAME = "processDltMessage";

		@Bean
		public RetryTopicConfiguration firstRetryTopic(KafkaTemplate<String, String> template) {
			return RetryTopicConfigurationBuilder
					.newInstance()
					.fixedBackOff(50)
					.maxAttempts(5)
					.useSingleTopicForFixedDelays()
					.includeTopic(FIRST_TOPIC)
					.doNotRetryOnDltFailure()
					.dltHandlerMethod(MyCustomDltProcessor.class, DLT_METHOD_NAME)
					.create(template);
		}

		@Bean
		public RetryTopicConfiguration secondRetryTopic(KafkaTemplate<String, String> template) {
			return RetryTopicConfigurationBuilder
					.newInstance()
					.exponentialBackoff(50, 2, 10000)
					.retryOn(Arrays.asList(IllegalStateException.class, IllegalAccessException.class))
					.traversingCauses()
					.includeTopic(SECOND_TOPIC)
					.doNotRetryOnDltFailure()
					.dltHandlerMethod(MyCustomDltProcessor.class, DLT_METHOD_NAME)
					.create(template);
		}

		@Bean
		public FirstTopicListener firstTopicListener() {
			return new FirstTopicListener();
		}

		@Bean
		public SecondTopicListener secondTopicListener() {
			return new SecondTopicListener();
		}

		@Bean
		public ThirdTopicListener thirdTopicListener() {
			return new ThirdTopicListener();
		}


		@Bean
		public FourthTopicListener fourthTopicListener() {
			return new FourthTopicListener();
		}

		@Bean
		public NoRetryTopicListener noRetryTopicListener() {
			return new NoRetryTopicListener();
		}

		@Bean
		CountDownLatchContainer latchContainer() {
			return new CountDownLatchContainer();
		}

		@Bean
		MyCustomDltProcessor myCustomDltProcessor() {
			return new MyCustomDltProcessor();
		}
	}


	@Configuration
	public static class RuntimeConfig {

		@Bean(name = KafkaConsumerBackoffManager.INTERNAL_BACKOFF_CLOCK_BEAN_NAME)
		public Clock clock() {
			return Clock.systemUTC();
		}
	}

	@Configuration
	public static class KafkaProducerConfig {

		@Autowired
		EmbeddedKafkaBroker broker;

		@Bean
		public ProducerFactory<String, String> producerFactory() {
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
			configs.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, this.broker.getBrokersAsString());
			return new KafkaAdmin(configs);
		}

		@Bean
		public ConsumerFactory<String, String> consumerFactory() {
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
		public ConcurrentKafkaListenerContainerFactory<String, String> retryTopicListenerContainerFactory(
				ConsumerFactory<String, String> consumerFactory) {

			ConcurrentKafkaListenerContainerFactory<String, String> factory = new ConcurrentKafkaListenerContainerFactory<>();
			ContainerProperties props = factory.getContainerProperties();
			props.setIdleEventInterval(100L);
			props.setPollTimeout(50L);
			props.setIdlePartitionEventInterval(100L);
			factory.setConsumerFactory(consumerFactory);
			factory.setConcurrency(1);
			factory.setContainerCustomizer(
					container -> container.getContainerProperties().setIdlePartitionEventInterval(100L));
			return factory;
		}

		@Bean
		public ConcurrentKafkaListenerContainerFactory<String, String> kafkaListenerContainerFactory(
				ConsumerFactory<String, String> consumerFactory) {

			ConcurrentKafkaListenerContainerFactory<String, String> factory = new ConcurrentKafkaListenerContainerFactory<>();
			ContainerProperties props = factory.getContainerProperties();
			props.setIdleEventInterval(100L);
			props.setPollTimeout(50L);
			props.setIdlePartitionEventInterval(100L);
			factory.setConsumerFactory(consumerFactory);
			factory.setConcurrency(1);
			return factory;
		}
	}

}
