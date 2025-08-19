/*
 * Copyright 2021-present the original author or authors.
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

import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.Test;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;
import org.springframework.kafka.annotation.BackOff;
import org.springframework.kafka.annotation.DltHandler;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.annotation.KafkaHandler;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.annotation.RetryableTopic;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.kafka.test.utils.KafkaTestUtils;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.stereotype.Component;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.junit.jupiter.SpringJUnitConfig;
import org.springframework.util.backoff.FixedBackOff;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;

/**
 * @author Tomaz Fernandes
 * @author Cenk Akin
 * @author Wang Zhiyang
 * @author Sanghyeok An
 *
 * @since 2.8.3
 */
@SpringJUnitConfig
@DirtiesContext
@EmbeddedKafka(topics = { RetryTopicSameContainerFactoryIntegrationTests.FIRST_TOPIC,
		RetryTopicSameContainerFactoryIntegrationTests.SECOND_TOPIC,
		RetryTopicSameContainerFactoryIntegrationTests.THIRD_TOPIC,
		RetryTopicSameContainerFactoryIntegrationTests.CLASS_LEVEL_FIRST_TOPIC,
		RetryTopicSameContainerFactoryIntegrationTests.CLASS_LEVEL_SECOND_TOPIC,
		RetryTopicSameContainerFactoryIntegrationTests.CLASS_LEVEL_THIRD_TOPIC}, partitions = 1)
public class RetryTopicSameContainerFactoryIntegrationTests {

	public final static String FIRST_TOPIC = "myRetryTopic1";

	public final static String SECOND_TOPIC = "myRetryTopic2";

	public final static String THIRD_TOPIC = "myRetryTopic3";

	public final static String CLASS_LEVEL_FIRST_TOPIC = "classLevelRetryTopic1";

	public final static String CLASS_LEVEL_SECOND_TOPIC = "classLevelRetryTopic2";

	public final static String CLASS_LEVEL_THIRD_TOPIC = "classLevelRetryTopic3";

	@Autowired
	private KafkaTemplate<String, String> sendKafkaTemplate;

	@Autowired
	private CountDownLatchContainer latchContainer;

	@Test
	void shouldRetryFirstAndSecondTopics(@Autowired RetryTopicComponentFactory componentFactory) {
		sendKafkaTemplate.send(FIRST_TOPIC, "Testing topic 1");
		sendKafkaTemplate.send(SECOND_TOPIC, "Testing topic 2");
		sendKafkaTemplate.send(THIRD_TOPIC, "Testing topic 3");
		assertThat(awaitLatch(latchContainer.countDownLatchFirstRetryable)).isTrue();
		assertThat(awaitLatch(latchContainer.countDownLatchDltOne)).isTrue();
		assertThat(awaitLatch(latchContainer.countDownLatchSecondRetryable)).isTrue();
		assertThat(awaitLatch(latchContainer.countDownLatchDltSecond)).isTrue();
		assertThat(awaitLatch(latchContainer.countDownLatchBasic)).isTrue();
		assertThat(awaitLatch(latchContainer.customizerLatch)).isTrue();
		verify(componentFactory).destinationTopicResolver();
	}

	@Test
	void shouldRetryClassLevelFirstAndSecondTopics(@Autowired RetryTopicComponentFactory componentFactory) {
		sendKafkaTemplate.send(CLASS_LEVEL_FIRST_TOPIC, "Testing topic 1");
		sendKafkaTemplate.send(CLASS_LEVEL_SECOND_TOPIC, "Testing topic 2");
		sendKafkaTemplate.send(CLASS_LEVEL_THIRD_TOPIC, "Testing topic 3");
		assertThat(awaitLatch(latchContainer.countDownLatchClassLevelFirstRetryable)).isTrue();
		assertThat(awaitLatch(latchContainer.countDownLatchClassLevelDltOne)).isTrue();
		assertThat(awaitLatch(latchContainer.countDownLatchClassLevelSecondRetryable)).isTrue();
		assertThat(awaitLatch(latchContainer.countDownLatchClassLevelDltSecond)).isTrue();
		assertThat(awaitLatch(latchContainer.countDownLatchClassLevelBasic)).isTrue();
		assertThat(awaitLatch(latchContainer.customizerClassLevelLatch)).isTrue();
		verify(componentFactory).destinationTopicResolver();
	}

	private boolean awaitLatch(CountDownLatch latch) {
		try {
			return latch.await(150, TimeUnit.SECONDS);
		}
		catch (Exception e) {
			fail(e.getMessage());
			throw new RuntimeException(e);
		}
	}

	@Component
	static class FirstRetryableKafkaListener {

		@Autowired
		CountDownLatchContainer countDownLatchContainer;

		@RetryableTopic(
				attempts = "4",
				backOff = @BackOff(delay = 1000, multiplier = 2.0),
				autoCreateTopics = "false",
				topicSuffixingStrategy = TopicSuffixingStrategy.SUFFIX_WITH_INDEX_VALUE,
				sameIntervalTopicReuseStrategy = SameIntervalTopicReuseStrategy.MULTIPLE_TOPICS)
		@KafkaListener(topics = RetryTopicSameContainerFactoryIntegrationTests.FIRST_TOPIC)
		public void listen(String in, @Header(KafkaHeaders.RECEIVED_TOPIC) String topic) {
			countDownLatchContainer.countDownLatchFirstRetryable.countDown();
			throw new RuntimeException("from FirstRetryableKafkaListener");
		}

		@DltHandler
		public void dlt(String in, @Header(KafkaHeaders.RECEIVED_TOPIC) String topic) {
			countDownLatchContainer.countDownLatchDltOne.countDown();
		}
	}

	@Component
	@RetryableTopic(
			attempts = "4",
			backOff = @BackOff(delay = 1000, multiplier = 2.0),
			autoCreateTopics = "false",
			topicSuffixingStrategy = TopicSuffixingStrategy.SUFFIX_WITH_INDEX_VALUE,
			sameIntervalTopicReuseStrategy = SameIntervalTopicReuseStrategy.MULTIPLE_TOPICS)
	@KafkaListener(topics = RetryTopicSameContainerFactoryIntegrationTests.CLASS_LEVEL_FIRST_TOPIC,
			containerFactory = "classLevelKafkaListenerContainerFactory")
	static class FirstClassLevelRetryableKafkaListener {

		@Autowired
		CountDownLatchContainer countDownLatchContainer;

		@KafkaHandler
		public void listen(String in, @Header(KafkaHeaders.RECEIVED_TOPIC) String topic) {
			countDownLatchContainer.countDownLatchClassLevelFirstRetryable.countDown();
			throw new RuntimeException("from FirstClassLevelRetryableKafkaListener");
		}

		@DltHandler
		public void dlt(String in, @Header(KafkaHeaders.RECEIVED_TOPIC) String topic) {
			countDownLatchContainer.countDownLatchClassLevelDltOne.countDown();
		}
	}

	@Component
	static class SecondRetryableKafkaListener {

		@Autowired
		CountDownLatchContainer countDownLatchContainer;

		@RetryableTopic(sameIntervalTopicReuseStrategy = SameIntervalTopicReuseStrategy.MULTIPLE_TOPICS)
		@KafkaListener(topics = RetryTopicSameContainerFactoryIntegrationTests.SECOND_TOPIC)
		public void listen(String in, @Header(KafkaHeaders.RECEIVED_TOPIC) String topic) {
			countDownLatchContainer.countDownLatchSecondRetryable.countDown();
			throw new RuntimeException("from SecondRetryableKafkaListener");
		}

		@DltHandler
		public void dlt(String in, @Header(KafkaHeaders.RECEIVED_TOPIC) String topic) {
			countDownLatchContainer.countDownLatchDltSecond.countDown();
		}
	}

	@Component
	@RetryableTopic(sameIntervalTopicReuseStrategy = SameIntervalTopicReuseStrategy.MULTIPLE_TOPICS)
	@KafkaListener(topics = RetryTopicSameContainerFactoryIntegrationTests.CLASS_LEVEL_SECOND_TOPIC,
			containerFactory = "classLevelKafkaListenerContainerFactory")
	static class SecondClassLevelRetryableKafkaListener {

		@Autowired
		CountDownLatchContainer countDownLatchContainer;

		@KafkaHandler
		public void listen(String in, @Header(KafkaHeaders.RECEIVED_TOPIC) String topic) {
			countDownLatchContainer.countDownLatchClassLevelSecondRetryable.countDown();
			throw new RuntimeException("from ClassLevelSecondRetryableKafkaListener");
		}

		@DltHandler
		public void dlt(String in, @Header(KafkaHeaders.RECEIVED_TOPIC) String topic) {
			countDownLatchContainer.countDownLatchClassLevelDltSecond.countDown();
		}
	}

	@Component
	static class BasicKafkaListener {

		@KafkaListener(topics = RetryTopicSameContainerFactoryIntegrationTests.THIRD_TOPIC)
		public void listen(String in, @Header(KafkaHeaders.RECEIVED_TOPIC) String topic) {
			throw new RuntimeException("from BasicKafkaListener");
		}
	}

	@Component
	@KafkaListener(topics = RetryTopicSameContainerFactoryIntegrationTests.CLASS_LEVEL_THIRD_TOPIC,
			containerFactory = "classLevelKafkaListenerContainerFactory")
	static class BasicClassLevelKafkaListener {

		@KafkaHandler
		public void listen(String in, @Header(KafkaHeaders.RECEIVED_TOPIC) String topic) {
			throw new RuntimeException("from BasicClassLevelKafkaListener");
		}
	}

	@Component
	static class CountDownLatchContainer {

		CountDownLatch countDownLatchFirstRetryable = new CountDownLatch(4);

		CountDownLatch countDownLatchSecondRetryable = new CountDownLatch(3);

		CountDownLatch countDownLatchDltOne = new CountDownLatch(1);

		CountDownLatch countDownLatchDltSecond = new CountDownLatch(1);

		CountDownLatch countDownLatchBasic = new CountDownLatch(1);

		CountDownLatch customizerLatch = new CountDownLatch(10);

		CountDownLatch countDownLatchClassLevelFirstRetryable = new CountDownLatch(4);

		CountDownLatch countDownLatchClassLevelSecondRetryable = new CountDownLatch(3);

		CountDownLatch countDownLatchClassLevelDltOne = new CountDownLatch(1);

		CountDownLatch countDownLatchClassLevelDltSecond = new CountDownLatch(1);

		CountDownLatch countDownLatchClassLevelBasic = new CountDownLatch(1);

		CountDownLatch customizerClassLevelLatch = new CountDownLatch(10);

	}

	@EnableKafka
	@Configuration
	static class Config {

		@Autowired
		EmbeddedKafkaBroker broker;

		@Bean
		CountDownLatchContainer latchContainer() {
			return new CountDownLatchContainer();
		}

		@Bean
		FirstRetryableKafkaListener firstRetryableKafkaListener() {
			return new FirstRetryableKafkaListener();
		}

		@Bean
		SecondRetryableKafkaListener secondRetryableKafkaListener() {
			return new SecondRetryableKafkaListener();
		}

		@Bean
		BasicKafkaListener basicKafkaListener() {
			return new BasicKafkaListener();
		}

		@Bean
		FirstClassLevelRetryableKafkaListener firstClassLevelRetryableKafkaListener() {
			return new FirstClassLevelRetryableKafkaListener();
		}

		@Bean
		SecondClassLevelRetryableKafkaListener secondClassLevelRetryableKafkaListener() {
			return new SecondClassLevelRetryableKafkaListener();
		}

		@Bean
		BasicClassLevelKafkaListener basicClassLevelKafkaListener() {
			return new BasicClassLevelKafkaListener();
		}

		@Bean
		@Primary
		public ConcurrentKafkaListenerContainerFactory<String, String> kafkaListenerContainerFactory(
				ConsumerFactory<String, String> consumerFactory, CountDownLatchContainer latchContainer) {

			return createKafkaListenerContainerFactory(consumerFactory, latchContainer.countDownLatchBasic,
					latchContainer.customizerLatch);
		}

		@Bean
		public ConcurrentKafkaListenerContainerFactory<String, String> classLevelKafkaListenerContainerFactory(
				ConsumerFactory<String, String> consumerFactory, CountDownLatchContainer latchContainer) {

			return createKafkaListenerContainerFactory(consumerFactory, latchContainer.countDownLatchClassLevelBasic,
					latchContainer.customizerClassLevelLatch);
		}

		private ConcurrentKafkaListenerContainerFactory<String, String> createKafkaListenerContainerFactory(
				ConsumerFactory<String, String> consumerFactory, CountDownLatch countDownLatchBasic,
				CountDownLatch customizerLatch) {

			ConcurrentKafkaListenerContainerFactory<String, String> factory = new ConcurrentKafkaListenerContainerFactory<>();
			factory.setConsumerFactory(consumerFactory);
			ContainerProperties props = factory.getContainerProperties();
			props.setIdleEventInterval(100L);
			props.setPollTimeout(50L);
			props.setIdlePartitionEventInterval(100L);
			factory.setConsumerFactory(consumerFactory);
			DefaultErrorHandler errorHandler = new DefaultErrorHandler(
					(cr, ex) -> countDownLatchBasic.countDown(),
					new FixedBackOff(0, 2));
			factory.setCommonErrorHandler(errorHandler);
			factory.setConcurrency(1);
			factory.setContainerCustomizer(
					container -> customizerLatch.countDown());
			return factory;
		}

		@Bean
		public ProducerFactory<String, String> producerFactory() {
			Map<String, Object> configProps = KafkaTestUtils.producerProps(
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

		@Bean
		public ConsumerFactory<String, String> consumerFactory() {
			Map<String, Object> props = KafkaTestUtils.consumerProps(
					this.broker.getBrokersAsString(), "groupId");
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
		RetryTopicComponentFactory componentFactory() {
			return spy(new RetryTopicComponentFactory());
		}

	}

}
