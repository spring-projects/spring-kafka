/*
 * Copyright 2024-present the original author or authors.
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

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Mono;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.annotation.KafkaHandler;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.kafka.listener.ContainerProperties.AckMode;
import org.springframework.kafka.listener.KafkaListenerErrorHandler;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.kafka.test.utils.KafkaTestUtils;
import org.springframework.messaging.Message;
import org.springframework.messaging.converter.CompositeMessageConverter;
import org.springframework.messaging.converter.GenericMessageConverter;
import org.springframework.messaging.converter.SmartMessageConverter;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.scheduling.TaskScheduler;
import org.springframework.scheduling.concurrent.ThreadPoolTaskScheduler;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.context.junit.jupiter.SpringJUnitConfig;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;

/**
 * @author Sanghyeok An
 * @author Soby Chacko
 * @since 3.3.0
 */

@SpringJUnitConfig
@DirtiesContext
@EmbeddedKafka
@TestPropertySource(properties = { "five.attempts=5", "kafka.template=customKafkaTemplate"})
public class AsyncMonoRetryTopicScenarioTests {

	private final static String MAIN_TOPIC_CONTAINER_FACTORY = "kafkaListenerContainerFactory";

	public final static String TEST_TOPIC0 = "myRetryTopic0";

	public final static String TEST_TOPIC1 = "myRetryTopic1";

	@Autowired
	private KafkaTemplate<String, String> kafkaTemplate;

	@Autowired
	private CountDownLatchContainer latchContainer;

	@Autowired
	DestinationTopicContainer topicContainer;

	@Test
	void allFailCaseTest(
			@Autowired TestTopicListener0 testTopicListener,
			@Autowired MyCustomDltProcessor myCustomDltProcessor0) {
		// All Fail case.
		String shortFailedMsg1 = "0";
		String shortFailedMsg2 = "1";
		String shortFailedMsg3 = "2";
		DestinationTopic destinationTopic = topicContainer.getNextDestinationTopicFor("0-topicId", TEST_TOPIC0);

		String expectedRetryTopic = TEST_TOPIC0 + "-retry";
		String[] expectedReceivedMsgs = {
				shortFailedMsg1,
				shortFailedMsg2,
				shortFailedMsg3,
				shortFailedMsg1,
				shortFailedMsg2,
				shortFailedMsg3,
				shortFailedMsg1,
				shortFailedMsg2,
				shortFailedMsg3,
				};
		String[] expectedReceivedTopics = {
				TEST_TOPIC0,
				TEST_TOPIC0,
				TEST_TOPIC0,
				expectedRetryTopic,
				expectedRetryTopic,
				expectedRetryTopic,
				expectedRetryTopic,
				expectedRetryTopic,
				expectedRetryTopic,
				};
		String[] expectedDltMsgs = {
				shortFailedMsg1,
				shortFailedMsg2,
				shortFailedMsg3
		};

		// When
		kafkaTemplate.send(TEST_TOPIC0, shortFailedMsg1);
		kafkaTemplate.send(TEST_TOPIC0, shortFailedMsg2);
		kafkaTemplate.send(TEST_TOPIC0, shortFailedMsg3);

		// Then
		assertThat(awaitLatch(latchContainer.countDownLatch0)).isTrue();
		assertThat(awaitLatch(latchContainer.dltCountdownLatch0)).isTrue();

		assertThat(destinationTopic.getDestinationName()).isEqualTo(TEST_TOPIC0 + "-retry");

		assertThat(testTopicListener.receivedMsgs).containsExactlyInAnyOrder(expectedReceivedMsgs);
		assertThat(testTopicListener.receivedTopics).containsExactlyInAnyOrder(expectedReceivedTopics);

		assertThat(myCustomDltProcessor0.receivedMsg).containsExactlyInAnyOrder(expectedDltMsgs);
	}

	@Test
	void firstShortFailAndLastLongSuccessRetryTest(
			@Autowired TestTopicListener1 testTopicListener1,
			@Autowired MyCustomDltProcessor myCustomDltProcessor1) {
		// Scenario.
		// 1. Short Fail msg (offset 0)
		// 2. Long success msg (offset 1) -> -ing (latch wait)
		// 3. Short fail msg (Retry1 offset 0) -> (latch down)
		// 4. Long success msg (offset 1) -> Success!
		// 5. Short fail msg (Retry2 offset 0)
		// 6. Short fail msg (Retry3 offset 0)
		// 7. Short fail msg (Retry4 offset 0)

		// Given
		String longSuccessMsg = testTopicListener1.LONG_SUCCESS_MSG;
		String shortFailedMsg = testTopicListener1.SHORT_FAIL_MSG;
		DestinationTopic destinationTopic = topicContainer.getNextDestinationTopicFor("1-topicId", TEST_TOPIC1);

		String expectedRetryTopic = TEST_TOPIC1 + "-retry";
		String[] expectedReceivedMsgs = {
				shortFailedMsg,
				longSuccessMsg,
				shortFailedMsg,
				shortFailedMsg,
				shortFailedMsg,
				shortFailedMsg
		};

		String[] expectedReceivedTopics = {
				TEST_TOPIC1,
				TEST_TOPIC1,
				expectedRetryTopic,
				expectedRetryTopic,
				expectedRetryTopic,
				expectedRetryTopic
		};

		String[] expectedDltMsgs = {
				shortFailedMsg
		};

		// When
		kafkaTemplate.send(TEST_TOPIC1, shortFailedMsg);
		kafkaTemplate.send(TEST_TOPIC1, longSuccessMsg);

		// Then
		assertThat(awaitLatch(latchContainer.countDownLatch1)).isTrue();
		assertThat(awaitLatch(latchContainer.dltCountdownLatch1)).isTrue();

		assertThat(destinationTopic.getDestinationName()).isEqualTo(expectedRetryTopic);
		assertThat(testTopicListener1.receivedMsgs).containsExactlyInAnyOrder(expectedReceivedMsgs);
		assertThat(testTopicListener1.receivedTopics).containsExactlyInAnyOrder(expectedReceivedTopics);
		assertThat(testTopicListener1.latchWaitFailCount).isEqualTo(0);

		assertThat(myCustomDltProcessor1.receivedMsg).containsExactlyInAnyOrder(expectedDltMsgs);
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

	@KafkaListener(
			id = "0-topicId",
			topics = TEST_TOPIC0,
			containerFactory = MAIN_TOPIC_CONTAINER_FACTORY,
			errorHandler = "myCustomErrorHandler",
			contentTypeConverter = "myCustomMessageConverter",
			concurrency = "2")
	static class TestTopicListener0 {

		@Autowired
		CountDownLatchContainer container;

		private final List<String> receivedMsgs = Collections.synchronizedList(new ArrayList<>());

		private final List<String> receivedTopics = Collections.synchronizedList(new ArrayList<>());

		@KafkaHandler
		public Mono<Void> listen(String message, @Header(KafkaHeaders.RECEIVED_TOPIC) String receivedTopic) {
			this.receivedMsgs.add(message);
			this.receivedTopics.add(receivedTopic);
			return Mono.fromCallable(() -> {
				try {
					throw new RuntimeException("Woooops... in topic " + receivedTopic);
				}
				finally {
					container.countDownLatch0.countDown();
				}
			});
		}

	}

	@KafkaListener(
			id = "1-topicId",
			topics = TEST_TOPIC1,
			containerFactory = MAIN_TOPIC_CONTAINER_FACTORY,
			errorHandler = "myCustomErrorHandler",
			contentTypeConverter = "myCustomMessageConverter",
			concurrency = "2")
	static class TestTopicListener1 {

		@Autowired
		CountDownLatchContainer container;

		private final List<String> receivedMsgs = Collections.synchronizedList(new ArrayList<>());

		private final List<String> receivedTopics = Collections.synchronizedList(new ArrayList<>());

		private CountDownLatch firstRetryFailMsgLatch = new CountDownLatch(1);

		protected final String LONG_SUCCESS_MSG = "success";

		protected final String SHORT_FAIL_MSG = "fail";

		protected int latchWaitFailCount = 0;

		@KafkaHandler
		public Mono<String> listen(
				String message,
				@Header(KafkaHeaders.RECEIVED_TOPIC) String receivedTopic,
				@Header(KafkaHeaders.OFFSET) String offset) {
			this.receivedTopics.add(receivedTopic);
			this.receivedMsgs.add(message);
			return Mono.fromCallable(() -> {
				try {
					if (message.equals(SHORT_FAIL_MSG)) {
						throw new RuntimeException("Woooops... in topic " + receivedTopic);
					}
					else {
						firstRetryFailMsgLatch.await(10, TimeUnit.SECONDS);
					}
				}
				catch (InterruptedException e) {
					latchWaitFailCount += 1;
					throw new RuntimeException(e);
				}
				finally {
					if (receivedTopic.equals(TEST_TOPIC1 + "-retry") &&
						offset.equals("0")) {
						firstRetryFailMsgLatch.countDown();
					}
					container.countDownLatch1.countDown();
				}
				return "Task Completed";
			});
		}

	}

	static class CountDownLatchContainer {

		static int COUNT0 = 9;

		static int DLT_COUNT0 = 3;

		CountDownLatch countDownLatch0 = new CountDownLatch(COUNT0);

		CountDownLatch dltCountdownLatch0 = new CountDownLatch(DLT_COUNT0);

		static int COUNT1 = 6;

		static int DLT_COUNT1 = 1;

		CountDownLatch countDownLatch1 = new CountDownLatch(COUNT1);

		CountDownLatch dltCountdownLatch1 = new CountDownLatch(DLT_COUNT1);

	}

	static class MyCustomDltProcessor {

		final List<String> receivedMsg = Collections.synchronizedList(new ArrayList<>());

		MyCustomDltProcessor(KafkaTemplate<String, String> kafkaTemplate, CountDownLatch latch) {
			this.kafkaTemplate = kafkaTemplate;
			this.latch = latch;
		}

		private final KafkaTemplate<String, String> kafkaTemplate;

		private final CountDownLatch latch;

		public void processDltMessage(String message) {
			this.receivedMsg.add(message);
			latch.countDown();
		}
	}

	@Configuration
	static class RetryTopicConfigurations extends RetryTopicConfigurationSupport {

		private static final String DLT_METHOD_NAME = "processDltMessage";

		static  RetryTopicConfiguration createRetryTopicConfiguration(
				KafkaTemplate<String, String> template,
				String topicName,
				String dltBeanName,
				int maxAttempts) {
			return RetryTopicConfigurationBuilder
					.newInstance()
					.fixedBackOff(50)
					.maxAttempts(maxAttempts)
					.concurrency(1)
					.useSingleTopicForSameIntervals()
					.includeTopic(topicName)
					.doNotRetryOnDltFailure()
					.dltHandlerMethod(dltBeanName, DLT_METHOD_NAME)
					.create(template);
		}

		@Bean
		RetryTopicConfiguration testRetryTopic0(KafkaTemplate<String, String> template) {
			return createRetryTopicConfiguration(
					template,
					TEST_TOPIC0,
					"myCustomDltProcessor0",
					3);
		}

		@Bean
		RetryTopicConfiguration testRetryTopic1(KafkaTemplate<String, String> template) {
			return createRetryTopicConfiguration(
					template,
					TEST_TOPIC1,
					"myCustomDltProcessor1",
					5);
		}

		@Bean
		KafkaListenerErrorHandler myCustomErrorHandler(
				CountDownLatchContainer container) {
			return (message, exception) -> {
				throw exception;
			};
		}

		@Bean
		SmartMessageConverter myCustomMessageConverter(
				CountDownLatchContainer container) {
			return new CompositeMessageConverter(Collections.singletonList(new GenericMessageConverter())) {

				@Override
				public Object fromMessage(Message<?> message, Class<?> targetClass, Object conversionHint) {
					return super.fromMessage(message, targetClass, conversionHint);
				}
			};
		}

		@Bean
		CountDownLatchContainer latchContainer() {
			return new CountDownLatchContainer();
		}

		@Bean
		TestTopicListener0 testTopicListener0() {
			return new TestTopicListener0();
		}

		@Bean
		TestTopicListener1 testTopicListener1() {
			return new TestTopicListener1();
		}

		@Bean
		MyCustomDltProcessor myCustomDltProcessor0(
				KafkaTemplate<String, String> kafkaTemplate,
				CountDownLatchContainer latchContainer) {
			return new MyCustomDltProcessor(kafkaTemplate,
											latchContainer.dltCountdownLatch0);
		}

		@Bean
		MyCustomDltProcessor myCustomDltProcessor1(
				KafkaTemplate<String, String> kafkaTemplate,
				CountDownLatchContainer latchContainer) {
			return new MyCustomDltProcessor(kafkaTemplate,
											latchContainer.dltCountdownLatch1);
		}

	}

	@Configuration
	static class KafkaProducerConfig {

		@Autowired
		EmbeddedKafkaBroker broker;

		@Bean
		ProducerFactory<String, String> producerFactory() {
			Map<String, Object> props = KafkaTestUtils.producerProps(
					this.broker.getBrokersAsString());
			props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
			return new DefaultKafkaProducerFactory<>(props);
		}

		@Bean("customKafkaTemplate")
		KafkaTemplate<String, String> kafkaTemplate() {
			return new KafkaTemplate<>(producerFactory());
		}

	}

	@EnableKafka
	@Configuration(proxyBeanMethods = false)
	static class KafkaConsumerConfig {

		@Autowired
		EmbeddedKafkaBroker broker;

		@Bean
		ConsumerFactory<String, String> consumerFactory() {
			Map<String, Object> props = KafkaTestUtils.consumerProps(
					this.broker.getBrokersAsString(),
					"groupId",
					false);
			return new DefaultKafkaConsumerFactory<>(props);
		}

		@Bean
		ConcurrentKafkaListenerContainerFactory<String, String> retryTopicListenerContainerFactory(
				ConsumerFactory<String, String> consumerFactory) {

			var factory = new ConcurrentKafkaListenerContainerFactory<String, String>();
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
		ConcurrentKafkaListenerContainerFactory<String, String> kafkaListenerContainerFactory(
				ConsumerFactory<String, String> consumerFactory) {

			var factory = new ConcurrentKafkaListenerContainerFactory<String, String>();
			factory.setConsumerFactory(consumerFactory);
			factory.setConcurrency(1);
			return factory;
		}

		@Bean
		TaskScheduler sched() {
			return new ThreadPoolTaskScheduler();
		}

	}

}
