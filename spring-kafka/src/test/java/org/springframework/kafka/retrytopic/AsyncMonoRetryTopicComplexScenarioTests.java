/*
 * Copyright 2026-present the original author or authors.
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
import org.springframework.beans.factory.annotation.Qualifier;
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
 * @since 4.0.0
 */

@SpringJUnitConfig
@DirtiesContext
@EmbeddedKafka
@TestPropertySource(properties = { "five.attempts=5", "kafka.template=customKafkaTemplate"})
public class AsyncMonoRetryTopicComplexScenarioTests {

	private final static String MAIN_TOPIC_CONTAINER_FACTORY = "kafkaListenerContainerFactory";

	public final static String TEST_TOPIC6 = "myRetryTopic6";

	@Autowired
	private KafkaTemplate<String, String> kafkaTemplate;

	@Autowired
	private CountDownLatchContainer latchContainer;

	@Autowired
	DestinationTopicContainer topicContainer;

	@Test
	void moreComplexAsyncScenarioTest(
			@Autowired TestTopicListener topicListener6,
			@Autowired @Qualifier("myCustomDltProcessor6")
			MyCustomDltProcessor myCustomDltProcessor6) {
		// Scenario.
		// 1. Fail Msg (offset 0) -> -ing
		// 2. Success Msg (offset 1) -> -ing
		// 3. Success Msg (offset 2) -> -ing
		// 4. Fail Msg (offset 3) -> done
		// 5. Success Msg (offset 4) -> -ing
		// 6. Success msg succeed (offset 2) - done
		// 7. Success msg succeed (offset 4) -> done
		// 8. Fail Msg (Retry1 offset 3) -> done
		// 9. Fail Msg (Retry2 offset 3) -> done
		// 10. Success msg succeed (offset 1) -> done
		// 11. Fail Msg (offset 0) -> done
		// 12. Fail Msg (Retry 1 offset 0) -> done
		// 13. Fail Msg (Retry 2 offset 0) -> done

		// Given
		String firstMsg = TestTopicListener.FAIL_PREFIX + "0";
		String secondMsg = TestTopicListener.SUCCESS_PREFIX + "1";
		String thirdMsg = TestTopicListener.SUCCESS_PREFIX + "2";
		String fourthMsg = TestTopicListener.FAIL_PREFIX + "3";
		String fifthMsg = TestTopicListener.SUCCESS_PREFIX + "4";

		DestinationTopic destinationTopic = topicContainer.getNextDestinationTopicFor("6-TopicId", TEST_TOPIC6);
		String expectedRetryTopic = TEST_TOPIC6 + "-retry";

		String[] expectedReceivedMsgs = {
				firstMsg,
				secondMsg,
				thirdMsg,
				fourthMsg,
				fifthMsg,
				fourthMsg,
				fourthMsg,
				firstMsg,
				firstMsg
		};

		String[] expectedReceivedTopics = {
				TEST_TOPIC6,
				TEST_TOPIC6,
				TEST_TOPIC6,
				TEST_TOPIC6,
				TEST_TOPIC6,
				expectedRetryTopic,
				expectedRetryTopic,
				expectedRetryTopic,
				expectedRetryTopic
		};

		String[] expectedDltMsgs = {
				TestTopicListener.FAIL_PREFIX + "3",
				TestTopicListener.FAIL_PREFIX + "0"
		};

		// When
		kafkaTemplate.send(TEST_TOPIC6, firstMsg);
		kafkaTemplate.send(TEST_TOPIC6, secondMsg);
		kafkaTemplate.send(TEST_TOPIC6, thirdMsg);
		kafkaTemplate.send(TEST_TOPIC6, fourthMsg);
		kafkaTemplate.send(TEST_TOPIC6, fifthMsg);

		// Then
		assertThat(awaitLatch(latchContainer.countDownLatch6)).isTrue();
		assertThat(awaitLatch(latchContainer.dltCountdownLatch6)).isTrue();

		assertThat(destinationTopic.getDestinationName()).isEqualTo(expectedRetryTopic);
		assertThat(topicListener6.receivedMsgs).containsExactlyInAnyOrder(expectedReceivedMsgs);
		assertThat(topicListener6.receivedTopics).containsExactlyInAnyOrder(expectedReceivedTopics);
		assertThat(topicListener6.latchWaitFailCount).isEqualTo(0);

		assertThat(myCustomDltProcessor6.receivedMsg).containsExactlyInAnyOrder(expectedDltMsgs);
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
			id = "6-TopicId",
			topics = TEST_TOPIC6,
			containerFactory = MAIN_TOPIC_CONTAINER_FACTORY,
			errorHandler = "myCustomErrorHandler",
			contentTypeConverter = "myCustomMessageConverter",
			concurrency = "2")
	static class TestTopicListener {

		@Autowired
		CountDownLatchContainer container;

		protected final List<String> receivedMsgs = Collections.synchronizedList(new ArrayList<>());

		private final List<String> receivedTopics = Collections.synchronizedList(new ArrayList<>());

		public static final String SUCCESS_PREFIX = "success";

		public static final String FAIL_PREFIX = "fail";

		protected CountDownLatch offset1CompletedLatch = new CountDownLatch(1);

		protected CountDownLatch offset2CompletedLatch = new CountDownLatch(1);

		protected CountDownLatch offset3RetryCompletedLatch = new CountDownLatch(3);

		protected CountDownLatch offset4ReceivedLatch = new CountDownLatch(1);

		protected int latchWaitFailCount = 0;

		@KafkaHandler
		public Mono<String> listen(
				String message,
				@Header(KafkaHeaders.RECEIVED_TOPIC) String receivedTopic,
				@Header(KafkaHeaders.OFFSET) String offset) {
			this.receivedMsgs.add(message);
			this.receivedTopics.add(receivedTopic);

			return Mono.fromCallable(() -> {
				try {
					if (message.startsWith(FAIL_PREFIX)) {
						if (offset.equals("0")) {
							if (receivedTopic.equals(TEST_TOPIC6)) {
								offset1CompletedLatch.await(10, TimeUnit.SECONDS);
							}
						}

						if (offset.equals("3")) {
							offset3RetryCompletedLatch.countDown();
						}

						throw new RuntimeException("Woooops... in topic " + receivedTopic + "msg : " + message);
					}
					else {
						if (offset.equals("1")) {
							offset3RetryCompletedLatch.await(10, TimeUnit.SECONDS);
							offset1CompletedLatch.countDown();
						}

						if (offset.equals("2")) {
							offset4ReceivedLatch.await(10, TimeUnit.SECONDS);
							offset2CompletedLatch.countDown();
						}

						if (offset.equals("4")) {
							offset4ReceivedLatch.countDown();
							offset2CompletedLatch.await(10, TimeUnit.SECONDS);
						}
					}
				}
				catch (InterruptedException ex) {
					latchWaitFailCount += 1;
					throw new RuntimeException(ex);
				}
				finally {
					container.countDownLatch6.countDown();
				}

				return "Task Completed";
			});
		}
	}

	static class CountDownLatchContainer {

		static int COUNT6 = 9;

		static int DLT_COUNT6 = 2;

		CountDownLatch countDownLatch6 = new CountDownLatch(COUNT6);

		CountDownLatch dltCountdownLatch6 = new CountDownLatch(DLT_COUNT6);

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

		@Bean
		RetryTopicConfiguration testRetryTopic6(KafkaTemplate<String, String> template) {
			return RetryTopicConfigurationBuilder
					.newInstance()
					.fixedBackOff(50)
					.maxAttempts(3)
					.concurrency(1)
					.useSingleTopicForSameIntervals()
					.includeTopic(TEST_TOPIC6)
					.doNotRetryOnDltFailure()
					.dltHandlerMethod("myCustomDltProcessor6", DLT_METHOD_NAME)
					.create(template);
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
		TestTopicListener testTopicListener6() {
			return new TestTopicListener();
		}

		@Bean
		MyCustomDltProcessor myCustomDltProcessor6(
				KafkaTemplate<String, String> kafkaTemplate,
				CountDownLatchContainer latchContainer) {
			return new MyCustomDltProcessor(kafkaTemplate,
											latchContainer.dltCountdownLatch6);
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
	@Configuration
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
		ConcurrentKafkaListenerContainerFactory<String, String> kafkaListenerContainerFactory(
				ConsumerFactory<String, String> consumerFactory) {

			ConcurrentKafkaListenerContainerFactory<String, String> factory = new ConcurrentKafkaListenerContainerFactory<>();
			factory.setConsumerFactory(consumerFactory);
			factory.setConcurrency(1);
			factory.setContainerCustomizer(container -> {
				if (container.getListenerId().startsWith("manual")) {
					container.getContainerProperties().setAckMode(AckMode.MANUAL);
					container.getContainerProperties().setAsyncAcks(true);
				}
			});
			return factory;
		}

		@Bean
		TaskScheduler sched() {
			return new ThreadPoolTaskScheduler();
		}

	}

}
