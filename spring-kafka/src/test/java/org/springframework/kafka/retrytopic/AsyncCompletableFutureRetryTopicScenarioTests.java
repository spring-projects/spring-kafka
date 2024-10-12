/*
 * Copyright 2024 the original author or authors.
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

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.Test;

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
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;
import org.springframework.scheduling.concurrent.ThreadPoolTaskScheduler;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.context.junit.jupiter.SpringJUnitConfig;

/**
 * @author Sanghyeok An
 * @since 3.3.0
 */

@SpringJUnitConfig
@DirtiesContext
@EmbeddedKafka
@TestPropertySource(properties = { "five.attempts=5", "kafka.template=customKafkaTemplate"})
public class AsyncCompletableFutureRetryTopicScenarioTests {

	private final static String MAIN_TOPIC_CONTAINER_FACTORY = "kafkaListenerContainerFactory";

	public final static String TEST_TOPIC0 = "myRetryTopic0";

	public final static String TEST_TOPIC1 = "myRetryTopic1";

	public final static String TEST_TOPIC2 = "myRetryTopic2";

	public final static String TEST_TOPIC3 = "myRetryTopic3";

	public final static String TEST_TOPIC4 = "myRetryTopic4";

	public final static String TEST_TOPIC5 = "myRetryTopic5";

	public final static String TEST_TOPIC6 = "myRetryTopic6";

	@Autowired
	private KafkaTemplate<String, String> kafkaTemplate;

	@Autowired
	private CountDownLatchContainer latchContainer;

	@Autowired
	DestinationTopicContainer topicContainer;

	@Test
	void allFailCaseTest(
			@Autowired TestTopicListener0 zeroTopicListener,
			@Autowired MyCustomDltProcessor myCustomDltProcessor0,
			@Autowired ThreadPoolTaskExecutor executor) {
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
				shortFailedMsg1,
				shortFailedMsg2,
				shortFailedMsg3,
				shortFailedMsg1,
				shortFailedMsg2,
				shortFailedMsg3
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
				expectedRetryTopic,
				expectedRetryTopic,
				expectedRetryTopic,
				expectedRetryTopic,
				expectedRetryTopic,
				expectedRetryTopic
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

		assertThat(zeroTopicListener.receivedMsgs).containsExactlyInAnyOrder(expectedReceivedMsgs);
		assertThat(zeroTopicListener.receivedTopics).containsExactlyInAnyOrder(expectedReceivedTopics);

		assertThat(myCustomDltProcessor0.receivedMsg).containsExactlyInAnyOrder(expectedDltMsgs);
	}

	@Test
	void firstShortFailAndLastLongSuccessRetryTest(
			@Autowired TestTopicListener1 testTopicListener1,
			@Autowired MyCustomDltProcessor myCustomDltProcessor1,
			@Autowired ThreadPoolTaskExecutor executor) {
		// Given
		String longSuccessMsg = "3";
		String shortFailedMsg = "1";
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

		assertThat(myCustomDltProcessor1.receivedMsg).containsExactlyInAnyOrder(expectedDltMsgs);
	}

	@Test
	void firstLongSuccessAndLastShortFailed(
			@Autowired TestTopicListener2 zero2TopicListener,
			@Autowired MyCustomDltProcessor myCustomDltProcessor2,
			@Autowired ThreadPoolTaskExecutor executor) {
		// Given
		String shortFailedMsg = "1";
		String longSuccessMsg = "3";
		DestinationTopic destinationTopic = topicContainer.getNextDestinationTopicFor("2-topicId", TEST_TOPIC2);

		String expectedRetryTopic = TEST_TOPIC2 + "-retry";
		String[] expectedReceivedMsgs = {
				longSuccessMsg,
				shortFailedMsg,
				shortFailedMsg,
				shortFailedMsg,
				shortFailedMsg,
				shortFailedMsg
		};

		String[] expectedReceivedTopics = {
				TEST_TOPIC2,
				TEST_TOPIC2,
				expectedRetryTopic,
				expectedRetryTopic,
				expectedRetryTopic,
				expectedRetryTopic
		};

		String[] expectedDltMsgs = {
				shortFailedMsg
		};

		// When
		kafkaTemplate.send(TEST_TOPIC2, longSuccessMsg);
		kafkaTemplate.send(TEST_TOPIC2, shortFailedMsg);

		// Then

		assertThat(awaitLatch(latchContainer.countDownLatch2)).isTrue();
		assertThat(awaitLatch(latchContainer.dltCountdownLatch2)).isTrue();

		assertThat(destinationTopic.getDestinationName()).isEqualTo(expectedRetryTopic);
		assertThat(zero2TopicListener.receivedMsgs).containsExactlyInAnyOrder(expectedReceivedMsgs);
		assertThat(zero2TopicListener.receivedTopics).containsExactlyInAnyOrder(expectedReceivedTopics);

		assertThat(myCustomDltProcessor2.receivedMsg).containsExactlyInAnyOrder(expectedDltMsgs);
	}

	@Test
	void longFailMsgTwiceThenShortSucessMsgThird(
			@Autowired TestTopicListener3 testTopicListener3,
			@Autowired MyCustomDltProcessor myCustomDltProcessor3,
			@Autowired ThreadPoolTaskExecutor executor) {
		// Given
		DestinationTopic destinationTopic = topicContainer.getNextDestinationTopicFor("3-topicId", TEST_TOPIC3);

		String expectedRetryTopic = TEST_TOPIC3 + "-retry";
		String[] expectedReceivedMsgs = {
				TestTopicListener3.LONG_FAIL_MSG,
				TestTopicListener3.LONG_FAIL_MSG,
				TestTopicListener3.SHORT_SUCCESS_MSG,
				TestTopicListener3.SHORT_SUCCESS_MSG,
				TestTopicListener3.SHORT_SUCCESS_MSG,
				TestTopicListener3.LONG_FAIL_MSG,
				TestTopicListener3.LONG_FAIL_MSG,
				TestTopicListener3.LONG_FAIL_MSG,
				TestTopicListener3.LONG_FAIL_MSG,
				TestTopicListener3.LONG_FAIL_MSG,
				TestTopicListener3.LONG_FAIL_MSG,
				TestTopicListener3.LONG_FAIL_MSG,
				TestTopicListener3.LONG_FAIL_MSG,
		};

		String[] expectedReceivedTopics = {
				TEST_TOPIC3,
				TEST_TOPIC3,
				TEST_TOPIC3,
				TEST_TOPIC3,
				TEST_TOPIC3,
				expectedRetryTopic,
				expectedRetryTopic,
				expectedRetryTopic,
				expectedRetryTopic,
				expectedRetryTopic,
				expectedRetryTopic,
				expectedRetryTopic,
				expectedRetryTopic
		};

		String[] expectedDltMsgs = {
				TestTopicListener3.LONG_FAIL_MSG,
				TestTopicListener3.LONG_FAIL_MSG,
				};

		// When
		kafkaTemplate.send(TEST_TOPIC3, TestTopicListener3.LONG_FAIL_MSG);
		kafkaTemplate.send(TEST_TOPIC3, TestTopicListener3.LONG_FAIL_MSG);
		kafkaTemplate.send(TEST_TOPIC3, TestTopicListener3.SHORT_SUCCESS_MSG);
		kafkaTemplate.send(TEST_TOPIC3, TestTopicListener3.SHORT_SUCCESS_MSG);
		kafkaTemplate.send(TEST_TOPIC3, TestTopicListener3.SHORT_SUCCESS_MSG);

		// Then
		assertThat(awaitLatch(latchContainer.countDownLatch3)).isTrue();
		assertThat(awaitLatch(latchContainer.dltCountdownLatch3)).isTrue();

		assertThat(destinationTopic.getDestinationName()).isEqualTo(expectedRetryTopic);
		assertThat(testTopicListener3.receivedMsgs).containsExactlyInAnyOrder(expectedReceivedMsgs);
		assertThat(testTopicListener3.receivedTopics).containsExactlyInAnyOrder(expectedReceivedTopics);

		assertThat(myCustomDltProcessor3.receivedMsg).containsExactlyInAnyOrder(expectedDltMsgs);
	}

	@Test
	void longSuccessMsgTwiceThenShortFailMsgTwice(
			@Autowired TestTopicListener4 topicListener4,
			@Autowired MyCustomDltProcessor myCustomDltProcessor4,
			@Autowired ThreadPoolTaskExecutor executor) {
		// Given
		DestinationTopic destinationTopic = topicContainer.getNextDestinationTopicFor("4-TopicId", TEST_TOPIC4);

		String expectedRetryTopic = TEST_TOPIC4 + "-retry";
		String[] expectedReceivedMsgs = {
				TestTopicListener4.LONG_SUCCESS_MSG,
				TestTopicListener4.LONG_SUCCESS_MSG,
				TestTopicListener4.SHORT_FAIL_MSG,
				TestTopicListener4.SHORT_FAIL_MSG,
				TestTopicListener4.SHORT_FAIL_MSG,
				TestTopicListener4.SHORT_FAIL_MSG,
				TestTopicListener4.SHORT_FAIL_MSG,
				TestTopicListener4.SHORT_FAIL_MSG,
				TestTopicListener4.SHORT_FAIL_MSG,
				TestTopicListener4.SHORT_FAIL_MSG,
				TestTopicListener4.SHORT_FAIL_MSG,
				TestTopicListener4.SHORT_FAIL_MSG,
				};

		String[] expectedReceivedTopics = {
				TEST_TOPIC4,
				TEST_TOPIC4,
				TEST_TOPIC4,
				TEST_TOPIC4,
				expectedRetryTopic,
				expectedRetryTopic,
				expectedRetryTopic,
				expectedRetryTopic,
				expectedRetryTopic,
				expectedRetryTopic,
				expectedRetryTopic,
				expectedRetryTopic,
		};

		String[] expectedDltMsgs = {
				TestTopicListener4.SHORT_FAIL_MSG,
				TestTopicListener4.SHORT_FAIL_MSG,
				};

		// When
		kafkaTemplate.send(TEST_TOPIC4, TestTopicListener4.LONG_SUCCESS_MSG);
		kafkaTemplate.send(TEST_TOPIC4, TestTopicListener4.LONG_SUCCESS_MSG);
		kafkaTemplate.send(TEST_TOPIC4, TestTopicListener4.SHORT_FAIL_MSG);
		kafkaTemplate.send(TEST_TOPIC4, TestTopicListener4.SHORT_FAIL_MSG);

		// Then
		assertThat(awaitLatch(latchContainer.countDownLatch4)).isTrue();
		assertThat(awaitLatch(latchContainer.dltCountdownLatch4)).isTrue();

		assertThat(destinationTopic.getDestinationName()).isEqualTo(expectedRetryTopic);
		assertThat(topicListener4.receivedMsgs).containsExactlyInAnyOrder(expectedReceivedMsgs);
		assertThat(topicListener4.receivedTopics).containsExactlyInAnyOrder(expectedReceivedTopics);

		assertThat(myCustomDltProcessor4.receivedMsg).containsExactlyInAnyOrder(expectedDltMsgs);
	}

	@Test
	void oneLongSuccessMsgBetweenHunderedShortFailMsg(
			@Autowired TestTopicListener5 topicListener5,
			@Autowired MyCustomDltProcessor myCustomDltProcessor5,
			@Autowired ThreadPoolTaskExecutor executor) {
		// Given
		DestinationTopic destinationTopic = topicContainer.getNextDestinationTopicFor("5-TopicId", TEST_TOPIC5);

		String expectedRetryTopic = TEST_TOPIC5 + "-retry";

		String[] expectedReceivedMsgs = new String[501];
		for (int i = 0; i < 500; i++) {
			expectedReceivedMsgs[i] = TestTopicListener5.SHORT_FAIL_MSG;
		}
		expectedReceivedMsgs[500] = TestTopicListener5.LONG_SUCCESS_MSG;


		String[] expectedReceivedTopics = new String[501];
		for (int i = 0; i < 100; i++) {
			expectedReceivedTopics[i] = TEST_TOPIC5;
		}
		for (int i = 100; i < 500; i++) {
			expectedReceivedTopics[i] = expectedRetryTopic;
		}
		expectedReceivedTopics[500] = TEST_TOPIC5;


		String[] expectedDltMsgs = new String[100];
		for (int i = 0; i < 100; i++) {
			expectedDltMsgs[i] = TestTopicListener5.SHORT_FAIL_MSG;
		}

		// When
		for (int i = 0; i < 100; i++) {
			kafkaTemplate.send(TEST_TOPIC5, TestTopicListener5.SHORT_FAIL_MSG);
			if (i == 50) {
				kafkaTemplate.send(TEST_TOPIC5, TestTopicListener5.LONG_SUCCESS_MSG);
			}
		}

		// Then
		assertThat(awaitLatch(latchContainer.countDownLatch5)).isTrue();
		assertThat(awaitLatch(latchContainer.dltCountdownLatch5)).isTrue();

		assertThat(destinationTopic.getDestinationName()).isEqualTo(expectedRetryTopic);
		assertThat(topicListener5.receivedMsgs).containsExactlyInAnyOrder(expectedReceivedMsgs);
		assertThat(topicListener5.receivedTopics).containsExactlyInAnyOrder(expectedReceivedTopics);

		assertThat(myCustomDltProcessor5.receivedMsg).containsExactlyInAnyOrder(expectedDltMsgs);
	}

	@Test
	void halfSuccessMsgAndHalfFailedMsgWithRandomSleepTime(
			@Autowired TestTopicListener6 topicListener6,
			@Autowired MyCustomDltProcessor myCustomDltProcessor6,
			@Autowired ThreadPoolTaskExecutor executor) {
		// Given
		DestinationTopic destinationTopic = topicContainer.getNextDestinationTopicFor("6-TopicId", TEST_TOPIC6);

		String expectedRetryTopic = TEST_TOPIC6 + "-retry";

		Random random = new Random();
		ConcurrentLinkedQueue<String> q = new ConcurrentLinkedQueue<>();

		for (int i = 0; i < 50; i++) {
			int randomSleepAWhile = random.nextInt(1, 100);
			String msg = String.valueOf(randomSleepAWhile) + TestTopicListener6.SUCCESS_SUFFIX;
			q.add(msg);
		}

		for (int i = 0; i < 50; i++) {
			int randomSleepAWhile = random.nextInt(1, 100);
			String msg = String.valueOf(randomSleepAWhile) + TestTopicListener6.FAIL_SUFFIX;
			q.add(msg);
		}

		int expectedSuccessMsgCount = 50;
		int expectedFailedMsgCount = 250;

		int expectedReceivedOriginalTopicCount = 100;
		int expectedReceivedRetryTopicCount = 200;
		int expectedReceivedDltMsgCount = 50;


		// When
		while (!q.isEmpty()) {
			String successOrFailMsg = q.poll();
			kafkaTemplate.send(TEST_TOPIC6, successOrFailMsg);
		}

		// Then
		assertThat(awaitLatch(latchContainer.countDownLatch6)).isTrue();
		assertThat(awaitLatch(latchContainer.dltCountdownLatch6)).isTrue();

		assertThat(destinationTopic.getDestinationName()).isEqualTo(expectedRetryTopic);

		long actualReceivedSuccessMsgCount = topicListener6.receivedMsgs.stream()
												.map(s -> s.split(",")[1])
												.filter(m -> (',' + m).equals(TestTopicListener6.SUCCESS_SUFFIX))
												.count();

		long actualReceivedFailedMsgCount = topicListener6.receivedMsgs.stream()
																		.map(s -> s.split(",")[1])
																		.filter(m -> (',' + m).equals(
																				TestTopicListener6.FAIL_SUFFIX))
																		.count();


		long actualReceivedOriginalTopicMsgCount = topicListener6.receivedTopics.stream()
												.filter(topic -> topic.equals(TEST_TOPIC6))
												.count();

		long actualReceivedRetryTopicMsgCount = topicListener6.receivedTopics.stream()
												.filter(topic -> topic.equals(expectedRetryTopic))
												.count();

		assertThat(actualReceivedSuccessMsgCount).isEqualTo(expectedSuccessMsgCount);
		assertThat(actualReceivedFailedMsgCount).isEqualTo(expectedFailedMsgCount);
		assertThat(actualReceivedOriginalTopicMsgCount).isEqualTo(expectedReceivedOriginalTopicCount);
		assertThat(actualReceivedRetryTopicMsgCount).isEqualTo(expectedReceivedRetryTopicCount);

		assertThat(myCustomDltProcessor6.receivedMsg.size()).isEqualTo(expectedReceivedDltMsgCount);
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

		private final List<String> receivedMsgs = new ArrayList<>();

		private final List<String> receivedTopics = new ArrayList<>();

		@KafkaHandler
		public CompletableFuture<Void> listen(String message, @Header(KafkaHeaders.RECEIVED_TOPIC) String receivedTopic) {
			this.receivedMsgs.add(message);
			this.receivedTopics.add(receivedTopic);
			return CompletableFuture.supplyAsync(() -> {
				container.countDownLatch0.countDown();
				throw new RuntimeException("Woooops... in topic " + receivedTopic);
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

		private final List<String> receivedMsgs = new ArrayList<>();

		private final List<String> receivedTopics = new ArrayList<>();

		@KafkaHandler
		public CompletableFuture<String> listen(String message, @Header(KafkaHeaders.RECEIVED_TOPIC) String receivedTopic) {
			this.receivedMsgs.add(message);
			this.receivedTopics.add(receivedTopic);
			return CompletableFuture.supplyAsync(() -> {
				try {
					Thread.sleep(Integer.parseInt(message));
				}
				catch (InterruptedException e) {
					throw new RuntimeException(e);
				}
				finally {
					container.countDownLatch1.countDown();
				}
				if (message.equals("1")) {
					throw new RuntimeException("Woooops... in topic " + receivedTopic);
				}
				return "Task Completed";
			});
		}

	}

	@KafkaListener(
			id = "2-topicId",
			topics = TEST_TOPIC2,
			containerFactory = MAIN_TOPIC_CONTAINER_FACTORY,
			errorHandler = "myCustomErrorHandler",
			contentTypeConverter = "myCustomMessageConverter",
			concurrency = "2")
	static class TestTopicListener2 {

		@Autowired
		CountDownLatchContainer container;

		protected final List<String> receivedMsgs = new ArrayList<>();

		private final List<String> receivedTopics = new ArrayList<>();

		@KafkaHandler
		public CompletableFuture<String> listen(String message, @Header(KafkaHeaders.RECEIVED_TOPIC) String receivedTopic) {
			this.receivedMsgs.add(message);
			this.receivedTopics.add(receivedTopic);

			return CompletableFuture.supplyAsync(() -> {
				try {
					Thread.sleep(Integer.parseInt(message));
				}
				catch (InterruptedException e) {
					throw new RuntimeException(e);
				}
				finally {
					container.countDownLatch2.countDown();
				}

				if (message.equals("1")) {
					throw new RuntimeException("Woooops... in topic " + receivedTopic);
				}
				return "Task Completed";
			});
		}
	}

	@KafkaListener(
			id = "3-topicId",
			topics = TEST_TOPIC3,
			containerFactory = MAIN_TOPIC_CONTAINER_FACTORY,
			errorHandler = "myCustomErrorHandler",
			contentTypeConverter = "myCustomMessageConverter",
			concurrency = "2")
	static class TestTopicListener3 {

		@Autowired
		CountDownLatchContainer container;

		protected final List<String> receivedMsgs = new ArrayList<>();

		private final List<String> receivedTopics = new ArrayList<>();

		public static final String LONG_FAIL_MSG = "5000";

		public static final String SHORT_SUCCESS_MSG = "1";

		@KafkaHandler
		public CompletableFuture<String> listen(String message, @Header(KafkaHeaders.RECEIVED_TOPIC) String receivedTopic) {
			this.receivedMsgs.add(message);
			this.receivedTopics.add(receivedTopic);

			return CompletableFuture.supplyAsync(() -> {
				try {
					Thread.sleep(Integer.parseInt(message));
				}
				catch (InterruptedException e) {
					throw new RuntimeException(e);
				}
				finally {
					container.countDownLatch3.countDown();
				}

				if (message.equals(LONG_FAIL_MSG)) {
					throw new RuntimeException("Woooops... in topic " + receivedTopic);
				}
				return "Task Completed";
			});
		}
	}

	@KafkaListener(
			id = "4-TopicId",
			topics = TEST_TOPIC4,
			containerFactory = MAIN_TOPIC_CONTAINER_FACTORY,
			errorHandler = "myCustomErrorHandler",
			contentTypeConverter = "myCustomMessageConverter",
			concurrency = "2")
	static class TestTopicListener4 {

		@Autowired
		CountDownLatchContainer container;

		protected final List<String> receivedMsgs = new ArrayList<>();

		private final List<String> receivedTopics = new ArrayList<>();

		public static final String LONG_SUCCESS_MSG = "5000";

		public static final String SHORT_FAIL_MSG = "1";

		@KafkaHandler
		public CompletableFuture<String> listen(String message, @Header(KafkaHeaders.RECEIVED_TOPIC) String receivedTopic) {
			this.receivedMsgs.add(message);
			this.receivedTopics.add(receivedTopic);

			return CompletableFuture.supplyAsync(() -> {
				try {
					Thread.sleep(Integer.parseInt(message));
				}
				catch (InterruptedException e) {
					throw new RuntimeException(e);
				}
				finally {
					container.countDownLatch4.countDown();
				}

				if (message.equals(SHORT_FAIL_MSG)) {
					throw new RuntimeException("Woooops... in topic " + receivedTopic);
				}
				return "Task Completed";
			});
		}
	}

	@KafkaListener(
			id = "5-TopicId",
			topics = TEST_TOPIC5,
			containerFactory = MAIN_TOPIC_CONTAINER_FACTORY,
			errorHandler = "myCustomErrorHandler",
			contentTypeConverter = "myCustomMessageConverter",
			concurrency = "2")
	static class TestTopicListener5 {

		@Autowired
		CountDownLatchContainer container;

		protected final List<String> receivedMsgs = new ArrayList<>();

		private final List<String> receivedTopics = new ArrayList<>();

		public static final String LONG_SUCCESS_MSG = "5000";

		public static final String SHORT_FAIL_MSG = "1";

		@KafkaHandler
		public CompletableFuture<String> listen(String message, @Header(KafkaHeaders.RECEIVED_TOPIC) String receivedTopic) {
			this.receivedMsgs.add(message);
			this.receivedTopics.add(receivedTopic);

			return CompletableFuture.supplyAsync(() -> {
				try {
					Thread.sleep(Integer.parseInt(message));
				}
				catch (InterruptedException e) {
					throw new RuntimeException(e);
				}
				finally {
					container.countDownLatch5.countDown();
				}

				if (message.startsWith(SHORT_FAIL_MSG)) {
					throw new RuntimeException("Woooops... in topic " + receivedTopic);
				}
				return "Task Completed";
			});
		}
	}

	@KafkaListener(
			id = "6-TopicId",
			topics = TEST_TOPIC6,
			containerFactory = MAIN_TOPIC_CONTAINER_FACTORY,
			errorHandler = "myCustomErrorHandler",
			contentTypeConverter = "myCustomMessageConverter",
			concurrency = "2")
	static class TestTopicListener6 {

		@Autowired
		CountDownLatchContainer container;

		protected final List<String> receivedMsgs = new ArrayList<>();

		private final List<String> receivedTopics = new ArrayList<>();

		public static final String SUCCESS_SUFFIX = ",s";

		public static final String FAIL_SUFFIX = ",f";

		@KafkaHandler
		public CompletableFuture<String> listen(String message, @Header(KafkaHeaders.RECEIVED_TOPIC) String receivedTopic) {
			this.receivedMsgs.add(message);
			this.receivedTopics.add(receivedTopic);

			return CompletableFuture.supplyAsync(() -> {
				String[] split = message.split(",");
				String sleepAWhile = split[0];
				String failOrSuccess = split[1];

				try {
					Thread.sleep(Integer.parseInt(sleepAWhile));
				}
				catch (InterruptedException e) {
					throw new RuntimeException(e);
				}
				finally {
					container.countDownLatch6.countDown();
				}

				if (failOrSuccess.equals("f")) {
					throw new RuntimeException("Woooops... in topic " + receivedTopic);
				}
				return "Task Completed";
			});
		}
	}

	static class CountDownLatchContainer {

		static int COUNT0 = 15;

		static int DLT_COUNT0 = 3;

		CountDownLatch countDownLatch0 = new CountDownLatch(COUNT0);

		CountDownLatch dltCountdownLatch0 = new CountDownLatch(DLT_COUNT0);

		static int COUNT1 = 6;

		static int DLT_COUNT1 = 1;

		CountDownLatch countDownLatch1 = new CountDownLatch(COUNT1);

		CountDownLatch dltCountdownLatch1 = new CountDownLatch(DLT_COUNT1);

		static int COUNT2 = 6;

		static int DLT_COUNT2 = 1;

		CountDownLatch countDownLatch2 = new CountDownLatch(COUNT2);

		CountDownLatch dltCountdownLatch2 = new CountDownLatch(DLT_COUNT2);

		static int COUNT3 = 13;

		static int DLT_COUNT3 = 2;

		CountDownLatch countDownLatch3 = new CountDownLatch(COUNT3);

		CountDownLatch dltCountdownLatch3 = new CountDownLatch(DLT_COUNT3);

		static int COUNT4 = 12;

		static int DLT_COUNT4 = 2;

		CountDownLatch countDownLatch4 = new CountDownLatch(COUNT4);

		CountDownLatch dltCountdownLatch4 = new CountDownLatch(DLT_COUNT4);

		static int COUNT5 = 501;

		static int DLT_COUNT5 = 100;

		CountDownLatch countDownLatch5 = new CountDownLatch(COUNT5);

		CountDownLatch dltCountdownLatch5 = new CountDownLatch(DLT_COUNT5);

		static int COUNT6 = 250;

		static int DLT_COUNT6 = 50;

		CountDownLatch countDownLatch6 = new CountDownLatch(COUNT6);

		CountDownLatch dltCountdownLatch6 = new CountDownLatch(DLT_COUNT6);

		CountDownLatch customErrorHandlerCountdownLatch = new CountDownLatch(6);

		CountDownLatch customMessageConverterCountdownLatch = new CountDownLatch(6);

	}

	static class MyCustomDltProcessor {

		final List<String> receivedMsg = new ArrayList<>();

		MyCustomDltProcessor(KafkaTemplate<String, String> kafkaTemplate,
									CountDownLatch latch) {
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
				String dltBeanName) {
			return RetryTopicConfigurationBuilder
					.newInstance()
					.fixedBackOff(50)
					.maxAttempts(5)
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
					"myCustomDltProcessor0");
		}

		@Bean
		RetryTopicConfiguration testRetryTopic1(KafkaTemplate<String, String> template) {
			return createRetryTopicConfiguration(
					template,
					TEST_TOPIC1,
					"myCustomDltProcessor1");
		}

		@Bean
		RetryTopicConfiguration testRetryTopic2(KafkaTemplate<String, String> template) {
			return createRetryTopicConfiguration(
					template,
					TEST_TOPIC2,
					"myCustomDltProcessor2");
		}

		@Bean
		RetryTopicConfiguration testRetryTopic3(KafkaTemplate<String, String> template) {
			return createRetryTopicConfiguration(
					template,
					TEST_TOPIC3,
					"myCustomDltProcessor3");
		}

		@Bean
		RetryTopicConfiguration testRetryTopic4(KafkaTemplate<String, String> template) {
			return createRetryTopicConfiguration(
					template,
					TEST_TOPIC4,
					"myCustomDltProcessor4");
		}

		@Bean
		RetryTopicConfiguration testRetryTopic5(KafkaTemplate<String, String> template) {
			return createRetryTopicConfiguration(
					template,
					TEST_TOPIC5,
					"myCustomDltProcessor5");
		}

		@Bean
		RetryTopicConfiguration testRetryTopic6(KafkaTemplate<String, String> template) {
			return createRetryTopicConfiguration(
					template,
					TEST_TOPIC6,
					"myCustomDltProcessor6");
		}

		@Bean
		KafkaListenerErrorHandler myCustomErrorHandler(
				CountDownLatchContainer container) {
			return (message, exception) -> {
				container.customErrorHandlerCountdownLatch.countDown();
				throw exception;
			};
		}

		@Bean
		SmartMessageConverter myCustomMessageConverter(
				CountDownLatchContainer container) {
			return new CompositeMessageConverter(Collections.singletonList(new GenericMessageConverter())) {

				@Override
				public Object fromMessage(Message<?> message, Class<?> targetClass, Object conversionHint) {
					container.customMessageConverterCountdownLatch.countDown();
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
		TestTopicListener2 testTopicListener2() {
			return new TestTopicListener2();
		}

		@Bean
		TestTopicListener3 testTopicListener3() {
			return new TestTopicListener3();
		}

		@Bean
		TestTopicListener4 testTopicListener4() {
			return new TestTopicListener4();
		}

		@Bean
		TestTopicListener5 testTopicListener5() {
			return new TestTopicListener5();
		}

		@Bean
		TestTopicListener6 testTopicListener6() {
			return new TestTopicListener6();
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

		@Bean
		MyCustomDltProcessor myCustomDltProcessor2(
				KafkaTemplate<String, String> kafkaTemplate,
				CountDownLatchContainer latchContainer) {
			return new MyCustomDltProcessor(kafkaTemplate,
											latchContainer.dltCountdownLatch2);
		}

		@Bean
		MyCustomDltProcessor myCustomDltProcessor3(
				KafkaTemplate<String, String> kafkaTemplate,
				CountDownLatchContainer latchContainer) {
			return new MyCustomDltProcessor(kafkaTemplate,
											latchContainer.dltCountdownLatch3);
		}

		@Bean
		MyCustomDltProcessor myCustomDltProcessor4(
				KafkaTemplate<String, String> kafkaTemplate,
				CountDownLatchContainer latchContainer) {
			return new MyCustomDltProcessor(kafkaTemplate,
											latchContainer.dltCountdownLatch4);
		}

		@Bean
		MyCustomDltProcessor myCustomDltProcessor5(
				KafkaTemplate<String, String> kafkaTemplate,
				CountDownLatchContainer latchContainer) {
			return new MyCustomDltProcessor(kafkaTemplate,
											latchContainer.dltCountdownLatch5);
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
					"false");
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

		@Bean
		ThreadPoolTaskExecutor threadPoolTaskExecutor() {
			return new ThreadPoolTaskExecutor();
		}

	}

}
