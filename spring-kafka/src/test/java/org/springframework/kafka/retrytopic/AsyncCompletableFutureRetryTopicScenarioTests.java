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
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.DisabledIfEnvironmentVariable;

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
 * @author Artem Bilan
 *
 * @since 3.3.0
 */

@SpringJUnitConfig
@DirtiesContext
@EmbeddedKafka
@TestPropertySource(properties = { "five.attempts=5", "kafka.template=customKafkaTemplate"})
@DisabledIfEnvironmentVariable(named = "GITHUB_ACTION", matches = "true",
		disabledReason = "The test is too heavy and rely a lot in the timing.")
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

	@Test
	void firstLongSuccessAndLastShortFailed(
			@Autowired TestTopicListener2 testTopicListener2,
			@Autowired MyCustomDltProcessor myCustomDltProcessor2) {
		// Scenario.
		// 1. Long success msg (offset 0) -> going on... (latch await)
		// 2. Short fail msg (offset 1) -> done.
		// 3. Short fail msg (Retry1 offset 1) -> done (latch down)
		// 4. Long success msg (offset 0) -> succeed.
		// 5. Short fail msg (Retry2 offset 1)
		// 6. Short fail msg (Retry3 offset 1)
		// 7. Short fail msg (Retry4 offset 1)
		// 8. Short fail msg (dlt offset 1)

		// Given
		String shortFailedMsg = testTopicListener2.SHORT_FAIL_MSG;
		String longSuccessMsg = testTopicListener2.LONG_SUCCESS_MSG;
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
		assertThat(testTopicListener2.receivedMsgs).containsExactlyInAnyOrder(expectedReceivedMsgs);
		assertThat(testTopicListener2.receivedTopics).containsExactlyInAnyOrder(expectedReceivedTopics);
		assertThat(testTopicListener2.latchWaitFailCount).isEqualTo(0);

		assertThat(myCustomDltProcessor2.receivedMsg).containsExactlyInAnyOrder(expectedDltMsgs);
	}

	@Test
	void longFailMsgTwiceThenShortSuccessMsgThird(
			@Autowired TestTopicListener3 testTopicListener3,
			@Autowired MyCustomDltProcessor myCustomDltProcessor3) {
		// Scenario
		// 1. Long fail msg arrived (offset 0) -> -ing (wait latch offset 4)
		// 2. Long fail msg arrived (offset 1) -> -ing (wait latch offset 1)
		// 3. Short success msg arrived (offset 2) -> done
		// 4. Short success msg arrived (offset 3) -> done
		// 5. Short success msg arrived (offset 4) -> done (latch offset 4 count down)
		// 6. Long fail msg throws error (offset 0) -> done
		// 7. Long fail msg throws error (offset 1) -> done
		// 8. Long fail msg (retry 1 with offset 0) -> done
		// 9. Long fail msg (retry 1 with offset 1) -> done
		// 10. Long fail msg (retry 2 with offset 0) -> done
		// 11. Long fail msg (retry 2 with offset 1) -> done
		// 12. Long fail msg (retry 3 with offset 0) -> done
		// 13. Long fail msg (retry 3 with offset 1) -> done
		// 14. Long fail msg (retry 4 with offset 0) -> done
		// 15. Long fail msg (retry 4 with offset 1) -> done

		// Given
		DestinationTopic destinationTopic = topicContainer.getNextDestinationTopicFor("3-topicId", TEST_TOPIC3);

		String firstMsg = TestTopicListener3.FAIL_PREFIX + "0";
		String secondMsg = TestTopicListener3.FAIL_PREFIX + "1";
		String thirdMsg = TestTopicListener3.SUCCESS_PREFIX + "2";
		String fourthMsg = TestTopicListener3.SUCCESS_PREFIX + "3";
		String fifthMsg = TestTopicListener3.SUCCESS_PREFIX + "4";

		String expectedRetryTopic = TEST_TOPIC3 + "-retry";

		String[] expectedReceivedMsgs = {
				firstMsg,
				secondMsg,
				thirdMsg,
				fourthMsg,
				fifthMsg,
				firstMsg,
				secondMsg,
				firstMsg,
				secondMsg,
				firstMsg,
				secondMsg,
				firstMsg,
				secondMsg,
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
				firstMsg,
				secondMsg,
				};

		// When
		kafkaTemplate.send(TEST_TOPIC3, firstMsg);
		kafkaTemplate.send(TEST_TOPIC3, secondMsg);
		kafkaTemplate.send(TEST_TOPIC3, thirdMsg);
		kafkaTemplate.send(TEST_TOPIC3, fourthMsg);
		kafkaTemplate.send(TEST_TOPIC3, fifthMsg);

		// Then
		assertThat(awaitLatch(latchContainer.countDownLatch3)).isTrue();
		assertThat(awaitLatch(latchContainer.dltCountdownLatch3)).isTrue();

		assertThat(destinationTopic.getDestinationName()).isEqualTo(expectedRetryTopic);
		assertThat(testTopicListener3.receivedMsgs).containsExactlyInAnyOrder(expectedReceivedMsgs);
		assertThat(testTopicListener3.receivedTopics).containsExactlyInAnyOrder(expectedReceivedTopics);
		assertThat(testTopicListener3.latchWaitFailCount).isEqualTo(0);

		assertThat(myCustomDltProcessor3.receivedMsg).containsExactlyInAnyOrder(expectedDltMsgs);
	}

	@Test
	void longSuccessMsgTwiceThenShortFailMsgTwice(
			@Autowired TestTopicListener4 topicListener4,
			@Autowired MyCustomDltProcessor myCustomDltProcessor4) {
		// Scenario
		// 1. Msg arrived (offset 0) -> -ing
		// 2. Msg arrived (offset 1) -> -ing
		// 3. Msg arrived (offset 2) throws error -> done
		// 4. Msg arrived (offset 3) throws error -> done
		// 5. Msg arrived (offset 0) succeed -> done
		// 6. Msg arrived (offset 1) succeed -> done
		// 7. Msg arrived (retry 1, offset 2) -> done
		// 8. Msg arrived (retry 1, offset 3) -> done
		// 9. Msg arrived (retry 2, offset 2) -> done
		// 10. Msg arrived (retry 2, offset 3) -> done
		// 11. Msg arrived (retry 3, offset 2) -> done
		// 12. Msg arrived (retry 3, offset 3) -> done
		// 13. Msg arrived (retry 4, offset 2) -> done
		// 14. Msg arrived (retry 4, offset 3) -> done

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
		assertThat(topicListener4.latchWaitFailCount).isEqualTo(0);

		assertThat(myCustomDltProcessor4.receivedMsg).containsExactlyInAnyOrder(expectedDltMsgs);
	}

	@Test
	void oneLongSuccessMsgBetween49ShortFailMsg(
			@Autowired TestTopicListener5 topicListener5,
			@Autowired MyCustomDltProcessor myCustomDltProcessor5) {
		// Scenario.
		// 1. msgs received (offsets 0 ~ 24) -> failed.
		// 2. msgs received (offset 25) -> -ing
		// 3. msgs received (offset 26 ~ 49) -> failed.
		// 4. msgs succeed (offset 50) -> done
		// 5. msgs received (Retry1 offset 0 ~ 49 except 25) -> failed.
		// 6. msgs received (Retry2 offset 0 ~ 49 except 25) -> failed.
		// 7. msgs received (Retry3 offset 0 ~ 49 except 25) -> failed.
		// 8. msgs received (Retry4 offset 0 ~ 49 except 25) -> failed.

		// Given
		DestinationTopic destinationTopic = topicContainer.getNextDestinationTopicFor("5-TopicId", TEST_TOPIC5);

		String expectedRetryTopic = TEST_TOPIC5 + "-retry";

		String[] expectedReceivedMsgs = new String[148];
		for (int i = 0; i < 147; i++) {
			expectedReceivedMsgs[i] = TestTopicListener5.SHORT_FAIL_MSG;
		}
		expectedReceivedMsgs[147] = TestTopicListener5.LONG_SUCCESS_MSG;


		String[] expectedReceivedTopics = new String[148];
		for (int i = 0; i < 49; i++) {
			expectedReceivedTopics[i] = TEST_TOPIC5;
		}
		for (int i = 49; i < 147; i++) {
			expectedReceivedTopics[i] = expectedRetryTopic;
		}
		expectedReceivedTopics[147] = TEST_TOPIC5;


		String[] expectedDltMsgs = new String[49];
		for (int i = 0; i < 49; i++) {
			expectedDltMsgs[i] = TestTopicListener5.SHORT_FAIL_MSG;
		}

		// When
		for (int i = 0; i < 50; i++) {
			if (i != 25) {
				kafkaTemplate.send(TEST_TOPIC5, TestTopicListener5.SHORT_FAIL_MSG);
			}
			else {
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
	void moreComplexAsyncScenarioTest(
			@Autowired TestTopicListener6 topicListener6,
			@Autowired @Qualifier("myCustomDltProcessor6") MyCustomDltProcessor myCustomDltProcessor6) {
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
		String firstMsg = TestTopicListener6.FAIL_PREFIX + "0";
		String secondMsg = TestTopicListener6.SUCCESS_PREFIX + "1";
		String thirdMsg = TestTopicListener6.SUCCESS_PREFIX + "2";
		String fourthMsg = TestTopicListener6.FAIL_PREFIX + "3";
		String fifthMsg = TestTopicListener6.SUCCESS_PREFIX + "4";

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
				TestTopicListener6.FAIL_PREFIX + "3",
				TestTopicListener6.FAIL_PREFIX + "0"
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
		public CompletableFuture<Void> listen(String message, @Header(KafkaHeaders.RECEIVED_TOPIC) String receivedTopic) {
			this.receivedMsgs.add(message);
			this.receivedTopics.add(receivedTopic);
			return CompletableFuture.supplyAsync(() -> {
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
		public CompletableFuture<String> listen(
				String message,
				@Header(KafkaHeaders.RECEIVED_TOPIC) String receivedTopic,
				@Header(KafkaHeaders.OFFSET) String offset) {
			this.receivedTopics.add(receivedTopic);
			this.receivedMsgs.add(message);
			return CompletableFuture.supplyAsync(() -> {
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

		private final List<String> receivedMsgs = Collections.synchronizedList(new ArrayList<>());

		private final List<String> receivedTopics = Collections.synchronizedList(new ArrayList<>());

		private CountDownLatch firstRetryFailMsgLatch = new CountDownLatch(1);

		protected final String LONG_SUCCESS_MSG = "success";

		protected final String SHORT_FAIL_MSG = "fail";

		protected int latchWaitFailCount = 0;

		@KafkaHandler
		public CompletableFuture<String> listen(
				String message,
				@Header(KafkaHeaders.RECEIVED_TOPIC) String receivedTopic,
				@Header(KafkaHeaders.OFFSET) String offset) {
			this.receivedMsgs.add(message);
			this.receivedTopics.add(receivedTopic);

			return CompletableFuture.supplyAsync(() -> {
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
					if (receivedTopic.equals(TEST_TOPIC2 + "-retry") &&
						offset.equals("1")) {
						firstRetryFailMsgLatch.countDown();
					}
					container.countDownLatch2.countDown();
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

		private final List<String> receivedMsgs = Collections.synchronizedList(new ArrayList<>());

		private final List<String> receivedTopics = Collections.synchronizedList(new ArrayList<>());

		public static final String FAIL_PREFIX = "fail";

		public static final String SUCCESS_PREFIX = "success";

		private CountDownLatch successLatchCount = new CountDownLatch(3);

		private CountDownLatch offset0Latch = new CountDownLatch(1);

		protected int latchWaitFailCount = 0;

		@KafkaHandler
		public CompletableFuture<String> listen(
				String message,
				@Header(KafkaHeaders.RECEIVED_TOPIC)String receivedTopic,
				@Header(KafkaHeaders.OFFSET) String offset) {
			this.receivedMsgs.add(message);
			this.receivedTopics.add(receivedTopic);

			return CompletableFuture.supplyAsync(() -> {
				try {
					if (message.startsWith(FAIL_PREFIX)) {
						if (receivedTopic.equals(TEST_TOPIC3)) {
							if (offset.equals("0")) {
								successLatchCount.await(10, TimeUnit.SECONDS);
								offset0Latch.countDown();
							}
							if (offset.equals("1")) {
								offset0Latch.await(10, TimeUnit.SECONDS);
							}
					}
						throw new RuntimeException("Woooops... in topic " + receivedTopic);
					}
					else {
						successLatchCount.countDown();
					}
				}
				catch (InterruptedException e) {
					latchWaitFailCount += 1;
					throw new RuntimeException(e);
				}
				finally {
					container.countDownLatch3.countDown();
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

		private final List<String> receivedMsgs = Collections.synchronizedList(new ArrayList<>());

		private final List<String> receivedTopics = Collections.synchronizedList(new ArrayList<>());

		public static final String LONG_SUCCESS_MSG = "success";

		public static final String SHORT_FAIL_MSG = "fail";

		private CountDownLatch failLatchCount = new CountDownLatch(2);

		private CountDownLatch offset0Latch = new CountDownLatch(1);

		protected int latchWaitFailCount = 0;

		@KafkaHandler
		public CompletableFuture<String> listen(String message,
												@Header(KafkaHeaders.RECEIVED_TOPIC) String receivedTopic,
												@Header(KafkaHeaders.OFFSET) String offset) {
			this.receivedMsgs.add(message);
			this.receivedTopics.add(receivedTopic);

			return CompletableFuture.supplyAsync(() -> {
				try {
					if (message.equals(SHORT_FAIL_MSG)) {
						throw new RuntimeException("Woooops... in topic " + receivedTopic);
					}
					else {
						failLatchCount.await(10, TimeUnit.SECONDS);
						if (offset.equals("1")) {
							offset0Latch.await(10, TimeUnit.SECONDS);
						}
					}
				}
				catch (InterruptedException e) {
					latchWaitFailCount += 1;
					throw new RuntimeException(e);
				}
				finally {
					if (message.equals(SHORT_FAIL_MSG) ||
						receivedTopic.equals(TEST_TOPIC4)) {
						failLatchCount.countDown();
					}
					if (offset.equals("0") &&
						receivedTopic.equals(TEST_TOPIC4)) {
						offset0Latch.countDown();
					}
					container.countDownLatch4.countDown();
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

		private final List<String> receivedMsgs = Collections.synchronizedList(new ArrayList<>());

		private final List<String> receivedTopics = Collections.synchronizedList(new ArrayList<>());

		public static final String LONG_SUCCESS_MSG = "success";

		public static final String SHORT_FAIL_MSG = "fail";

		private CountDownLatch failLatchCount = new CountDownLatch(24 + 49);

		protected int latchWaitFailCount = 0;

		@KafkaHandler
		public CompletableFuture<String> listen(
				String message,
				@Header(KafkaHeaders.RECEIVED_TOPIC) String receivedTopic,
				@Header(KafkaHeaders.OFFSET) String offset) {
			this.receivedMsgs.add(message);
			this.receivedTopics.add(receivedTopic);

			return CompletableFuture.supplyAsync(() -> {
				try {
					if (message.equals(SHORT_FAIL_MSG)) {
						throw new RuntimeException("Woooops... in topic " + receivedTopic);
					}
					else {
						failLatchCount.await(10, TimeUnit.SECONDS);
					}
				}
				catch (InterruptedException e) {
					latchWaitFailCount += 1;
					throw new RuntimeException(e);
				}
				finally {
					if (message.equals(SHORT_FAIL_MSG)) {
						if (receivedTopic.equals(TEST_TOPIC5) &&
							Integer.valueOf(offset) > 25) {
							failLatchCount.countDown();
						}
						else {
							if (failLatchCount.getCount() > 0) {
								failLatchCount.countDown();
							}
						}
					}
					container.countDownLatch5.countDown();
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

		private final List<String> receivedMsgs = Collections.synchronizedList(new ArrayList<>());

		private final List<String> receivedTopics = Collections.synchronizedList(new ArrayList<>());

		public static final String SUCCESS_PREFIX = "success";

		public static final String FAIL_PREFIX = "fail";

		protected CountDownLatch offset1CompletedLatch = new CountDownLatch(1);

		protected CountDownLatch offset2CompletedLatch = new CountDownLatch(1);

		protected CountDownLatch offset3RetryCompletedLatch = new CountDownLatch(3);

		protected CountDownLatch offset4ReceivedLatch = new CountDownLatch(1);

		protected int latchWaitFailCount = 0;

		@KafkaHandler
		public CompletableFuture<String> listen(
				String message,
				@Header(KafkaHeaders.RECEIVED_TOPIC) String receivedTopic,
				@Header(KafkaHeaders.OFFSET) String offset) {
			this.receivedMsgs.add(message);
			this.receivedTopics.add(receivedTopic);

			return CompletableFuture.supplyAsync(() -> {
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

		static int COUNT0 = 9;

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

		static int COUNT5 = 24 + 73;

		static int DLT_COUNT5 = 49;

		CountDownLatch countDownLatch5 = new CountDownLatch(COUNT5);

		CountDownLatch dltCountdownLatch5 = new CountDownLatch(DLT_COUNT5);

		static int COUNT6 = 9;

		static int DLT_COUNT6 = 2;

		CountDownLatch countDownLatch6 = new CountDownLatch(COUNT6);

		CountDownLatch dltCountdownLatch6 = new CountDownLatch(DLT_COUNT6);

	}

	static class MyCustomDltProcessor {

		private final List<String> receivedMsg = Collections.synchronizedList(new ArrayList<>());

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
		RetryTopicConfiguration testRetryTopic2(KafkaTemplate<String, String> template) {
			return createRetryTopicConfiguration(
					template,
					TEST_TOPIC2,
					"myCustomDltProcessor2",
					5);
		}

		@Bean
		RetryTopicConfiguration testRetryTopic3(KafkaTemplate<String, String> template) {
			return createRetryTopicConfiguration(
					template,
					TEST_TOPIC3,
					"myCustomDltProcessor3",
					5);
		}

		@Bean
		RetryTopicConfiguration testRetryTopic4(KafkaTemplate<String, String> template) {
			return createRetryTopicConfiguration(
					template,
					TEST_TOPIC4,
					"myCustomDltProcessor4",
					5);
		}

		@Bean
		RetryTopicConfiguration testRetryTopic5(KafkaTemplate<String, String> template) {
			return createRetryTopicConfiguration(
					template,
					TEST_TOPIC5,
					"myCustomDltProcessor5",
					3);
		}

		@Bean
		RetryTopicConfiguration testRetryTopic6(KafkaTemplate<String, String> template) {
			return createRetryTopicConfiguration(
					template,
					TEST_TOPIC6,
					"myCustomDltProcessor6",
					3);
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
