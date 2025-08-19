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

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.assertj.core.api.InstanceOfAssertFactories;
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
import org.springframework.kafka.annotation.PartitionOffset;
import org.springframework.kafka.annotation.RetryableTopic;
import org.springframework.kafka.annotation.TopicPartition;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaAdmin;
import org.springframework.kafka.core.KafkaAdmin.NewTopics;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.listener.ConcurrentMessageListenerContainer;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.kafka.listener.ContainerProperties.AckMode;
import org.springframework.kafka.listener.KafkaListenerErrorHandler;
import org.springframework.kafka.support.Acknowledgment;
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
import static org.awaitility.Awaitility.await;


/**
 * @author Tomaz Fernandes
 * @author Gary Russell
 * @author Wang Zhiyang
 * @author Sanghyeok An
 * @since 2.7
 */
@SpringJUnitConfig
@DirtiesContext
@EmbeddedKafka(topics = { RetryTopicIntegrationTests.FIRST_TOPIC,
		RetryTopicIntegrationTests.SECOND_TOPIC,
		RetryTopicIntegrationTests.THIRD_TOPIC,
		RetryTopicIntegrationTests.FOURTH_TOPIC,
		RetryTopicIntegrationTests.TWO_LISTENERS_TOPIC,
		RetryTopicIntegrationTests.MANUAL_TOPIC })
@TestPropertySource(properties = { "five.attempts=5", "kafka.template=customKafkaTemplate"})
public class RetryTopicIntegrationTests {

	private static final Logger logger = LoggerFactory.getLogger(RetryTopicIntegrationTests.class);

	public final static String FIRST_TOPIC = "myRetryTopic1";

	public final static String SECOND_TOPIC = "myRetryTopic2";

	public final static String THIRD_TOPIC = "myRetryTopic3";

	public final static String FOURTH_TOPIC = "myRetryTopic4";

	public final static String TWO_LISTENERS_TOPIC = "myRetryTopic5";

	public final static String MANUAL_TOPIC = "myRetryTopic6";

	public final static String NOT_RETRYABLE_EXCEPTION_TOPIC = "noRetryTopic";

	public final static String FIRST_REUSE_RETRY_TOPIC = "reuseRetry1";

	public final static String SECOND_REUSE_RETRY_TOPIC = "reuseRetry2";

	public final static String THIRD_REUSE_RETRY_TOPIC = "reuseRetry3";

	private final static String MAIN_TOPIC_CONTAINER_FACTORY = "kafkaListenerContainerFactory";

	@Autowired
	private KafkaTemplate<String, String> kafkaTemplate;

	@Autowired
	private CountDownLatchContainer latchContainer;

	@Autowired
	DestinationTopicContainer topicContainer;

	@Test
	void shouldRetryFirstTopic(@Autowired KafkaListenerEndpointRegistry registry) {
		logger.debug("Sending message to topic " + FIRST_TOPIC);
		kafkaTemplate.send(FIRST_TOPIC, "Testing topic 1");
		assertThat(topicContainer.getNextDestinationTopicFor("firstTopicId", FIRST_TOPIC).getDestinationName())
				.isEqualTo("myRetryTopic1-retry");
		assertThat(awaitLatch(latchContainer.countDownLatch1)).isTrue();
		assertThat(awaitLatch(latchContainer.customDltCountdownLatch)).isTrue();
		assertThat(awaitLatch(latchContainer.customErrorHandlerCountdownLatch)).isTrue();
		assertThat(awaitLatch(latchContainer.customMessageConverterCountdownLatch)).isTrue();
		registry.getListenerContainerIds().stream()
				.filter(id -> id.startsWith("first"))
				.forEach(id -> {
					ConcurrentMessageListenerContainer<?, ?> container = (ConcurrentMessageListenerContainer<?, ?>) registry
							.getListenerContainer(id);
					if (id.equals("firstTopicId")) {
						assertThat(container.getConcurrency()).isEqualTo(2);
					}
					else {
						assertThat(container.getConcurrency())
								.describedAs("Expected %s to have concurrency", id)
								.isEqualTo(1);
					}
				});
	}

	@Test
	void shouldRetrySecondTopic() {
		logger.debug("Sending message to topic " + SECOND_TOPIC);
		kafkaTemplate.send(SECOND_TOPIC, "Testing topic 2");
		assertThat(awaitLatch(latchContainer.countDownLatch2)).isTrue();
		assertThat(awaitLatch(latchContainer.customDltCountdownLatch)).isTrue();
	}

	@Test
	void shouldRetryThirdTopicWithTimeout(@Autowired KafkaAdmin admin,
			@Autowired KafkaListenerEndpointRegistry registry) throws Exception {
		logger.debug("Sending message to topic " + THIRD_TOPIC);
		kafkaTemplate.send(THIRD_TOPIC, "Testing topic 3");
		assertThat(awaitLatch(latchContainer.countDownLatch3)).isTrue();
		assertThat(awaitLatch(latchContainer.countDownLatchDltOne)).isTrue();
		Map<String, TopicDescription> topics = admin.describeTopics(THIRD_TOPIC, THIRD_TOPIC + "-dlt", FOURTH_TOPIC);
		assertThat(topics.get(THIRD_TOPIC).partitions()).hasSize(2);
		assertThat(topics.get(THIRD_TOPIC + "-dlt").partitions()).hasSize(3);
		assertThat(topics.get(FOURTH_TOPIC).partitions()).hasSize(2);
		AtomicReference<Method> method = new AtomicReference<>();
		org.springframework.util.ReflectionUtils.doWithMethods(KafkaAdmin.class, m -> {
			m.setAccessible(true);
			method.set(m);
		}, m -> m.getName().equals("newTopics"));
		@SuppressWarnings("unchecked")
		Collection<NewTopic> weededTopics = (Collection<NewTopic>) method.get().invoke(admin);
		AtomicInteger weeded = new AtomicInteger();
		weededTopics.forEach(topic -> {
			if (topic.name().equals(THIRD_TOPIC) || topic.name().equals(FOURTH_TOPIC)) {
				assertThat(topic).isExactlyInstanceOf(NewTopic.class);
				weeded.incrementAndGet();
			}
		});
		assertThat(weeded.get()).isEqualTo(2);
		registry.getListenerContainerIds().stream()
				.filter(id -> id.startsWith("third"))
				.forEach(id -> {
					ConcurrentMessageListenerContainer<?, ?> container =
							(ConcurrentMessageListenerContainer<?, ?>) registry.getListenerContainer(id);
					if (id.equals("thirdTopicId")) {
						assertThat(container.getConcurrency()).isEqualTo(2);
					}
					else {
						assertThat(container.getConcurrency())
								.describedAs("Expected %s to have concurrency", id)
								.isEqualTo(1);
					}
				});
	}

	@Test
	void shouldRetryFourthTopicWithNoDlt() {
		logger.debug("Sending message to topic " + FOURTH_TOPIC);
		kafkaTemplate.send(FOURTH_TOPIC, "Testing topic 4");
		assertThat(awaitLatch(latchContainer.countDownLatch4)).isTrue();
	}

	@Test
	void shouldRetryFifthTopicWithTwoListenersAndManualAssignment(@Autowired FifthTopicListener1 listener1,
			@Autowired FifthTopicListener2 listener2) {

		logger.debug("Sending two messages to topic " + TWO_LISTENERS_TOPIC);
		kafkaTemplate.send(TWO_LISTENERS_TOPIC, 0, "0", "Testing topic 5 - 0");
		kafkaTemplate.send(TWO_LISTENERS_TOPIC, 1, "0", "Testing topic 5 - 1");
		assertThat(awaitLatch(latchContainer.countDownLatch51)).isTrue();
		assertThat(awaitLatch(latchContainer.countDownLatch52)).isTrue();
		assertThat(awaitLatch(latchContainer.countDownLatchDltThree)).isTrue();
		assertThat(awaitLatch(latchContainer.countDownLatchDltFour)).isTrue();
		assertThat(listener1.topics).containsExactly(TWO_LISTENERS_TOPIC, TWO_LISTENERS_TOPIC
				+ "-listener1-0", TWO_LISTENERS_TOPIC + "-listener1-1", TWO_LISTENERS_TOPIC + "-listener1-2",
				TWO_LISTENERS_TOPIC + "-listener1-dlt");
		assertThat(listener2.topics).containsExactly(TWO_LISTENERS_TOPIC, TWO_LISTENERS_TOPIC
				+ "-listener2-0", TWO_LISTENERS_TOPIC + "-listener2-1", TWO_LISTENERS_TOPIC + "-listener2-2",
				TWO_LISTENERS_TOPIC + "-listener2-dlt");
	}

	@Test
	void shouldRetryManualTopicWithDefaultDlt(@Autowired KafkaListenerEndpointRegistry registry,
			@Autowired ConsumerFactory<String, String> cf) {

		logger.debug("Sending message to topic " + MANUAL_TOPIC);
		kafkaTemplate.send(MANUAL_TOPIC, "Testing topic 6");
		assertThat(awaitLatch(latchContainer.countDownLatch6)).isTrue();
		registry.getListenerContainerIds().stream()
				.filter(id -> id.startsWith("manual"))
				.forEach(id -> {
					ConcurrentMessageListenerContainer<?, ?> container =
							(ConcurrentMessageListenerContainer<?, ?>) registry.getListenerContainer(id);
					assertThat(container).extracting("commonErrorHandler")
							.extracting("seekAfterError", InstanceOfAssertFactories.BOOLEAN)
							.isFalse();
				});
		Consumer<String, String> consumer = cf.createConsumer("manual-dlt", "");
		Set<org.apache.kafka.common.TopicPartition> tp =
				Set.of(new org.apache.kafka.common.TopicPartition(MANUAL_TOPIC + "-dlt", 0));
		consumer.assign(tp);
		try {
			await().untilAsserted(() -> {
				OffsetAndMetadata offsetAndMetadata = consumer.committed(tp).get(tp.iterator().next());
				assertThat(offsetAndMetadata).isNotNull();
				assertThat(offsetAndMetadata.offset()).isEqualTo(1L);
			});
		}
		finally {
			consumer.close();
		}
	}

	@Test
	void shouldFirstReuseRetryTopic(@Autowired FirstReuseRetryTopicListener listener1,
			@Autowired SecondReuseRetryTopicListener listener2, @Autowired ThirdReuseRetryTopicListener listener3) {

		logger.debug("Sending message to topic " + FIRST_REUSE_RETRY_TOPIC);
		kafkaTemplate.send(FIRST_REUSE_RETRY_TOPIC, "Testing reuse topic 1");
		logger.debug("Sending message to topic " + SECOND_REUSE_RETRY_TOPIC);
		kafkaTemplate.send(SECOND_REUSE_RETRY_TOPIC, "Testing reuse topic 2");
		logger.debug("Sending message to topic " + THIRD_REUSE_RETRY_TOPIC);
		kafkaTemplate.send(THIRD_REUSE_RETRY_TOPIC, "Testing reuse topic 3");
		assertThat(awaitLatch(latchContainer.countDownLatchReuseOne)).isTrue();
		assertThat(awaitLatch(latchContainer.countDownLatchReuseTwo)).isTrue();
		assertThat(awaitLatch(latchContainer.countDownLatchReuseThree)).isTrue();
		assertThat(listener1.topics).containsExactly(FIRST_REUSE_RETRY_TOPIC,
				FIRST_REUSE_RETRY_TOPIC + "-retry");
		assertThat(listener2.topics).containsExactly(SECOND_REUSE_RETRY_TOPIC,
				SECOND_REUSE_RETRY_TOPIC + "-retry-30", SECOND_REUSE_RETRY_TOPIC + "-retry-60",
				SECOND_REUSE_RETRY_TOPIC + "-retry-100", SECOND_REUSE_RETRY_TOPIC + "-retry-100");
		assertThat(listener3.topics).containsExactly(THIRD_REUSE_RETRY_TOPIC,
				THIRD_REUSE_RETRY_TOPIC + "-retry", THIRD_REUSE_RETRY_TOPIC + "-retry",
				THIRD_REUSE_RETRY_TOPIC + "-retry", THIRD_REUSE_RETRY_TOPIC + "-retry");
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

	static class FirstTopicListener {

		@Autowired
		DestinationTopicContainer topicContainer;

		@Autowired
		CountDownLatchContainer container;

		@KafkaListener(id = "firstTopicId", topics = FIRST_TOPIC, containerFactory = MAIN_TOPIC_CONTAINER_FACTORY,
				errorHandler = "myCustomErrorHandler", contentTypeConverter = "myCustomMessageConverter",
				concurrency = "2")
		public void listen(String message, @Header(KafkaHeaders.RECEIVED_TOPIC) String receivedTopic) {
			logger.debug("Message {} received in topic {}", message, receivedTopic);
			container.countDownLatch1.countDown();
			throw new RuntimeException("Woooops... in topic " + receivedTopic);
		}

	}

	static class SecondTopicListener {

		@Autowired
		CountDownLatchContainer container;

		@KafkaListener(topics = SECOND_TOPIC, containerFactory = MAIN_TOPIC_CONTAINER_FACTORY)
		public void listenAgain(String message, @Header(KafkaHeaders.RECEIVED_TOPIC) String receivedTopic) {
			logger.debug("Message {} received in topic {} ", message, receivedTopic);
			container.countDownIfNotKnown(receivedTopic, container.countDownLatch2);
			throw new IllegalStateException("Another woooops... " + receivedTopic);
		}
	}

	static class ThirdTopicListener {

		@Autowired
		CountDownLatchContainer container;

		@RetryableTopic(attempts = "${five.attempts}",
				backOff = @BackOff(delay = 250, maxDelay = 1000, multiplier = 1.5),
				numPartitions = "#{3}",
				timeout = "${missing.property:2000}",
				include = MyRetryException.class, kafkaTemplate = "${kafka.template}",
				topicSuffixingStrategy = TopicSuffixingStrategy.SUFFIX_WITH_INDEX_VALUE,
				concurrency = "1")
		@KafkaListener(id = "thirdTopicId", topics = THIRD_TOPIC, containerFactory = MAIN_TOPIC_CONTAINER_FACTORY,
				concurrency = "2")
		public void listenWithAnnotation(String message, @Header(KafkaHeaders.RECEIVED_TOPIC) String receivedTopic) {
			container.countDownIfNotKnown(receivedTopic, container.countDownLatch3);
			logger.debug("========================== Message {} received in annotated topic {} ", message, receivedTopic);
			throw new MyRetryException("Annotated woooops... " + receivedTopic);
		}

		@DltHandler
		public void annotatedDltMethod(Object message) {
			logger.debug("Received message in annotated Dlt method");
			container.countDownLatchDltOne.countDown();
		}
	}

	static class FourthTopicListener {

		@Autowired
		CountDownLatchContainer container;

		@RetryableTopic(dltStrategy = DltStrategy.NO_DLT, attempts = "4", backOff = @BackOff(300),
				sameIntervalTopicReuseStrategy = SameIntervalTopicReuseStrategy.MULTIPLE_TOPICS,
				kafkaTemplate = "${kafka.template}")
		@KafkaListener(topics = FOURTH_TOPIC, containerFactory = MAIN_TOPIC_CONTAINER_FACTORY)
		public void listenNoDlt(String message, @Header(KafkaHeaders.RECEIVED_TOPIC) String receivedTopic) {
			logger.debug("Message {} received in topic {} ", message, receivedTopic);
			container.countDownIfNotKnown(receivedTopic, container.countDownLatch4);
			throw new IllegalStateException("Another woooops... " + receivedTopic);
		}

		@DltHandler
		public void shouldNotGetHere() {
			fail("Dlt should not be processed!");
		}
	}

	static class FifthTopicListener1 {

		final List<String> topics = Collections.synchronizedList(new ArrayList<>());

		@Autowired
		CountDownLatchContainer container;

		@RetryableTopic(attempts = "4",
				backOff = @BackOff(250),
				numPartitions = "2",
				retryTopicSuffix = "-listener1", dltTopicSuffix = "-listener1-dlt",
				topicSuffixingStrategy = TopicSuffixingStrategy.SUFFIX_WITH_INDEX_VALUE,
				sameIntervalTopicReuseStrategy = SameIntervalTopicReuseStrategy.MULTIPLE_TOPICS,
				kafkaTemplate = "${kafka.template}")
		@KafkaListener(id = "fifthTopicId1", topicPartitions = {@TopicPartition(topic = TWO_LISTENERS_TOPIC,
				partitionOffsets = @PartitionOffset(partition = "0", initialOffset = "0"))},
				containerFactory = MAIN_TOPIC_CONTAINER_FACTORY)
		public void listenWithAnnotation(String message, @Header(KafkaHeaders.RECEIVED_TOPIC) String receivedTopic) {
			this.topics.add(receivedTopic);
			container.countDownIfNotKnown(receivedTopic, container.countDownLatch51);
			logger.debug("Message {} received in annotated topic {} ", message, receivedTopic);
			throw new RuntimeException("Annotated woooops... " + receivedTopic);
		}

		@DltHandler
		public void annotatedDltMethod(ConsumerRecord<?, ?> record) {
			logger.debug("Received message in annotated Dlt method");
			this.topics.add(record.topic());
			container.countDownLatchDltThree.countDown();
		}

	}

	static class FifthTopicListener2 {

		final List<String> topics = Collections.synchronizedList(new ArrayList<>());

		@Autowired
		CountDownLatchContainer container;

		@RetryableTopic(attempts = "4",
				backOff = @BackOff(250),
				numPartitions = "2",
				retryTopicSuffix = "-listener2", dltTopicSuffix = "-listener2-dlt",
				topicSuffixingStrategy = TopicSuffixingStrategy.SUFFIX_WITH_INDEX_VALUE,
				sameIntervalTopicReuseStrategy = SameIntervalTopicReuseStrategy.MULTIPLE_TOPICS,
				kafkaTemplate = "${kafka.template}")
		@KafkaListener(id = "fifthTopicId2", topicPartitions = {@TopicPartition(topic = TWO_LISTENERS_TOPIC,
				partitionOffsets = @PartitionOffset(partition = "1", initialOffset = "0"))},
				containerFactory = MAIN_TOPIC_CONTAINER_FACTORY)
		public void listenWithAnnotation2(String message, @Header(KafkaHeaders.RECEIVED_TOPIC) String receivedTopic) {
			this.topics.add(receivedTopic);
			container.countDownLatch52.countDown();
			logger.debug("Message {} received in annotated topic {} ", message, receivedTopic);
			throw new RuntimeException("Annotated woooops... " + receivedTopic);
		}

		@DltHandler
		public void annotatedDltMethod(ConsumerRecord<?, ?> record) {
			logger.debug("Received message in annotated Dlt method");
			this.topics.add(record.topic());
			container.countDownLatchDltFour.countDown();
		}

	}

	static class SixthTopicDefaultDLTListener {

		@Autowired
		CountDownLatchContainer container;

		@RetryableTopic(attempts = "4", backOff = @BackOff(50),
				sameIntervalTopicReuseStrategy = SameIntervalTopicReuseStrategy.MULTIPLE_TOPICS)
		@KafkaListener(id = "manual", topics = MANUAL_TOPIC, containerFactory = MAIN_TOPIC_CONTAINER_FACTORY)
		public void listenNoDlt(String message, @Header(KafkaHeaders.RECEIVED_TOPIC) String receivedTopic,
				@SuppressWarnings("unused") Acknowledgment ack) {

			logger.debug("Message {} received in topic {} ", message, receivedTopic);
			container.countDownIfNotKnown(receivedTopic, container.countDownLatch6);
			throw new IllegalStateException("Another woooops... " + receivedTopic);
		}

	}

	static class NoRetryTopicListener {

		@Autowired
		CountDownLatchContainer container;

		@RetryableTopic(attempts = "3", numPartitions = "3", exclude = MyDontRetryException.class,
				backOff = @BackOff(delay = 50, maxDelay = 100, multiplier = 3),
				traversingCauses = "true", kafkaTemplate = "${kafka.template}")
		@KafkaListener(topics = NOT_RETRYABLE_EXCEPTION_TOPIC, containerFactory = MAIN_TOPIC_CONTAINER_FACTORY)
		public void listenWithAnnotation2(String message, @Header(KafkaHeaders.RECEIVED_TOPIC) String receivedTopic) {
			container.countDownIfNotKnown(receivedTopic, container.countDownLatchNoRetry);
			logger.info("Message {} received in second annotated topic {} ", message, receivedTopic);
			throw new MyDontRetryException("Annotated second woooops... " + receivedTopic);
		}

		@DltHandler
		public void annotatedDltMethod(Object message) {
			logger.info("Received message in annotated Dlt method!");
			container.countDownLatchDltTwo.countDown();
		}
	}

	static class FirstReuseRetryTopicListener {

		final List<String> topics = Collections.synchronizedList(new ArrayList<>());

		@Autowired
		CountDownLatchContainer container;

		@RetryableTopic(attempts = "2", backOff = @BackOff(50))
		@KafkaListener(id = "reuseRetry1", topics = FIRST_REUSE_RETRY_TOPIC,
				containerFactory = "retryTopicListenerContainerFactory")
		public void listen1(String message, @Header(KafkaHeaders.RECEIVED_TOPIC) String receivedTopic) {
			logger.debug("Message {} received in topic {} ", message, receivedTopic);
			this.topics.add(receivedTopic);
			container.countDownLatchReuseOne.countDown();
			throw new RuntimeException("Another woooops... " + receivedTopic);
		}

	}

	static class SecondReuseRetryTopicListener {

		final List<String> topics = Collections.synchronizedList(new ArrayList<>());

		@Autowired
		CountDownLatchContainer container;

		@RetryableTopic(attempts = "5", backOff = @BackOff(delay = 30, maxDelay = 100, multiplier = 2))
		@KafkaListener(id = "reuseRetry2", topics = SECOND_REUSE_RETRY_TOPIC,
				containerFactory = "retryTopicListenerContainerFactory")
		public void listen2(String message, @Header(KafkaHeaders.RECEIVED_TOPIC) String receivedTopic) {
			logger.debug("Message {} received in topic {} ", message, receivedTopic);
			this.topics.add(receivedTopic);
			container.countDownLatchReuseTwo.countDown();
			throw new RuntimeException("Another woooops... " + receivedTopic);
		}

	}

	static class ThirdReuseRetryTopicListener {

		final List<String> topics = Collections.synchronizedList(new ArrayList<>());

		@Autowired
		CountDownLatchContainer container;

		@RetryableTopic(attempts = "5", backOff = @BackOff(delay = 1, maxDelay = 5, multiplier = 1.4))
		@KafkaListener(id = "reuseRetry3", topics = THIRD_REUSE_RETRY_TOPIC,
				containerFactory = "retryTopicListenerContainerFactory")
		public void listen3(String message, @Header(KafkaHeaders.RECEIVED_TOPIC) String receivedTopic) {
			logger.debug("Message {} received in topic {} ", message, receivedTopic);
			this.topics.add(receivedTopic);
			container.countDownLatchReuseThree.countDown();
			throw new RuntimeException("Another woooops... " + receivedTopic);
		}

	}

	static class CountDownLatchContainer {

		CountDownLatch countDownLatch1 = new CountDownLatch(5);

		CountDownLatch countDownLatch2 = new CountDownLatch(3);

		CountDownLatch countDownLatch3 = new CountDownLatch(3);

		CountDownLatch countDownLatch4 = new CountDownLatch(4);

		CountDownLatch countDownLatch51 = new CountDownLatch(4);

		CountDownLatch countDownLatch52 = new CountDownLatch(4);

		CountDownLatch countDownLatch6 = new CountDownLatch(4);

		CountDownLatch countDownLatchNoRetry = new CountDownLatch(1);

		CountDownLatch countDownLatchDltOne = new CountDownLatch(1);

		CountDownLatch countDownLatchDltTwo = new CountDownLatch(1);

		CountDownLatch countDownLatchDltThree = new CountDownLatch(1);

		CountDownLatch countDownLatchDltFour = new CountDownLatch(1);

		CountDownLatch countDownLatchReuseOne = new CountDownLatch(2);

		CountDownLatch countDownLatchReuseTwo = new CountDownLatch(5);

		CountDownLatch countDownLatchReuseThree = new CountDownLatch(5);

		CountDownLatch customDltCountdownLatch = new CountDownLatch(1);

		CountDownLatch customErrorHandlerCountdownLatch = new CountDownLatch(6);

		CountDownLatch customMessageConverterCountdownLatch = new CountDownLatch(6);

		final List<String> knownTopics = new ArrayList<>();

		private void countDownIfNotKnown(String receivedTopic, CountDownLatch countDownLatch) {
			synchronized (knownTopics) {
				if (!knownTopics.contains(receivedTopic)) {
					knownTopics.add(receivedTopic);
					countDownLatch.countDown();
				}
			}
		}
	}

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
	static class RetryTopicConfigurations extends RetryTopicConfigurationSupport {

		private static final String DLT_METHOD_NAME = "processDltMessage";

		@SuppressWarnings("deprecation")
		@Bean
		public RetryTopicConfiguration firstRetryTopic(KafkaTemplate<String, String> template) {
			return RetryTopicConfigurationBuilder
					.newInstance()
					.fixedBackOff(50)
					.maxAttempts(5)
					.concurrency(1)
					.useSingleTopicForSameIntervals()
					.includeTopic(FIRST_TOPIC)
					.doNotRetryOnDltFailure()
					.dltHandlerMethod("myCustomDltProcessor", DLT_METHOD_NAME)
					.create(template);
		}

		@Bean
		public RetryTopicConfiguration secondRetryTopic(KafkaTemplate<String, String> template) {
			return RetryTopicConfigurationBuilder
					.newInstance()
					.exponentialBackoff(500, 2, 10000)
					.retryOn(Arrays.asList(IllegalStateException.class, IllegalAccessException.class))
					.traversingCauses()
					.includeTopic(SECOND_TOPIC)
					.doNotRetryOnDltFailure()
					.dltHandlerMethod("myCustomDltProcessor", DLT_METHOD_NAME)
					.create(template);
		}

		@Bean
		public FirstTopicListener firstTopicListener() {
			return new FirstTopicListener();
		}

		@Bean
		public KafkaListenerErrorHandler myCustomErrorHandler(CountDownLatchContainer container) {
			return (message, exception) -> {
				container.customErrorHandlerCountdownLatch.countDown();
				throw exception;
			};
		}

		@Bean
		public SmartMessageConverter myCustomMessageConverter(CountDownLatchContainer container) {
			return new CompositeMessageConverter(Collections.singletonList(new GenericMessageConverter())) {

				@Override
				public Object fromMessage(Message<?> message, Class<?> targetClass, Object conversionHint) {
					container.customMessageConverterCountdownLatch.countDown();
					return super.fromMessage(message, targetClass, conversionHint);
				}
			};
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
		public FifthTopicListener1 fifthTopicListener1() {
			return new FifthTopicListener1();
		}

		@Bean
		public FifthTopicListener2 fifthTopicListener2() {
			return new FifthTopicListener2();
		}

		@Bean
		SixthTopicDefaultDLTListener manualTopicListener() {
			return new SixthTopicDefaultDLTListener();
		}

		@Bean
		public NoRetryTopicListener noRetryTopicListener() {
			return new NoRetryTopicListener();
		}

		@Bean
		public FirstReuseRetryTopicListener firstReuseRetryTopicListener() {
			return new FirstReuseRetryTopicListener();
		}

		@Bean
		public SecondReuseRetryTopicListener secondReuseRetryTopicListener() {
			return new SecondReuseRetryTopicListener();
		}

		@Bean
		public ThirdReuseRetryTopicListener thirdReuseRetryTopicListener() {
			return new ThirdReuseRetryTopicListener();
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
	public static class KafkaProducerConfig {

		@Autowired
		EmbeddedKafkaBroker broker;

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

		@Bean("customKafkaTemplate")
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
		public NewTopic topic() {
			return TopicBuilder.name(THIRD_TOPIC).partitions(2).replicas(1).build();
		}

		@Bean
		public NewTopics topics() {
			return new NewTopics(TopicBuilder.name(FOURTH_TOPIC).partitions(2).replicas(1).build());
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
