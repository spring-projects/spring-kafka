package org.springframework.kafka.retrytopic;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.DltHandler;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.annotation.KafkaHandler;
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
import org.springframework.kafka.listener.AsyncRetryableException;
import org.springframework.kafka.listener.ConcurrentMessageListenerContainer;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.kafka.listener.ContainerProperties.AckMode;
import org.springframework.kafka.listener.KafkaListenerErrorHandler;

import org.springframework.kafka.retrytopic.RetryTopicClassLevelIntegrationTests.AbstractFifthTopicListener;
import org.springframework.kafka.retrytopic.RetryTopicClassLevelIntegrationTests.CountDownLatchContainer;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.messaging.Message;
import org.springframework.messaging.converter.CompositeMessageConverter;
import org.springframework.messaging.converter.GenericMessageConverter;
import org.springframework.messaging.converter.SmartMessageConverter;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.retry.annotation.Backoff;
import org.springframework.scheduling.TaskScheduler;
import org.springframework.scheduling.concurrent.ThreadPoolTaskScheduler;
import org.springframework.stereotype.Component;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.context.junit.jupiter.SpringJUnitConfig;
import org.springframework.util.ReflectionUtils;

import reactor.core.publisher.Mono;

@SpringJUnitConfig
@DirtiesContext
@EmbeddedKafka(topics = { RetryTopicClassLevelIntegrationTests.FIRST_TOPIC,
						  RetryTopicClassLevelIntegrationTests.SECOND_TOPIC,
						  RetryTopicClassLevelIntegrationTests.THIRD_TOPIC,
						  RetryTopicClassLevelIntegrationTests.FOURTH_TOPIC,
						  RetryTopicClassLevelIntegrationTests.TWO_LISTENERS_TOPIC,
						  RetryTopicClassLevelIntegrationTests.MANUAL_TOPIC })
@TestPropertySource(properties = { "five.attempts=5", "kafka.template=customKafkaTemplate"})
public class AsyncRetryTopicClassLevelIntegrationTests {

	public final static String FIRST_FUTURE_TOPIC = "myRetryFutureTopic1";

	public final static String FIRST_FUTURE_LISTENER_ID = "firstFutureTopicId";

	public final static String FIRST_MONO_TOPIC = "myRetryMonoTopic1";

	public final static String FIRST_MONO_LISTENER_ID = "firstMonoTopicId";

	public final static String SECOND_FUTURE_TOPIC = "myRetryFutureTopic2";

	public final static String SECOND_MONO_TOPIC = "myRetryMonoTopic2";

	public final static String THIRD_FUTURE_TOPIC = "myRetryFutureTopic3";

	public final static String THIRD_FUTURE_LISTENER_ID = "thirdFutureTopicId";

	public final static String THIRD_MONO_TOPIC = "myRetryMonoTopic3";

	public final static String THIRD_MONO_LISTENER_ID = "thirdMonoTopicId";

	public final static String FOURTH_FUTURE_TOPIC = "myRetryFutureTopic4";

	public final static String FOURTH_MONO_TOPIC = "myRetryMonoTopic4";

	public final static String TWO_LISTENERS_FUTURE_TOPIC = "myRetryFutureTopic5";

	public final static String TWO_LISTENERS_MONO_TOPIC = "myRetryMonoTopic5";

	private final static String MAIN_TOPIC_CONTAINER_FACTORY = "kafkaListenerContainerFactory";


	public final static String NOT_RETRYABLE_EXCEPTION_FUTURE_TOPIC = "noRetryFutureTopic";

	public final static String NOT_RETRYABLE_EXCEPTION_MONO_TOPIC = "noRetryMonoTopic";

	@Autowired
	private KafkaTemplate<String, String> kafkaTemplate;

	@Autowired
	private CountDownLatchContainer latchContainer;

	@Autowired
	DestinationTopicContainer topicContainer;

	@Test
	void shouldRetryFirstFutureTopic(@Autowired KafkaListenerEndpointRegistry registry) {

		kafkaTemplate.send(FIRST_FUTURE_TOPIC, "Testing topic 1");
		assertThat(topicContainer.getNextDestinationTopicFor(FIRST_FUTURE_LISTENER_ID, FIRST_FUTURE_TOPIC).getDestinationName())
				.isEqualTo(FIRST_FUTURE_TOPIC + "-retry");
		assertThat(awaitLatch(latchContainer.futureCountDownLatch1)).isTrue();
		assertThat(awaitLatch(latchContainer.customDltCountdownLatch)).isTrue();
		assertThat(awaitLatch(latchContainer.customMessageConverterCountdownLatch)).isTrue();
		assertThat(awaitLatch(latchContainer.customErrorHandlerCountdownLatch)).isTrue();
		registry.getListenerContainerIds().stream()
				.filter(id -> id.startsWith(FIRST_FUTURE_LISTENER_ID))
				.forEach(id -> {
					ConcurrentMessageListenerContainer<?, ?> container
							= (ConcurrentMessageListenerContainer<?, ?>) registry.getListenerContainer(id);
					if (id.equals(FIRST_FUTURE_LISTENER_ID)) {
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
	void shouldRetryFirstMonoTopic(@Autowired KafkaListenerEndpointRegistry registry) {

		kafkaTemplate.send(FIRST_MONO_TOPIC, "Testing topic Mono 1");
		assertThat(topicContainer.getNextDestinationTopicFor(FIRST_MONO_LISTENER_ID, FIRST_MONO_TOPIC).getDestinationName())
				.isEqualTo(FIRST_MONO_TOPIC + "-retry");
		assertThat(awaitLatch(latchContainer.monoCountDownLatch1)).isTrue();
		assertThat(awaitLatch(latchContainer.customDltCountdownLatch)).isTrue();
		assertThat(awaitLatch(latchContainer.customMessageConverterCountdownLatch)).isTrue();
		assertThat(awaitLatch(latchContainer.customErrorHandlerCountdownLatch)).isTrue();
		registry.getListenerContainerIds().stream()
				.filter(id -> id.startsWith(FIRST_MONO_LISTENER_ID))
				.forEach(id -> {
					ConcurrentMessageListenerContainer<?, ?> container
							= (ConcurrentMessageListenerContainer<?, ?>) registry.getListenerContainer(id);
					if (id.equals(FIRST_MONO_LISTENER_ID)) {
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
	void shouldRetrySecondFutureTopic() {
		kafkaTemplate.send(SECOND_FUTURE_TOPIC, "Testing topic 2");
		assertThat(awaitLatch(latchContainer.futureCountDownLatch2)).isTrue();
		assertThat(awaitLatch(latchContainer.customDltCountdownLatch)).isTrue();
	}

	@Test
	void shouldRetrySecondMonoTopic() {
		kafkaTemplate.send(SECOND_MONO_TOPIC, "Testing topic 2");
		assertThat(awaitLatch(latchContainer.monoCountDownLatch2)).isTrue();
		assertThat(awaitLatch(latchContainer.customDltCountdownLatch)).isTrue();
	}


	@Test
	void shouldRetryThirdFutureTopicWithTimeout(@Autowired KafkaAdmin admin,
										  @Autowired KafkaListenerEndpointRegistry registry) throws Exception {
		kafkaTemplate.send(THIRD_FUTURE_TOPIC, "Testing topic 3");
		assertThat(awaitLatch(latchContainer.futureCountDownLatch3)).isTrue();
		assertThat(awaitLatch(latchContainer.countDownLatchDltOne)).isTrue();
		Map<String, TopicDescription> topics = admin.describeTopics(THIRD_FUTURE_TOPIC, THIRD_FUTURE_TOPIC + "-dlt", FOURTH_FUTURE_TOPIC);
		assertThat(topics.get(THIRD_FUTURE_TOPIC).partitions()).hasSize(2);
		assertThat(topics.get(THIRD_FUTURE_TOPIC + "-dlt").partitions()).hasSize(3);
		assertThat(topics.get(FOURTH_FUTURE_TOPIC).partitions()).hasSize(2);
		AtomicReference<Method> method = new AtomicReference<>();
		ReflectionUtils.doWithMethods(KafkaAdmin.class, m -> {
			m.setAccessible(true);
			method.set(m);
		}, m -> m.getName().equals("newTopics"));
		@SuppressWarnings("unchecked")
		Collection<NewTopic> weededTopics = (Collection<NewTopic>) method.get().invoke(admin);
		AtomicInteger weeded = new AtomicInteger();
		weededTopics.forEach(topic -> {
			if (topic.name().equals(THIRD_FUTURE_TOPIC) || topic.name().equals(FOURTH_FUTURE_TOPIC)) {
				assertThat(topic).isExactlyInstanceOf(NewTopic.class);
				weeded.incrementAndGet();
			}
		});
		assertThat(weeded.get()).isEqualTo(2);
		registry.getListenerContainerIds().stream()
				.filter(id -> id.startsWith(THIRD_FUTURE_LISTENER_ID))
				.forEach(id -> {
					ConcurrentMessageListenerContainer<?, ?> container =
							(ConcurrentMessageListenerContainer<?, ?>) registry.getListenerContainer(id);
					if (id.equals(THIRD_FUTURE_LISTENER_ID)) {
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
	void shouldRetryThirdMonoTopicWithTimeout(@Autowired KafkaAdmin admin,
												@Autowired KafkaListenerEndpointRegistry registry) throws Exception {

		kafkaTemplate.send(THIRD_MONO_TOPIC, "Testing topic 3");
		assertThat(awaitLatch(latchContainer.monoCountDownLatch3)).isTrue();
		assertThat(awaitLatch(latchContainer.countDownLatchDltOne)).isTrue();
		Map<String, TopicDescription> topics = admin.describeTopics(THIRD_MONO_TOPIC, THIRD_MONO_TOPIC + "-dlt", FOURTH_MONO_TOPIC);
		assertThat(topics.get(THIRD_MONO_TOPIC).partitions()).hasSize(2);
		assertThat(topics.get(THIRD_MONO_TOPIC + "-dlt").partitions()).hasSize(3);
		assertThat(topics.get(FOURTH_MONO_TOPIC).partitions()).hasSize(2);
		AtomicReference<Method> method = new AtomicReference<>();
		ReflectionUtils.doWithMethods(KafkaAdmin.class, m -> {
			m.setAccessible(true);
			method.set(m);
		}, m -> m.getName().equals("newTopics"));
		@SuppressWarnings("unchecked")
		Collection<NewTopic> weededTopics = (Collection<NewTopic>) method.get().invoke(admin);
		AtomicInteger weeded = new AtomicInteger();
		weededTopics.forEach(topic -> {
			if (topic.name().equals(THIRD_MONO_TOPIC) || topic.name().equals(FOURTH_MONO_TOPIC)) {
				assertThat(topic).isExactlyInstanceOf(NewTopic.class);
				weeded.incrementAndGet();
			}
		});
		assertThat(weeded.get()).isEqualTo(2);
		registry.getListenerContainerIds().stream()
				.filter(id -> id.startsWith(THIRD_MONO_LISTENER_ID))
				.forEach(id -> {
					ConcurrentMessageListenerContainer<?, ?> container =
							(ConcurrentMessageListenerContainer<?, ?>) registry.getListenerContainer(id);
					if (id.equals(THIRD_MONO_LISTENER_ID)) {
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
	void shouldRetryFourthFutureTopicWithNoDlt() {
		kafkaTemplate.send(FOURTH_FUTURE_TOPIC, "Testing topic 4");
		assertThat(awaitLatch(latchContainer.futureCountDownLatch4)).isTrue();
	}

	@Test
	void shouldRetryFourthMonoTopicWithNoDlt() {
		kafkaTemplate.send(FOURTH_MONO_TOPIC, "Testing topic 4");
		assertThat(awaitLatch(latchContainer.monoCountDownLatch4)).isTrue();
	}



	@Test
	void shouldRetryFifthTopicWithTwoListenersAndManualAssignment1(@Autowired FifthFutureTopicListener1 listener1,
																  @Autowired FifthFutureTopicListener2 listener2) {

		kafkaTemplate.send(TWO_LISTENERS_FUTURE_TOPIC, 0, "0", "Testing topic 5 - 0");
		kafkaTemplate.send(TWO_LISTENERS_FUTURE_TOPIC, 1, "0", "Testing topic 5 - 1");

		assertThat(awaitLatch(latchContainer.countDownLatchDltThree)).isTrue();
		assertThat(awaitLatch(latchContainer.countDownLatch51)).isTrue();
		assertThat(awaitLatch(latchContainer.countDownLatch52)).isTrue();
		assertThat(listener1.topics).containsExactly(TWO_LISTENERS_FUTURE_TOPIC, TWO_LISTENERS_FUTURE_TOPIC
																				 + "-listener1-0", TWO_LISTENERS_FUTURE_TOPIC + "-listener1-1", TWO_LISTENERS_FUTURE_TOPIC + "-listener1-2",
													 TWO_LISTENERS_FUTURE_TOPIC + "-listener1-dlt");
		assertThat(listener2.topics).containsExactly(TWO_LISTENERS_FUTURE_TOPIC, TWO_LISTENERS_FUTURE_TOPIC
																				 + "-listener2-0", TWO_LISTENERS_FUTURE_TOPIC + "-listener2-1", TWO_LISTENERS_FUTURE_TOPIC + "-listener2-2",
													 TWO_LISTENERS_FUTURE_TOPIC + "-listener2-dlt");
	}



	@Test
	void shouldGoStraightToDltInFuture() {
		kafkaTemplate.send(NOT_RETRYABLE_EXCEPTION_FUTURE_TOPIC, "Testing topic with annotation 1");
		assertThat(awaitLatch(latchContainer.futureCountDownLatchNoRetry)).isTrue();
		assertThat(awaitLatch(latchContainer.countDownLatchDltTwo)).isTrue();
	}

	@Test
	void shouldGoStraightToDltInMono() {
		kafkaTemplate.send(NOT_RETRYABLE_EXCEPTION_MONO_TOPIC, "Testing topic with annotation 1");
		assertThat(awaitLatch(latchContainer.monoCountDownLatchNoRetry)).isTrue();
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

	@KafkaListener(
			id = FIRST_FUTURE_LISTENER_ID,
			topics = FIRST_FUTURE_TOPIC,
			containerFactory = MAIN_TOPIC_CONTAINER_FACTORY,
			errorHandler = "myCustomErrorHandler",
			contentTypeConverter = "myCustomMessageConverter",
			concurrency = "2")
	static class FirstFutureTopicListener {

		@Autowired
		DestinationTopicContainer topicContainer;

		@Autowired
		CountDownLatchContainer container;


		@KafkaHandler
		public CompletableFuture<Void> listen(String message, @Header(KafkaHeaders.RECEIVED_TOPIC) String receivedTopic) {
			return CompletableFuture.supplyAsync(() -> {
				container.futureCountDownLatch1.countDown();
				try {
					Thread.sleep(1);
				} catch (InterruptedException e) {
					throw new RuntimeException(e);
				}
				throw new RuntimeException("Woooops... in topic " + receivedTopic);
			});
		}
	}


	@KafkaListener(
			id = FIRST_MONO_LISTENER_ID,
			topics = FIRST_MONO_TOPIC,
			containerFactory = MAIN_TOPIC_CONTAINER_FACTORY,
			errorHandler = "myCustomErrorHandler",
			contentTypeConverter = "myCustomMessageConverter",
			concurrency = "2")
	static class FirstTopicMonoListener {

		@Autowired
		DestinationTopicContainer topicContainer;

		@Autowired
		CountDownLatchContainer container;

		@KafkaHandler
		public Mono<Void> listen(String message, @Header(KafkaHeaders.RECEIVED_TOPIC) String receivedTopic) {
			return Mono.fromCallable(() -> {
				container.monoCountDownLatch1.countDown();
				try {
					Thread.sleep(1);
				} catch (InterruptedException e) {
					throw new RuntimeException(e);
				}
				throw new RuntimeException("Woooops... in topic " + receivedTopic);
			}).then();
		}
	}


	@KafkaListener(topics = SECOND_FUTURE_TOPIC, containerFactory = MAIN_TOPIC_CONTAINER_FACTORY)
	static class SecondFutureTopicListener {

		@Autowired
		CountDownLatchContainer container;

		@KafkaHandler
		public CompletableFuture<Void> listenAgain(String message, @Header(KafkaHeaders.RECEIVED_TOPIC) String receivedTopic) {
			return CompletableFuture.supplyAsync(() -> {
				container.countDownIfNotKnown(receivedTopic, container.futureCountDownLatch2);
				try {
					Thread.sleep(1);
				} catch (InterruptedException e) {
					throw new RuntimeException(e);
				}
				throw new IllegalStateException("Another woooops... " + receivedTopic);
			});
		}
	}


	@KafkaListener(topics = SECOND_MONO_TOPIC, containerFactory = MAIN_TOPIC_CONTAINER_FACTORY)
	static class SecondMonoTopicListener {

		@Autowired
		CountDownLatchContainer container;

		@KafkaHandler
		public Mono<Void> listenAgain(String message, @Header(KafkaHeaders.RECEIVED_TOPIC) String receivedTopic) {
			return Mono.fromCallable(() -> {
				container.countDownIfNotKnown(receivedTopic, container.monoCountDownLatch2);
				try {
					Thread.sleep(1);
				} catch (InterruptedException e) {
					throw new RuntimeException(e);
				}
				throw new IllegalStateException("Another woooops... " + receivedTopic);
			}).then();
		}
	}


	@RetryableTopic(
			attempts = "${five.attempts}",
			backoff = @Backoff(delay = 250, maxDelay = 1000, multiplier = 1.5),
			numPartitions = "#{3}",
			timeout = "${missing.property:2000}",
			include = MyRetryException.class, kafkaTemplate = "${kafka.template}",
			topicSuffixingStrategy = TopicSuffixingStrategy.SUFFIX_WITH_INDEX_VALUE,
			concurrency = "1")
	@KafkaListener(
			id = THIRD_FUTURE_LISTENER_ID,
			topics = THIRD_FUTURE_TOPIC,
			containerFactory = MAIN_TOPIC_CONTAINER_FACTORY,
			concurrency = "2")
	static class ThirdFutureTopicListener {

		@Autowired
		CountDownLatchContainer container;

		@KafkaHandler
		public CompletableFuture<Void> listenWithAnnotation(String message, @Header(KafkaHeaders.RECEIVED_TOPIC) String receivedTopic) {
			return CompletableFuture.supplyAsync(() -> {
				container.countDownIfNotKnown(receivedTopic, container.futureCountDownLatch3);
				try {
					Thread.sleep(1);
				} catch (InterruptedException e) {
					throw new RuntimeException(e);
				}
				throw new MyRetryException("Annotated woooops... " + receivedTopic);
			});
		}

		@DltHandler
		public void annotatedDltMethod(Object message) {
			container.countDownLatchDltOne.countDown();
		}
	}

	@Component
	@RetryableTopic(
			attempts = "${five.attempts}",
			backoff = @Backoff(delay = 250, maxDelay = 1000, multiplier = 1.5),
			numPartitions = "#{3}",
			timeout = "${missing.property:2000}",
			include = MyRetryException.class, kafkaTemplate = "${kafka.template}",
			topicSuffixingStrategy = TopicSuffixingStrategy.SUFFIX_WITH_INDEX_VALUE,
			concurrency = "1")
	@KafkaListener(
			id = THIRD_MONO_LISTENER_ID,
			topics = THIRD_MONO_TOPIC,
			containerFactory = MAIN_TOPIC_CONTAINER_FACTORY,
			concurrency = "2")
	static class ThirdMonoTopicListener {

		@Autowired
		CountDownLatchContainer container;

		@KafkaHandler
		public Mono<Void> listenWithAnnotation(String message, @Header(KafkaHeaders.RECEIVED_TOPIC) String receivedTopic) {
			return Mono.fromCallable(() -> {
				container.countDownIfNotKnown(receivedTopic, container.monoCountDownLatch3);
				try {
					Thread.sleep(1);
				} catch (InterruptedException e) {
					throw new RuntimeException(e);
				}
				throw new MyRetryException("Annotated woooops... " + receivedTopic);
			}).then();
		}

		@DltHandler
		public void annotatedDltMethod(Object message) {
			container.countDownLatchDltOne.countDown();
		}
	}



	@RetryableTopic(
			dltStrategy = DltStrategy.NO_DLT,
			attempts = "4",
			backoff = @Backoff(300),
			sameIntervalTopicReuseStrategy = SameIntervalTopicReuseStrategy.MULTIPLE_TOPICS,
			kafkaTemplate = "${kafka.template}")
	@KafkaListener(topics = FOURTH_FUTURE_TOPIC, containerFactory = MAIN_TOPIC_CONTAINER_FACTORY)
	static class FourthFutureTopicListener {

		@Autowired
		CountDownLatchContainer container;

		@KafkaHandler
		public CompletableFuture<Void> listenNoDlt(String message, @Header(KafkaHeaders.RECEIVED_TOPIC) String receivedTopic) {
			return CompletableFuture.supplyAsync(() -> {
				container.countDownIfNotKnown(receivedTopic, container.futureCountDownLatch4);
				try {
					Thread.sleep(1);
				} catch (InterruptedException e) {
					throw new RuntimeException(e);
				}
				throw new IllegalStateException("Another woooops... " + receivedTopic);
			});
		}

		@DltHandler
		public void shouldNotGetHere() {
			fail("Dlt should not be processed!");
		}
	}

	@RetryableTopic(
			dltStrategy = DltStrategy.NO_DLT,
			attempts = "4",
			backoff = @Backoff(300),
			sameIntervalTopicReuseStrategy = SameIntervalTopicReuseStrategy.MULTIPLE_TOPICS,
			kafkaTemplate = "${kafka.template}")
	@KafkaListener(topics = FOURTH_MONO_TOPIC, containerFactory = MAIN_TOPIC_CONTAINER_FACTORY)
	static class FourthMonoTopicListener {

		@Autowired
		CountDownLatchContainer container;

		@KafkaHandler
		public Mono<Void> listenNoDlt(String message, @Header(KafkaHeaders.RECEIVED_TOPIC) String receivedTopic) {
			return Mono.fromCallable(() -> {
				container.countDownIfNotKnown(receivedTopic, container.monoCountDownLatch4);
				try {
					Thread.sleep(1);
				} catch (InterruptedException e) {
					throw new RuntimeException(e);
				}
				throw new IllegalStateException("Another woooops... " + receivedTopic);
			}).then();
		}

		@DltHandler
		public void shouldNotGetHere() {
			fail("Dlt should not be processed!");
		}
	}


	static class AbstractFifthTopicListener {

		final List<String> topics = Collections.synchronizedList(new ArrayList<>());

		@Autowired
		CountDownLatchContainer container;

		@DltHandler
		public void annotatedDltMethod(ConsumerRecord<?, ?> record) {
			this.topics.add(record.topic());
			container.countDownLatchDltThree.countDown();
		}

	}

	@RetryableTopic(
			attempts = "4",
			backoff = @Backoff(1),
			numPartitions = "2",
			retryTopicSuffix = "-listener1",
			dltTopicSuffix = "-listener1-dlt",
			topicSuffixingStrategy = TopicSuffixingStrategy.SUFFIX_WITH_INDEX_VALUE,
			sameIntervalTopicReuseStrategy = SameIntervalTopicReuseStrategy.MULTIPLE_TOPICS,
			kafkaTemplate = "${kafka.template}")
	@KafkaListener(
			id = "fifthTopicId1",
			topicPartitions = {@TopicPartition(topic = TWO_LISTENERS_FUTURE_TOPIC,
			partitionOffsets = @PartitionOffset(partition = "0", initialOffset = "0"))},
			containerFactory = MAIN_TOPIC_CONTAINER_FACTORY)
	static class FifthFutureTopicListener1 extends AbstractFifthTopicListener {

		@KafkaHandler
		public CompletableFuture<Void> listenWithAnnotation(String message, @Header(KafkaHeaders.RECEIVED_TOPIC) String receivedTopic) {
			this.topics.add(receivedTopic);
			container.countDownIfNotKnown(receivedTopic, container.countDownLatch51);
			return CompletableFuture.supplyAsync(() -> {

				try {
					Thread.sleep(1);
				} catch (InterruptedException e) {
					throw new RuntimeException(e);
				}
				throw new RuntimeException("Annotated woooops... " + receivedTopic);
			});
		}
	}

	@RetryableTopic(
			attempts = "4",
			backoff = @Backoff(1),
			numPartitions = "2",
			retryTopicSuffix = "-listener2",
			dltTopicSuffix = "-listener2-dlt",
			topicSuffixingStrategy = TopicSuffixingStrategy.SUFFIX_WITH_INDEX_VALUE,
			sameIntervalTopicReuseStrategy = SameIntervalTopicReuseStrategy.MULTIPLE_TOPICS,
			kafkaTemplate = "${kafka.template}")
	@KafkaListener(
			id = "fifthTopicId2",
			topicPartitions = {@TopicPartition(topic = TWO_LISTENERS_FUTURE_TOPIC,
			partitionOffsets = @PartitionOffset(partition = "1", initialOffset = "0"))},
			containerFactory = MAIN_TOPIC_CONTAINER_FACTORY)
	static class FifthFutureTopicListener2 extends AbstractFifthTopicListener {

		@KafkaHandler
		public CompletableFuture<Void> listenWithAnnotation2(String message, @Header(KafkaHeaders.RECEIVED_TOPIC) String receivedTopic) {
			this.topics.add(receivedTopic);
			container.countDownIfNotKnown(receivedTopic, container.countDownLatch52);
			return CompletableFuture.supplyAsync(() -> {

				try {
					Thread.sleep(1);
				} catch (InterruptedException e) {
					throw new RuntimeException(e);
				}
				throw new RuntimeException("Annotated woooops... " + receivedTopic);
			});
		}

	}


	@Component
	@RetryableTopic(attempts = "3", numPartitions = "3", exclude = MyDontRetryException.class,
			backoff = @Backoff(delay = 50, maxDelay = 100, multiplier = 3),
			traversingCauses = "true", kafkaTemplate = "${kafka.template}")
	@KafkaListener(topics = NOT_RETRYABLE_EXCEPTION_FUTURE_TOPIC, containerFactory = MAIN_TOPIC_CONTAINER_FACTORY)
	static class NoRetryFutureTopicListener {

		@Autowired
		CountDownLatchContainer container;

		@KafkaHandler
		public CompletableFuture<Void> listenWithAnnotation2(String message, @Header(KafkaHeaders.RECEIVED_TOPIC) String receivedTopic) {
			return CompletableFuture.supplyAsync(() -> {
				container.countDownIfNotKnown(receivedTopic, container.futureCountDownLatchNoRetry);
				try {
					Thread.sleep(1);
				} catch (InterruptedException e) {
					throw new RuntimeException(e);
				}
				throw new MyDontRetryException("Annotated second woooops... " + receivedTopic);
			});
		}

		@DltHandler
		public void annotatedDltMethod(Object message) {
			container.countDownLatchDltTwo.countDown();
		}
	}

	@Component
	@RetryableTopic(attempts = "3", numPartitions = "3", exclude = MyDontRetryException.class,
			backoff = @Backoff(delay = 50, maxDelay = 100, multiplier = 3),
			traversingCauses = "true", kafkaTemplate = "${kafka.template}")
	@KafkaListener(topics = NOT_RETRYABLE_EXCEPTION_MONO_TOPIC, containerFactory = MAIN_TOPIC_CONTAINER_FACTORY)
	static class NoRetryMonoTopicListener {

		@Autowired
		CountDownLatchContainer container;

		@KafkaHandler
		public Mono<Void> listenWithAnnotation2(String message, @Header(KafkaHeaders.RECEIVED_TOPIC) String receivedTopic) {
			return Mono.fromCallable(() -> {
				container.countDownIfNotKnown(receivedTopic, container.monoCountDownLatchNoRetry);
				try {
					Thread.sleep(1);
				} catch (InterruptedException e) {
					throw new RuntimeException(e);
				}
				throw new MyDontRetryException("Annotated second woooops... " + receivedTopic);
			}).then();
		}

		@DltHandler
		public void annotatedDltMethod(Object message) {
			container.countDownLatchDltTwo.countDown();
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

		@Bean
		CountDownLatchContainer latchContainer() {
			return new CountDownLatchContainer();
		}
	}

	@Component
	static class CountDownLatchContainer {

		CountDownLatch futureCountDownLatch1 = new CountDownLatch(5);

		CountDownLatch monoCountDownLatch1 = new CountDownLatch(5);

		CountDownLatch futureCountDownLatch2 = new CountDownLatch(3);

		CountDownLatch monoCountDownLatch2 = new CountDownLatch(3);

		CountDownLatch futureCountDownLatch3 = new CountDownLatch(3);

		CountDownLatch monoCountDownLatch3 = new CountDownLatch(3);

		CountDownLatch futureCountDownLatch4 = new CountDownLatch(4);

		CountDownLatch monoCountDownLatch4 = new CountDownLatch(4);

		CountDownLatch countDownLatch51 = new CountDownLatch(4);

		CountDownLatch countDownLatch52 = new CountDownLatch(4);

		CountDownLatch countDownLatch6 = new CountDownLatch(4);

		CountDownLatch futureCountDownLatchNoRetry = new CountDownLatch(1);

		CountDownLatch monoCountDownLatchNoRetry = new CountDownLatch(1);

		CountDownLatch countDownLatchDltOne = new CountDownLatch(1);

		CountDownLatch countDownLatchDltTwo = new CountDownLatch(1);

		CountDownLatch countDownLatchDltThree = new CountDownLatch(2);

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

	@SuppressWarnings("serial")
	static class MyRetryException extends RuntimeException {
		MyRetryException(String msg) {
			super(msg);
		}
	}

	@SuppressWarnings("serial")
	static class MyDontRetryException extends RuntimeException {
		MyDontRetryException(String msg) {
			super(msg);
		}
	}

	@Configuration
	static class RetryTopicConfigurations extends RetryTopicConfigurationSupport {

		private static final String DLT_METHOD_NAME = "processDltMessage";

		@Bean
		FirstFutureTopicListener firstTopicListener() {
			return new FirstFutureTopicListener();
		}

		@Bean
		FirstTopicMonoListener firstTopicMonoListener() {
			return new FirstTopicMonoListener();
		}

		@Bean
		SecondFutureTopicListener secondFutureTopicListener() {
			return new SecondFutureTopicListener();
		}

		@Bean
		SecondMonoTopicListener secondMonoTopicListener() {
			return new SecondMonoTopicListener();
		}

		@Bean
		ThirdFutureTopicListener thirdFutureTopicListener() {
			return new ThirdFutureTopicListener();
		}

		@Bean
		ThirdMonoTopicListener thirdMonoTopicListener() {
			return new ThirdMonoTopicListener();
		}

		@Bean
		FourthFutureTopicListener fourthFutureTopicListener() {
			return new FourthFutureTopicListener();
		}

		@Bean
		FourthMonoTopicListener fourthMonoTopicListener() {
			return new FourthMonoTopicListener();
		}

		@Bean
		NoRetryFutureTopicListener noRetryFutureTopicListener() {
			return new NoRetryFutureTopicListener();
		}

		@Bean
		NoRetryMonoTopicListener noRetryMonoTopicListener() {
			return new NoRetryMonoTopicListener();
		}

		@Bean
		FifthFutureTopicListener1 fifthFutureTopicListener1() {
			return new FifthFutureTopicListener1();
		}

		@Bean
		FifthFutureTopicListener2 fifthFutureTopicListener2() {
			return new FifthFutureTopicListener2();
		}

		@Bean
		MyCustomDltProcessor myCustomDltProcessor() {
			return new MyCustomDltProcessor();
		}

		@Bean
		TaskScheduler taskScheduler() {
			return new ThreadPoolTaskScheduler();
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
		RetryTopicConfiguration firstRetryTopic(KafkaTemplate<String, String> template) {
			return RetryTopicConfigurationBuilder
					.newInstance()
					.fixedBackOff(50)
					.maxAttempts(5)
					.concurrency(1)
					.useSingleTopicForSameIntervals()
					.includeTopic(FIRST_FUTURE_TOPIC)
					.doNotRetryOnDltFailure()
					.dltHandlerMethod("myCustomDltProcessor", DLT_METHOD_NAME)
					.create(template);
		}

		@Bean
		RetryTopicConfiguration firstRetryMonoTopic(KafkaTemplate<String, String> template) {
			return RetryTopicConfigurationBuilder
					.newInstance()
					.fixedBackOff(50)
					.maxAttempts(5)
					.concurrency(1)
					.useSingleTopicForSameIntervals()
					.includeTopic(FIRST_MONO_TOPIC)
					.doNotRetryOnDltFailure()
					.dltHandlerMethod("myCustomDltProcessor", DLT_METHOD_NAME)
					.create(template);
		}

		@Bean
		RetryTopicConfiguration secondRetryFutureTopic(KafkaTemplate<String, String> template) {
			return RetryTopicConfigurationBuilder
					.newInstance()
					.exponentialBackoff(500, 2, 10000)
					.retryOn(Arrays.asList(IllegalStateException.class, IllegalAccessException.class))
					.traversingCauses()
					.includeTopic(SECOND_FUTURE_TOPIC)
					.doNotRetryOnDltFailure()
					.dltHandlerMethod("myCustomDltProcessor", DLT_METHOD_NAME)
					.create(template);
		}

		@Bean
		RetryTopicConfiguration secondRetryMonoTopic(KafkaTemplate<String, String> template) {
			return RetryTopicConfigurationBuilder
					.newInstance()
					.exponentialBackoff(500, 2, 10000)
					.retryOn(Arrays.asList(IllegalStateException.class, IllegalAccessException.class))
					.traversingCauses()
					.includeTopic(SECOND_MONO_TOPIC)
					.doNotRetryOnDltFailure()
					.dltHandlerMethod("myCustomDltProcessor", DLT_METHOD_NAME)
					.create(template);
		}


		@Bean
		NewTopics topics() {

			NewTopic thirdFutureTopic = TopicBuilder.name(THIRD_FUTURE_TOPIC).partitions(2).replicas(1).build();
			NewTopic thirdMonoTopic = TopicBuilder.name(THIRD_MONO_TOPIC).partitions(2).replicas(1).build();
			NewTopic fourthFutureTopic = TopicBuilder.name(FOURTH_FUTURE_TOPIC).partitions(2).replicas(1).build();
			NewTopic fourthMonoTopic = TopicBuilder.name(FOURTH_MONO_TOPIC).partitions(2).replicas(1).build();

			return new NewTopics(
					thirdFutureTopic,
					thirdMonoTopic,
					fourthFutureTopic,
					fourthMonoTopic);
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

//		@Bean
//		NewTopic topic() {
//			return TopicBuilder.name(THIRD_TOPIC).partitions(2).replicas(1).build();
//		}
//
//		@Bean
//		NewTopics topics() {
//			return new NewTopics(TopicBuilder.name(FOURTH_TOPIC).partitions(2).replicas(1).build());
//		}

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

//		@Bean
//		TaskScheduler sched() {
//			return new ThreadPoolTaskScheduler();
//		}

	}

	@Component
	static class MyCustomDltProcessor {

		@Autowired
		KafkaTemplate<String, String> kafkaTemplate;

		@Autowired
		CountDownLatchContainer container;

		public void processDltMessage(Object message) {
			container.customDltCountdownLatch.countDown();
			throw new RuntimeException("Dlt Error!");
		}
	}
}
