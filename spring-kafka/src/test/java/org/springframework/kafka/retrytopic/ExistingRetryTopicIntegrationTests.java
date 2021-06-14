/*
 * Copyright 2021 the original author or authors.
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
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;

import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.task.TaskExecutor;
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
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.retry.annotation.Backoff;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.context.junit.jupiter.SpringJUnitConfig;


/**
 * Tests for https://github.com/spring-projects/spring-kafka/issues/1828
 * @author Deepesh Verma
 * @since 2.7
 */
@SpringJUnitConfig
@DirtiesContext
@EmbeddedKafka(topics = {ExistingRetryTopicIntegrationTests.MAIN_TOPIC_WITH_NO_PARTITION_INFO,
		ExistingRetryTopicIntegrationTests.RETRY_TOPIC_WITH_NO_PARTITION_INFO,
		ExistingRetryTopicIntegrationTests.MAIN_TOPIC_WITH_PARTITION_INFO,
		ExistingRetryTopicIntegrationTests.RETRY_TOPIC_WITH_PARTITION_INFO}, partitions = 4)
@TestPropertySource(properties = "two.attempts=2")
public class ExistingRetryTopicIntegrationTests {

	private static final Logger logger = LoggerFactory.getLogger(ExistingRetryTopicIntegrationTests.class);

	public final static String MAIN_TOPIC_WITH_NO_PARTITION_INFO = "main-topic-1";

	public final static String RETRY_TOPIC_WITH_NO_PARTITION_INFO = "main-topic-1-retry-0";

	public final static String MAIN_TOPIC_WITH_PARTITION_INFO = "main-topic-2";

	public final static String RETRY_TOPIC_WITH_PARTITION_INFO = "main-topic-2-retry-0";

	private final static String MAIN_TOPIC_CONTAINER_FACTORY = "kafkaListenerContainerFactory";

	@Autowired
	private KafkaTemplate<String, String> kafkaTemplate;

	@Autowired
	CountByPartitionContainer countByPartitionContainerWithoutPartition;

	@Autowired
	CountByPartitionContainer countByPartitionContainerWithPartition;

	@Autowired
	private CountDownLatchContainer latchContainer;

	@Test
	@DisplayName("When a @RetryableTopic listener method, with autoCreateTopic=false and NO PARTITION info called, " +
			"should send messages to be retried across partitions for a retry topic")
	void whenNoPartitionInfoProvided_shouldRetryMainTopicCoveringAllPartitionOfRetryTopic() {

		send10MessagesToPartitionWithKey(0, "foo", MAIN_TOPIC_WITH_NO_PARTITION_INFO, this.countByPartitionContainerWithoutPartition);
		send10MessagesToPartitionWithKey(1, "bar", MAIN_TOPIC_WITH_NO_PARTITION_INFO, this.countByPartitionContainerWithoutPartition);
		send10MessagesToPartitionWithKey(2, "buzz", MAIN_TOPIC_WITH_NO_PARTITION_INFO, this.countByPartitionContainerWithoutPartition);
		send10MessagesToPartitionWithKey(3, "fizz", MAIN_TOPIC_WITH_NO_PARTITION_INFO, this.countByPartitionContainerWithoutPartition);

		assertThat(awaitLatch(latchContainer.countDownLatch1)).isTrue();
		assertThat(countByPartitionContainerWithoutPartition.mainTopicMessageCountByPartition)
				.isEqualTo(countByPartitionContainerWithoutPartition.retryTopicMessageCountByPartition);
	}

	@Test
	@DisplayName("When a @RetryableTopic listener method, with autoCreateTopic=false and WITH PARTITION info called, " +
			"should send messages to be retried across partitions for a retry topic")
	void whenPartitionInfoProvided_shouldRetryMainTopicCoveringAllPartitionOfRetryTopic() {

		send10MessagesToPartitionWithKey(0, "foo", MAIN_TOPIC_WITH_PARTITION_INFO, this.countByPartitionContainerWithPartition);
		send10MessagesToPartitionWithKey(1, "bar", MAIN_TOPIC_WITH_PARTITION_INFO, this.countByPartitionContainerWithPartition);
		send10MessagesToPartitionWithKey(2, "buzz", MAIN_TOPIC_WITH_PARTITION_INFO, this.countByPartitionContainerWithPartition);
		send10MessagesToPartitionWithKey(3, "fizz", MAIN_TOPIC_WITH_PARTITION_INFO, this.countByPartitionContainerWithPartition);

		assertThat(awaitLatch(latchContainer.countDownLatch2)).isTrue();
		assertThat(countByPartitionContainerWithPartition.mainTopicMessageCountByPartition)
				.isEqualTo(countByPartitionContainerWithPartition.retryTopicMessageCountByPartition);
	}

	private void send10MessagesToPartitionWithKey(int partition, String messageKey, String mainTopic, CountByPartitionContainer countByPartitionContainer) {
		IntStream.range(0, 10).forEach(messageNumber -> {
			String data = "Test-partition-" + partition + "-messages-" + messageNumber;
			logger.debug("Sending message number:{} to topic {}. Message: {}", messageNumber, mainTopic, data);
			kafkaTemplate.send(mainTopic, partition, messageKey, data);
			countByPartitionContainer.mainTopicMessageCountByPartition.merge(String.valueOf(partition), 1, Integer::sum);
		});
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

	static class MainTopicListener {

		@Autowired
		CountDownLatchContainer container;

		@Autowired
		CountByPartitionContainer countByPartitionContainerWithoutPartition;

		@RetryableTopic(autoCreateTopics = "false", dltStrategy = DltStrategy.NO_DLT,
				attempts = "${two.attempts}", backoff = @Backoff(0), kafkaTemplate = "kafkaTemplate")
		@KafkaListener(id = "firstTopicId", topics = MAIN_TOPIC_WITH_NO_PARTITION_INFO, containerFactory = MAIN_TOPIC_CONTAINER_FACTORY)
		public void listenFirst(String message, @Header(KafkaHeaders.RECEIVED_TOPIC) String receivedTopic,
								@Header(KafkaHeaders.ORIGINAL_PARTITION) String originalPartition,
								@Header(KafkaHeaders.RECEIVED_PARTITION_ID) String receivedPartition) {
			logger.debug("Message {} received in topic {}. originalPartition: {}, receivedPartition: {}",
					message, receivedTopic, originalPartition, receivedPartition);

			if (receivedTopic.contains("-retry")) {
				countByPartitionContainerWithoutPartition.retryTopicMessageCountByPartition.merge(receivedPartition, 1, Integer::sum);
				container.countDownLatch1.countDown();
			}

			throw new RuntimeException("Woooops... in topic " + receivedTopic);
		}

		@Autowired
		CountByPartitionContainer countByPartitionContainerWithPartition;

		@RetryableTopic(autoCreateTopics = "false", numPartitions = "4", dltStrategy = DltStrategy.NO_DLT,
				attempts = "${two.attempts}", backoff = @Backoff(0), kafkaTemplate = "kafkaTemplate")
		@KafkaListener(id = "secondTopicId", topics = MAIN_TOPIC_WITH_PARTITION_INFO, containerFactory = MAIN_TOPIC_CONTAINER_FACTORY)
		public void listenSecond(String message, @Header(KafkaHeaders.RECEIVED_TOPIC) String receivedTopic,
								@Header(KafkaHeaders.ORIGINAL_PARTITION) String originalPartition,
								@Header(KafkaHeaders.RECEIVED_PARTITION_ID) String receivedPartition) {
			logger.debug("Message {} received in topic {}. originalPartition: {}, receivedPartition: {}",
					message, receivedTopic, originalPartition, receivedPartition);

			if (receivedTopic.contains("-retry")) {
				countByPartitionContainerWithPartition.retryTopicMessageCountByPartition.merge(receivedPartition, 1, Integer::sum);
				container.countDownLatch2.countDown();
			}

			throw new RuntimeException("Woooops... in topic " + receivedTopic);
		}
	}

	static class CountDownLatchContainer {

		CountDownLatch countDownLatch1 = new CountDownLatch(40);
		CountDownLatch countDownLatch2 = new CountDownLatch(40);
	}

	static class CountByPartitionContainer {

		Map<String, Integer> mainTopicMessageCountByPartition = new HashMap<>();
		Map<String, Integer> retryTopicMessageCountByPartition = new HashMap<>();
	}

	@Configuration
	static class RetryTopicConfigurations {

		@Bean
		public MainTopicListener mainTopicListener() {
			return new MainTopicListener();
		}

		@Bean
		CountDownLatchContainer latchContainer() {
			return new CountDownLatchContainer();
		}

		@Bean
		CountByPartitionContainer countByPartitionContainerWithoutPartition() {
			return new CountByPartitionContainer();
		}

		@Bean
		CountByPartitionContainer countByPartitionContainerWithPartition() {
			return new CountByPartitionContainer();
		}
	}


	@Configuration
	public static class RuntimeConfig {

		@Bean(name = RetryTopicInternalBeanNames.INTERNAL_BACKOFF_CLOCK_BEAN_NAME)
		public Clock clock() {
			return Clock.systemUTC();
		}

		@Bean
		public TaskExecutor taskExecutor() {
			return new ThreadPoolTaskExecutor();
		}

		@Bean(destroyMethod = "destroy")
		public TaskExecutorManager taskExecutorManager(ThreadPoolTaskExecutor taskExecutor) {
			return new TaskExecutorManager(taskExecutor);
		}
	}

	static class TaskExecutorManager {
		private final ThreadPoolTaskExecutor taskExecutor;

		TaskExecutorManager(ThreadPoolTaskExecutor taskExecutor) {
			this.taskExecutor = taskExecutor;
		}

		void destroy() {
			this.taskExecutor.shutdown();
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
			factory.setConsumerFactory(consumerFactory);
			factory.setConcurrency(1);
			return factory;
		}
	}

}
