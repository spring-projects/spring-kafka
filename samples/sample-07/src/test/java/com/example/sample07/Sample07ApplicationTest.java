/*
 * Copyright 2022-2024 the original author or authors.
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

package com.example.sample07;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.time.Duration;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.BytesDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.Test;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.core.io.ClassPathResource;
import org.springframework.core.io.Resource;
import org.springframework.core.io.support.PropertiesLoaderUtils;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.listener.ConsumerAwareRebalanceListener;
import org.springframework.kafka.listener.MessageListenerContainer;
import org.springframework.kafka.test.utils.KafkaTestUtils;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;
import org.springframework.test.annotation.DirtiesContext;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.InternetProtocol;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

/**
 * New consumer rebalance protocol sample which purpose is only to be used in the test assertions.
 * In this sample, Testcontainers is used for testing instead of @EmbeddedKafka.
 * See unit tests for this project for more information.
 *
 * @author Sanghyeok An.
 *
 * @since 3.3
 */
@Testcontainers
@SpringBootTest
@DirtiesContext
public class Sample07ApplicationTest {

	static final String GROUP_ID = "hello";

	static final String TOPIC_NAME = "hello-topic";

	static final String BROKER = "localhost:12000";

	static final String BROKER_PROPERTIES_FILE_PATH = "src/test/resources/broker.properties";

	static final String KAFKA_IMAGE_NAME = "bitnami/kafka:3.7.0";

	@Autowired
	TestConfig config;

	@Autowired
	KafkaTemplate<Integer, String> template;

	@Autowired
	KafkaListenerEndpointRegistry registry;

	@Autowired
	ThreadPoolTaskExecutor threadPoolTaskExecutor;

	CountDownLatch rawConsumerPollCount;

	CountDownLatch rawConsumerRevokedCount;

	CountDownLatch rawConsumerAssignedCount;

	/*
	Test Scenario
	1. spring-kafka consumer subscribe hello-topic. (1st consumer rebalancing occurs)
	2. rawKafkaConsumer subscribe hello-topic, too. (2nd consumer rebalancing occurs)
	3. rawKafkaConsumer is closed.                  (3rd consumer rebalancing occurs)

	Execute this step and check side effect by ConsumerRebalancing at each step,
	testing the new consumer rebalancing protocol.
	 */
	@Test
	public void newConsumerRebalancingProtocolTest() throws InterruptedException, ExecutionException {

		// One spring-kafka listener subscribe
		final MessageListenerContainer listenerContainer = registry.getListenerContainer(GROUP_ID);
		assertThat(listenerContainer).isNotNull();

		// Send two messages to broker each partition (0, 1)
		this.template.send(TOPIC_NAME, 0, null, "my-data");
		this.template.send(TOPIC_NAME, 1, null, "my-data");

		// spring-kafka listener start to poll from broker.
		listenerContainer.start();

		// spring-kafka should have this experiences.
		// 1. should have assignment of topic partitions.
		// 2. should poll two messages from broker. because, producer send two messages.
		assertThat(config.listenerLatch.await(10, TimeUnit.SECONDS)).isTrue();
		assertThat(config.partitionAssignedLatch.await(10, TimeUnit.SECONDS)).isTrue();


		// Set initial CountDownLatch.
		rawConsumerAssignedCount = new CountDownLatch(1);
		config.partitionRevokedLatch = new CountDownLatch(1);
		config.partitionAssignedLatch = new CountDownLatch(1);
		config.listenerLatch = new CountDownLatch(1);
		rawConsumerPollCount = new CountDownLatch(1);

		// If kafka consumer is created, consumer backed ground thread will send group coordinator heartbeat request to broker.
		final KafkaConsumer<Object, String> rawConsumer = createRawConsumer();

		// When consumer subscribe topic, broker aware it.
		// And consumer will get assignment as response of poll.
		rawConsumer.subscribe(List.of(TOPIC_NAME), new RawConsumerRebalanceListener());
		threadPoolTaskExecutor.execute(() -> {
			while (true) {
				ConsumerRecords<Object, String> poll = rawConsumer.poll(Duration.ofSeconds(1));
				if (!poll.isEmpty()) {
					rawConsumerPollCount.countDown();
					break;
				}
			}
		});

		// spring-kafka consumer will revoke one of partitions.
		// because two topic partitions, two consumers.
		assertThat(rawConsumerAssignedCount.await(20, TimeUnit.SECONDS)).isTrue();
		assertThat(config.partitionRevokedLatch.await(20, TimeUnit.SECONDS)).isTrue();
		assertThat(config.partitionAssignedLatch.await(20, TimeUnit.SECONDS)).isTrue();

		// Send two messages each partition to broker.
		this.template.send(TOPIC_NAME, 0, null, "my-data");
		this.template.send(TOPIC_NAME, 1, null, "my-data");

		// Both spring-kafka consumer and rawConsumer will received message from broker each by each.
		assertThat(config.listenerLatch.await(20, TimeUnit.SECONDS)).isTrue();
		assertThat(rawConsumerPollCount.await(20, TimeUnit.SECONDS)).isTrue();

		// Set initial CountDown latch.
		// Scenario : one consumer closed, the other will have two partitions as result of rebalancing.
		rawConsumerRevokedCount = new CountDownLatch(1);
		config.partitionAssignedLatch = new CountDownLatch(1);

		rawConsumer.close();

		assertThat(rawConsumerRevokedCount.await(10, TimeUnit.SECONDS)).isTrue();
		assertThat(config.partitionAssignedLatch.await(10, TimeUnit.SECONDS)).isTrue();

		config.listenerLatch = new CountDownLatch(2);
		this.template.send(TOPIC_NAME, 0, null, "my-data");
		this.template.send(TOPIC_NAME, 1, null, "my-data");
		assertThat(config.listenerLatch.await(20, TimeUnit.SECONDS)).isTrue();
	}

	KafkaConsumer<Object, String> createRawConsumer() {
		final Map<String, Object> props = getConsumerProperties();
		return new KafkaConsumer<>(props);
	}

	static Map<String, Object> getConsumerProperties() {
		final Map<String, Object> props = new HashMap<>();

		props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, BytesDeserializer.class.getName());
		props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
		props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BROKER);
		props.put(ConsumerConfig.GROUP_ID_CONFIG, GROUP_ID);
		props.put(ConsumerConfig.GROUP_PROTOCOL_CONFIG, "consumer");
		props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
		return props;
	}

	static Map<String, String> getPropertiesFromFile() {
		final Resource resource = new ClassPathResource(BROKER_PROPERTIES_FILE_PATH);
		try {
			final Properties properties = PropertiesLoaderUtils.loadAllProperties(resource.getFilename());
			return properties.stringPropertyNames().stream()
							.collect(Collectors.toMap(
										s -> s,
										s -> (String) properties.get(s)));
		}
		catch (IOException e) {
			throw new RuntimeException(e);
		}
	}

	@EnableKafka
	@TestConfiguration
	public static class TestConfig {

		CountDownLatch partitionRevokedLatch = new CountDownLatch(0);

		CountDownLatch partitionAssignedLatch = new CountDownLatch(1);

		CountDownLatch listenerLatch = new CountDownLatch(2);

		@KafkaListener(id = GROUP_ID, topics = TOPIC_NAME, autoStartup = "false")
		void listen(ConsumerRecord<Integer, String> ignored) {
			listenerLatch.countDown();
		}

		@Bean
		KafkaTemplate<Integer, String> template(ProducerFactory<Integer, String> producerFactory) {
			return new KafkaTemplate<>(producerFactory);
		}

		@Bean
		ProducerFactory<Integer, String> producerFactory() {
			return new DefaultKafkaProducerFactory<>(KafkaTestUtils.producerProps(BROKER));
		}

		@Bean
		ConcurrentKafkaListenerContainerFactory<Integer, String>
		kafkaListenerContainerFactory(ConsumerFactory<Integer, String> consumerFactory) {

			final ConcurrentKafkaListenerContainerFactory<Integer, String> factory =
					new ConcurrentKafkaListenerContainerFactory<>();

			factory.setConsumerFactory(consumerFactory);

			factory.getContainerProperties().setConsumerRebalanceListener(new ConsumerAwareRebalanceListener() {
				@Override
				public void onPartitionsAssigned(Consumer<?, ?> consumer,
												Collection<TopicPartition> partitions) {
					partitionAssignedLatch.countDown();
				}

				@Override
				public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
					partitionRevokedLatch.countDown();
				}
			});
			return factory;
		}

		@Bean
		ConsumerFactory<Integer, String> consumerFactory() throws InterruptedException {
			Properties propsDummy = new Properties();
			propsDummy.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, BytesDeserializer.class.getName());
			propsDummy.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
									StringDeserializer.class.getName());
			propsDummy.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BROKER);
			propsDummy.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "hello");
			propsDummy.setProperty(ConsumerConfig.GROUP_PROTOCOL_CONFIG, "consumer");
			propsDummy.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
			Consumer<Object, Object> dummyConsumer = new KafkaConsumer<>(propsDummy);

			Thread.sleep(5000);
			dummyConsumer.close();

			final Map<String, Object> props = getConsumerProperties();
			return new DefaultKafkaConsumerFactory<>(props);
		}

		@Bean
		public ThreadPoolTaskExecutor threadPoolTaskExecutor() {
			final ThreadPoolTaskExecutor executor = new ThreadPoolTaskExecutor();
			executor.setCorePoolSize(5);
			executor.setQueueCapacity(20);
			executor.setMaxPoolSize(10);
			return executor;
		}

		@Bean
		public GenericContainer<?> brokerContainer() {
			final DockerImageName imageName = DockerImageName.parse(KAFKA_IMAGE_NAME);
			final GenericContainer<?> broker = new GenericContainer<>(imageName);

			broker.getEnvMap().putAll(getPropertiesFromFile());

			// Set Port forwarding. 12000 -> 9094.
			final String portFormat = String.format("%d:%d/%s", 12000, 9094, InternetProtocol.TCP.toDockerNotation());
			final List<String> portsBinding = Collections.singletonList(portFormat);
			broker.setPortBindings(portsBinding);

			return broker;
		}

	}

	public class RawConsumerRebalanceListener implements ConsumerRebalanceListener {

		@Override
		public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
			rawConsumerRevokedCount.countDown();
		}

		@Override
		public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
			rawConsumerAssignedCount.countDown();
		}

	}

}
