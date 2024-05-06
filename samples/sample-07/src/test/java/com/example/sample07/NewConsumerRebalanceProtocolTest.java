package com.example.sample07;

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
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.kafka.core.*;
import org.springframework.kafka.listener.ConsumerAwareRebalanceListener;
import org.springframework.kafka.listener.MessageListenerContainer;
import org.springframework.kafka.test.utils.KafkaTestUtils;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;
import org.springframework.test.annotation.DirtiesContext;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.InternetProtocol;
import org.testcontainers.utility.DockerImageName;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

@SpringBootTest
@DirtiesContext
public class NewConsumerRebalanceProtocolTest {

	static final String GROUP_ID = "hello";
	static final String TOPIC_NAME = "hello-topic";
	static final String BROKER = "localhost:12000";
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


	@Test
	public void test3() throws InterruptedException, ExecutionException {

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
		this.template.send(TOPIC_NAME, 1, null,"my-data");

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
		this.template.send(TOPIC_NAME, 1, null,"my-data");
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

	@EnableKafka
	@Configuration
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
		ConsumerFactory<Integer, String> consumerFactory() {
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
		GenericContainer genericContainer(ConsumerFactory<Integer, String> cf) throws InterruptedException {
			GenericContainer genericContainer = setUpBroker();

			Properties props = new Properties();
			props.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, BytesDeserializer.class.getName());
			props.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
							  StringDeserializer.class.getName());
			props.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BROKER);
			props.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "hello");
			props.setProperty(ConsumerConfig.GROUP_PROTOCOL_CONFIG, "consumer");
			props.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
			Consumer<Object, Object> dummyConsumer = new KafkaConsumer<>(props);

			Thread.sleep(5000);
			dummyConsumer.close();

			return genericContainer;
		}

		GenericContainer setUpBroker() {
			final DockerImageName imageName = DockerImageName.parse(KAFKA_IMAGE_NAME);
			final GenericContainer broker = new GenericContainer(imageName);

			// Set KRAFT.
			broker.addEnv("KAFKA_CFG_NODE_ID", "0");
			broker.addEnv("KAFKA_CFG_KRAFT_CLUSTER_ID", "HsDBs9l6UUmQq7Y5E6bNlw");
			broker.addEnv("KAFKA_CFG_CONTROLLER_QUORUM_VOTERS", "0@localhost:9093");
			broker.addEnv("KAFKA_CFG_PROCESS_ROLES", "controller,broker");

			// Set listener.
			broker.addEnv("KAFKA_CFG_LISTENERS",
						  "INTERNAL://localhost:29092, PLAINTEXT://0.0.0.0:9092, EXTERNAL://:9094, CONTROLLER://:9093");
			broker.addEnv("KAFKA_CFG_ADVERTISED_LISTENERS",
						  "INTERNAL://localhost:29092, PLAINTEXT://localhost:9092, EXTERNAL://127.0.0.1:12000");
			broker.addEnv("KAFKA_CFG_LISTENER_SECURITY_PROTOCOL_MAP",
						  "CONTROLLER:PLAINTEXT,EXTERNAL:PLAINTEXT,PLAINTEXT:PLAINTEXT,INTERNAL:PLAINTEXT");
			broker.addEnv("KAFKA_CFG_CONTROLLER_LISTENER_NAMES", "CONTROLLER");

			// Set ETC.
			broker.addEnv("KAFKA_CFG_AUTO_CREATE_TOPICS_ENABLE", "true");
			broker.addEnv("KAFKA_CFG_NUM_PARTITIONS", "2");
			broker.addEnv("KAFKA_CFG_INTER_BROKER_LISTENER_NAME", "INTERNAL");
			broker.addEnv("KAFKA_CFG_GROUP_INITIAL_REBALANCE_DELAY_MS", "0");
			broker.addEnv("KAFKA_CFG_TRANSACTION_STATE_LOG_MIN_ISR", "1");
			broker.addEnv("KAFKA_CFG_TRANSACTION_STATE_LOG_REPLICATION_FACTOR", "1");
			broker.addEnv("KAFKA_CFG_OFFSETS_TOPIC_REPLICATION_FACTOR", "1");
			broker.addEnv("KAFKA_CFG_DEFAULT_REPLICATION_FACTOR", "1");

			// KIP-848. If you want more, see links below.
			// https://cwiki.apache.org/confluence/display/KAFKA/KIP-848%3A+The+Next+Generation+of+the+Consumer+Rebalance+Protocol
			// https://cwiki.apache.org/confluence/display/KAFKA/The+Next+Generation+of+the+Consumer+Rebalance+Protocol+%28KIP-848%29+-+Early+Access+Release+Notes
			broker.addEnv("KAFKA_CFG_GROUP_COORDINATOR_REBALANCE_PROTOCOLS", "classic,consumer");
			broker.addEnv("KAFKA_CFG_TRANSACTION_PARTITION_VERIFICATION_ENABLE", "false");

			// Set Port forwarding. 12000 -> 9094.
			String portFormat = String.format("%d:%d/%s", 12000, 9094, InternetProtocol.TCP.toDockerNotation());
			final List<String> portsBinding = Collections.singletonList(portFormat);
			broker.setPortBindings(portsBinding);
			broker.start();
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
