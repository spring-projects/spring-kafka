/*
 * Copyright 2025-present the original author or authors.
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

package org.springframework.kafka.core;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AlterConfigOp;
import org.apache.kafka.clients.admin.ConfigEntry;
import org.apache.kafka.clients.consumer.ShareConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.Test;

import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.context.EmbeddedKafka;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Basic tests for {@link DefaultShareConsumerFactory}.
 *
 * @author Soby Chacko
 * @since 4.0
 */
@EmbeddedKafka(
		topics = {"embedded-share-test", "embedded-share-distribution-test"}, partitions = 1,
		brokerProperties = {
				"share.coordinator.state.topic.replication.factor=1",
				"share.coordinator.state.topic.min.isr=1"
		})
class DefaultShareConsumerFactoryTests {

	@Test
	void shouldInstantiateWithConfigs() {
		Map<String, Object> configs = new HashMap<>();
		configs.put("bootstrap.servers", "localhost:9092");
		DefaultShareConsumerFactory<String, String> factory = new DefaultShareConsumerFactory<>(configs);
		assertThat(factory).isNotNull();
		assertThat(factory.getConfigurationProperties()).containsKey("bootstrap.servers");
	}

	@Test
	void shouldConfigureDeserializersViaSetters() {
		var configs = new HashMap<String, Object>();
		var factory = new DefaultShareConsumerFactory<String, String>(configs);
		var keyDeserializer = new StringDeserializer();
		var valueDeserializer = new StringDeserializer();
		factory.setKeyDeserializer(keyDeserializer);
		factory.setValueDeserializer(valueDeserializer);
		assertThat(factory.getKeyDeserializer())
				.as("Key deserializer should match the set instance")
				.isSameAs(keyDeserializer);
		assertThat(factory.getValueDeserializer())
				.as("Value deserializer should match the set instance")
				.isSameAs(valueDeserializer);
	}

	@Test
	void shouldConfigureDeserializersViaConstructor() {
		var configs = new HashMap<String, Object>();
		var keyDeserializer = new StringDeserializer();
		var valueDeserializer = new StringDeserializer();
		var factory = new DefaultShareConsumerFactory<>(configs, keyDeserializer, valueDeserializer, true);
		assertThat(factory.getKeyDeserializer())
				.as("Key deserializer should match the constructor instance")
				.isSameAs(keyDeserializer);
		assertThat(factory.getValueDeserializer())
				.as("Value deserializer should match the constructor instance")
				.isSameAs(valueDeserializer);
	}

	@Test
	void shouldRegisterAndRemoveListeners() {
		var configs = new HashMap<String, Object>();
		var factory = new DefaultShareConsumerFactory<String, String>(configs);
		var listener = new ShareConsumerFactory.Listener<String, String>() {

		};
		factory.addListener(listener);
		assertThat(factory.getListeners())
				.as("Listeners should contain the added listener")
				.contains(listener);
		factory.removeListener(listener);
		assertThat(factory.getListeners())
				.as("Listeners should not contain the removed listener")
				.doesNotContain(listener);
	}

	@Test
	void shouldCreateShareConsumer() {
		Map<String, Object> configs = new HashMap<>();
		configs.put("bootstrap.servers", "localhost:9092");
		configs.put("key.deserializer", StringDeserializer.class);
		configs.put("value.deserializer", StringDeserializer.class);
		DefaultShareConsumerFactory<String, String> factory = new DefaultShareConsumerFactory<>(configs);
		ShareConsumer<String, String> shareConsumer = factory.createShareConsumer("group", "myapp-client-id");
		assertThat(shareConsumer).isNotNull();
	}

	@Test
	void shouldReturnUnmodifiableListenersList() {
		var configs = new HashMap<String, Object>();
		var factory = new DefaultShareConsumerFactory<String, String>(configs);
		var listener = new ShareConsumerFactory.Listener<String, String>() {

		};
		factory.addListener(listener);
		var listeners = factory.getListeners();
		assertThat(listeners).contains(listener);
		// Attempting to modify the returned list should throw
		assertThatThrownBy(() -> listeners.add(new ShareConsumerFactory.Listener<>() {

		}))
				.as("Listeners list should be unmodifiable")
				.isInstanceOf(UnsupportedOperationException.class);
	}

	@Test
	void integrationTestDefaultShareConsumerFactory(EmbeddedKafkaBroker broker) throws Exception {
		final String topic = "embedded-share-test";
		final String groupId = "testGroup";
		var bootstrapServers = broker.getBrokersAsString();

		var producerProps = new java.util.Properties();
		producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
		producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
		producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);

		try (var producer = new KafkaProducer<String, String>(producerProps)) {
			producer.send(new ProducerRecord<>(topic, "key", "integration-test-value")).get();
		}

		setShareAutoOffsetResetEarliest(bootstrapServers, groupId);

		var consumerProps = new HashMap<String, Object>();
		consumerProps.put("bootstrap.servers", bootstrapServers);
		consumerProps.put("key.deserializer", StringDeserializer.class);
		consumerProps.put("value.deserializer", StringDeserializer.class);
		consumerProps.put("group.id", groupId);

		var factory = new DefaultShareConsumerFactory<String, String>(consumerProps);
		var consumer = factory.createShareConsumer(groupId, "myapp-client-id");
		consumer.subscribe(Collections.singletonList(topic));

		var records = consumer.poll(Duration.ofSeconds(10));
		assertThat(records.count())
				.as("Should have received at least one record")
				.isGreaterThan(0);
		assertThat(records.iterator().next().value())
				.as("Record value should match")
				.isEqualTo("integration-test-value");
		consumer.close();
	}

	@Test
	void integrationTestSharedConsumersDistribution(EmbeddedKafkaBroker broker) throws Exception {
		String topic = "shared-consumer-dist-test";
		final String groupId = "distributionTestGroup";
		int recordCount = 8;
		List<String> consumerIds = List.of("client-dist-1", "client-dist-2");
		List<String> allReceived = runSharedConsumerTest(topic, groupId, consumerIds, recordCount, broker);

		// Assert all records were received (no loss and no duplicates)
		assertThat(allReceived)
			.containsExactlyInAnyOrder(
				topic + "-value-0",
				topic + "-value-1",
				topic + "-value-2",
				topic + "-value-3",
				topic + "-value-4",
				topic + "-value-5",
				topic + "-value-6",
				topic + "-value-7"
			)
			.doesNotHaveDuplicates();
	}

	/**
	 * Runs multiple Kafka consumers in parallel using ExecutorService, collects all records received,
	 * and returns a list of all record values received by all consumers.
	 */
	private static List<String> runSharedConsumerTest(String topic, String groupId,
			List<String> consumerIds, int recordCount, EmbeddedKafkaBroker broker) throws Exception {
		var bootstrapServers = broker.getBrokersAsString();

		var producerProps = new java.util.Properties();
		producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
		producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
		producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
		try (var producer = new KafkaProducer<String, String>(producerProps)) {
			for (int i = 0; i < recordCount; i++) {
				producer.send(new ProducerRecord<>(topic, "key" + i, topic + "-value-" + i)).get();
			}
			producer.flush();
		}

		setShareAutoOffsetResetEarliest(bootstrapServers, groupId);

		List<String> allReceived = Collections.synchronizedList(new ArrayList<>());
		var latch = new java.util.concurrent.CountDownLatch(recordCount);
		ExecutorService executor = Executors.newCachedThreadPool();
		DefaultShareConsumerFactory<String, String> shareConsumerFactory = new DefaultShareConsumerFactory<>(
				Map.of(
					"bootstrap.servers", bootstrapServers,
					"key.deserializer", org.apache.kafka.common.serialization.StringDeserializer.class,
					"value.deserializer", org.apache.kafka.common.serialization.StringDeserializer.class
				)
		);
		for (int i = 0; i < consumerIds.size(); i++) {
			final int idx = i;
			executor.submit(() -> {
				try (var consumer = shareConsumerFactory.createShareConsumer(groupId, consumerIds.get(idx))) {
					consumer.subscribe(Collections.singletonList(topic));
					while (latch.getCount() > 0) {
						var records = consumer.poll(Duration.ofMillis(200));
						for (var r : records) {
							allReceived.add(r.value());
							// Leverage implicit acknowledgment where records are automatically treated as ACCEPT
							// Use commitSync() for explicit commit timing instead of relying on next poll() auto-commit
							consumer.commitSync(Duration.ofMillis(10000));
							latch.countDown();
						}
					}
				}
			});
		}

		assertThat(latch.await(10, TimeUnit.SECONDS))
			.as("All records should be received within timeout")
			.isTrue();
		executor.shutdown();
		assertThat(executor.awaitTermination(10, TimeUnit.SECONDS))
			.as("Executor should terminate after shutdown")
			.isTrue();
		return allReceived;
	}

	/**
	 * Sets the share.auto.offset.reset group config to earliest for the given groupId,
	 * using the provided bootstrapServers.
	 */
	private static void setShareAutoOffsetResetEarliest(String bootstrapServers, String groupId) throws Exception {
		Map<String, Object> adminProperties = new HashMap<>();
		adminProperties.put("bootstrap.servers", bootstrapServers);
		ConfigEntry entry = new ConfigEntry("share.auto.offset.reset", "earliest");
		AlterConfigOp op = new AlterConfigOp(entry, AlterConfigOp.OpType.SET);
		Map<ConfigResource, Collection<AlterConfigOp>> configs = Map.of(
			new ConfigResource(ConfigResource.Type.GROUP, groupId), List.of(op));
		try (Admin admin = AdminClient.create(adminProperties)) {
			admin.incrementalAlterConfigs(configs).all().get();
		}
	}

}
