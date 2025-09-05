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

package org.springframework.kafka.listener;

import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AlterConfigOp;
import org.apache.kafka.clients.admin.ConfigEntry;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.Test;

import org.springframework.kafka.core.DefaultShareConsumerFactory;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.context.EmbeddedKafka;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Basic tests for {@link ShareKafkaMessageListenerContainer}.
 *
 * @author Soby Chacko
 * @since 4.0
 */
@EmbeddedKafka(
	topics = {"share-listener-integration-test"}, partitions = 1,
	brokerProperties = {
		"share.coordinator.state.topic.replication.factor=1",
		"share.coordinator.state.topic.min.isr=1"
	}
)
class ShareKafkaMessageListenerContainerIntegrationTests {

	@Test
	void integrationTestShareKafkaMessageListenerContainer(EmbeddedKafkaBroker broker) throws Exception {
		final String topic = "share-listener-integration-test";
		final String groupId = "shareListenerGroup";
		String bootstrapServers = broker.getBrokersAsString();

		// Produce a record
		var producerProps = new java.util.Properties();
		producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
		producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
		producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
		try (var producer = new KafkaProducer<String, String>(producerProps)) {
			producer.send(new ProducerRecord<>(topic, "key", "integration-test-value")).get();
		}

		setShareAutoOffsetResetEarliest(bootstrapServers, groupId);

		var consumerProps = new java.util.HashMap<String, Object>();
		consumerProps.put("bootstrap.servers", bootstrapServers);
		consumerProps.put("key.deserializer", StringDeserializer.class);
		consumerProps.put("value.deserializer", StringDeserializer.class);
		consumerProps.put("group.id", groupId);

		DefaultShareConsumerFactory<String, String> consumerFactory = new DefaultShareConsumerFactory<>(consumerProps);
		ContainerProperties containerProps = new ContainerProperties(topic);
		CountDownLatch latch = new CountDownLatch(1);
		AtomicReference<String> received = new AtomicReference<>();
		containerProps.setMessageListener((MessageListener<String, String>) record -> {
			received.set(record.value());
			latch.countDown();
		});

		ShareKafkaMessageListenerContainer<String, String> container =
				new ShareKafkaMessageListenerContainer<>(consumerFactory, containerProps);
		container.setBeanName("integrationTestShareKafkaMessageListenerContainer");
		container.start();

		try {
			assertThat(latch.await(10, java.util.concurrent.TimeUnit.SECONDS)
					&& "integration-test-value".equals(received.get()))
				.as("Message should be received and have expected value")
				.isTrue();
		}
		finally {
			container.stop();
		}
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
