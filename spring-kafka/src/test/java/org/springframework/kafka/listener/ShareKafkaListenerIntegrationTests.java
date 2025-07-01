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
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AlterConfigOp;
import org.apache.kafka.clients.admin.AlterConfigsResult;
import org.apache.kafka.clients.admin.ConfigEntry;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.Test;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.config.ShareKafkaListenerContainerFactory;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.DefaultShareConsumerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.core.ShareConsumerFactory;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.junit.jupiter.SpringJUnitConfig;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Integration tests for share Kafka listener.
 *
 * @author Soby Chacko
 * @since 4.0
 */
@SpringJUnitConfig
@DirtiesContext
@EmbeddedKafka(topics = "share-listener-integration-test",
		brokerProperties = {
				"unstable.api.versions.enable=true",
				"group.coordinator.rebalance.protocols=classic,share",
				"share.coordinator.state.topic.replication.factor=1",
				"share.coordinator.state.topic.min.isr=1"
		})
class ShareKafkaListenerIntegrationTests {

	private static final CountDownLatch latch = new CountDownLatch(1);

	private static final AtomicReference<String> received = new AtomicReference<>();

	@Autowired
	EmbeddedKafkaBroker broker;

	@Test
	void integrationTestShareKafkaListener() throws Exception {
		final String topic = "share-listener-integration-test";
		final String groupId = "share-listener-test-group";
		setShareAutoOffsetResetEarliest(this.broker.getBrokersAsString(), groupId);

		Map<String, Object> props = new HashMap<>();
		props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, this.broker.getBrokersAsString());
		props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
		props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
		ProducerFactory<String, String> pf = new DefaultKafkaProducerFactory<>(props);
		KafkaTemplate<String, String> template = new KafkaTemplate<>(pf);
		template.setDefaultTopic(topic);
		template.sendDefault("foo");
		assertThat(latch.await(10, TimeUnit.SECONDS)).isTrue();
		assertThat(received.get()).isEqualTo("foo");
	}

	/**
	 * Sets the share.auto.offset.reset group config to earliest for the given groupId,
	 * using the provided bootstrapServers.
	 */
	private static void setShareAutoOffsetResetEarliest(String bootstrapServers, String groupId) throws Exception {
		Map<String, Object> adminProperties = new HashMap<>();
		adminProperties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
		try (Admin admin = Admin.create(adminProperties)) {
			ConfigResource configResource = new ConfigResource(ConfigResource.Type.GROUP, groupId);
			ConfigEntry configEntry = new ConfigEntry("share.auto.offset.reset", "earliest");
			Map<ConfigResource, Collection<AlterConfigOp>> configs = java.util.Collections.singletonMap(configResource,
					java.util.Collections.singleton(new AlterConfigOp(configEntry, AlterConfigOp.OpType.SET)));
			AlterConfigsResult alterConfigsResult = admin.incrementalAlterConfigs(configs);
			alterConfigsResult.all().get();
		}
	}

	@Configuration
	@EnableKafka
	static class TestConfig {

		@Bean
		public ShareConsumerFactory<String, String> shareConsumerFactory(EmbeddedKafkaBroker broker) {
			Map<String, Object> configs = new HashMap<>();
			configs.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, broker.getBrokersAsString());
			configs.put(ConsumerConfig.GROUP_ID_CONFIG, "share-listener-test-group");
			configs.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
			configs.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
			return new DefaultShareConsumerFactory<>(configs);
		}

		@Bean
		public ShareKafkaListenerContainerFactory<String, String> shareKafkaListenerContainerFactory(
				ShareConsumerFactory<String, String> shareConsumerFactory) {
			return new ShareKafkaListenerContainerFactory<>(shareConsumerFactory);
		}

		@Bean
		public TestListener listener() {
			return new TestListener();
		}
	}

	static class TestListener {

		@KafkaListener(topics = "share-listener-integration-test", containerFactory = "shareKafkaListenerContainerFactory")
		public void listen(ConsumerRecord<String, String> record) {
			received.set(record.value());
			latch.countDown();
		}
	}
}
