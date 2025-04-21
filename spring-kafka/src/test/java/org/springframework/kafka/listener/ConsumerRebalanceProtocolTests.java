/*
 * Copyright 2025 the original author or authors.
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

import java.time.Duration;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.awaitility.Awaitility;
import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.Test;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.junit.jupiter.SpringJUnitConfig;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Rudimentary test to verify the consumer rebalance protocols.
 *
 * @author Soby Chacko
 * @since 4.0.0
 */
@SpringJUnitConfig
@DirtiesContext
@EmbeddedKafka(topics = "rebalance.test", partitions = 6)
public class ConsumerRebalanceProtocolTests {

	@Autowired
	private EmbeddedKafkaBroker embeddedKafka;

	@Autowired
	private ConsumerFactory<String, String> consumerFactoryWithNewProtocol;

	@Autowired
	private ConsumerFactory<String, String> consumerFactoryWithLegacyProtocol;

	@Test
	public void testRebalanceWithNewProtocol() throws Exception {
		testRebalance(consumerFactoryWithNewProtocol, "new-protocol-group", true);
	}

	@Test
	public void testRebalanceWithLegacyProtocol() throws Exception {
		testRebalance(consumerFactoryWithLegacyProtocol, "legacy-protocol-group", false);
	}

	private void testRebalance(ConsumerFactory<String, String> consumerFactory, String groupId, boolean isNewProtocol)
			throws Exception {
		AtomicReference<Consumer<?, ?>> consumerRef = new AtomicReference<>();
		CountDownLatch revokedBeforeCommitLatch = new CountDownLatch(1);
		CountDownLatch revokedAfterCommitLatch = new CountDownLatch(1);
		CountDownLatch assignedLatch = new CountDownLatch(1);

		Set<TopicPartition> assignedPartitionSet = new HashSet<>();

		ConsumerAwareRebalanceListener listener = new ConsumerAwareRebalanceListener() {
			@Override
			public void onPartitionsRevokedBeforeCommit(Consumer<?, ?> consumer, Collection<TopicPartition> partitions) {
				consumerRef.set(consumer);
				revokedBeforeCommitLatch.countDown();
				assignedPartitionSet.removeAll(partitions);
			}

			@Override
			public void onPartitionsRevokedAfterCommit(Consumer<?, ?> consumer, Collection<TopicPartition> partitions) {
				consumerRef.set(consumer);
				revokedAfterCommitLatch.countDown();
				assignedPartitionSet.removeAll(partitions);
			}

			@Override
			public void onPartitionsAssigned(Consumer<?, ?> consumer, Collection<TopicPartition> partitions) {
				consumerRef.set(consumer);
				assignedLatch.countDown();
				assignedPartitionSet.addAll(partitions);
			}
		};

		KafkaMessageListenerContainer<String, String> container1 =
				new KafkaMessageListenerContainer<>(consumerFactory, getContainerProperties(groupId, listener));
		container1.start();

		assertThat(assignedLatch.await(10, TimeUnit.SECONDS)).isTrue();

		KafkaMessageListenerContainer<String, String> container2 =
				new KafkaMessageListenerContainer<>(consumerFactory, getContainerProperties(groupId, listener));
		container2.start();

		assertThat(revokedBeforeCommitLatch.await(10, TimeUnit.SECONDS)).isTrue();
		assertThat(revokedAfterCommitLatch.await(10, TimeUnit.SECONDS)).isTrue();

		assertThat(consumerRef.get()).isNotNull();
		assertThat(consumerRef.get()).isInstanceOf(Consumer.class);

		// In both protocols (new vs legacy), we expect the assignments to contain 6 partitions.
		// The way they get assigned are dictated by the Kafka server and beyond the scope of this test.
		// What we are mostly interested is assignment works properly in both protocols.
		// The new protocol may take several incremental rebalances to attain the full assignments - thus waiting for a
		// longer duration.
		Awaitility.await().timeout(Duration.ofSeconds(30))
				.untilAsserted(() -> assertThat(assignedPartitionSet.size() == 6).isTrue());

		container1.stop();
		container2.stop();
	}

	private static @NotNull ContainerProperties getContainerProperties(String groupId, ConsumerAwareRebalanceListener listener) {
		ContainerProperties containerProps = new ContainerProperties("rebalance.test");
		containerProps.setGroupId(groupId);
		containerProps.setConsumerRebalanceListener(listener);
		containerProps.setMessageListener((MessageListener<String, String>) (ConsumerRecord<String, String> record) -> {
		});
		return containerProps;
	}

	@Configuration
	public static class Config {

		@Bean
		public ConsumerFactory<String, String> consumerFactoryWithNewProtocol(EmbeddedKafkaBroker embeddedKafka) {
			Map<String, Object> props = new HashMap<>();
			props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, embeddedKafka.getBrokersAsString());
			props.put(ConsumerConfig.GROUP_ID_CONFIG, "new-protocol-group");
			props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
			props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
			props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
			props.put("group.protocol", "consumer");
			return new DefaultKafkaConsumerFactory<>(props);
		}

		@Bean
		public ConsumerFactory<String, String> consumerFactoryWithLegacyProtocol(EmbeddedKafkaBroker embeddedKafka) {
			Map<String, Object> props = new HashMap<>();
			props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, embeddedKafka.getBrokersAsString());
			props.put(ConsumerConfig.GROUP_ID_CONFIG, "legacy-protocol-group");
			props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
			props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
			props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
			props.put("group.protocol", "classic");
			return new DefaultKafkaConsumerFactory<>(props);
		}
	}

}
