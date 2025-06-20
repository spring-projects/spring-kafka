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

package org.springframework.kafka.listener;

import java.time.Duration;
import java.util.Collection;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.Test;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.kafka.test.utils.KafkaTestUtils;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.junit.jupiter.SpringJUnitConfig;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * @author Soby Chacko
 * @since 3.1.2
 */
@SpringJUnitConfig
@DirtiesContext
@EmbeddedKafka(topics = "enforce-rebalance-topic")
public class ContainerEnforceRebalanceTests {

	@Test
	void enforceRebalance(@Autowired Config config, @Autowired KafkaTemplate<Integer, String> template,
						@Autowired KafkaListenerEndpointRegistry registry) throws InterruptedException {
		template.send("enforce-rebalance-topic", "my-data");
		final MessageListenerContainer listenerContainer = registry.getListenerContainer("enforce-rebalance-grp");
		assertThat(config.listenerLatch.await(10, TimeUnit.SECONDS)).isTrue();
		assertThat(listenerContainer).isNotNull();
		listenerContainer.enforceRebalance();
		assertThat(((ConcurrentMessageListenerContainer<?, ?>) listenerContainer).enforceRebalanceRequested).isTrue();
		// The test is expecting partition revoke once and assign twice.
		assertThat(config.partitionRevokedLatch.await(10, TimeUnit.SECONDS)).isTrue();
		assertThat(config.partitionAssignedLatch.await(10, TimeUnit.SECONDS)).isTrue();
		assertThat(((ConcurrentMessageListenerContainer<?, ?>) listenerContainer).enforceRebalanceRequested).isFalse();
		listenerContainer.pause();
		await().timeout(Duration.ofSeconds(10)).untilAsserted(() -> assertThat(listenerContainer.isPauseRequested()).isTrue());
		await().timeout(Duration.ofSeconds(10)).untilAsserted(() -> assertThat(listenerContainer.isContainerPaused()).isTrue());
		// resetting the latches
		config.partitionRevokedLatch = new CountDownLatch(1);
		config.partitionAssignedLatch = new CountDownLatch(1);
		listenerContainer.enforceRebalance();
		assertThat(config.partitionRevokedLatch.await(10, TimeUnit.SECONDS)).isTrue();
		assertThat(config.partitionAssignedLatch.await(10, TimeUnit.SECONDS)).isTrue();
		// Although the rebalance causes the consumer to resume again, since the container is paused,
		// it will pause the rebalanced consumers again.
		assertThat(listenerContainer.isPauseRequested()).isTrue();
		assertThat(listenerContainer.isContainerPaused()).isTrue();
	}

	@Configuration
	@EnableKafka
	public static class Config {

		@Autowired
		EmbeddedKafkaBroker broker;

		CountDownLatch partitionRevokedLatch = new CountDownLatch(1);

		CountDownLatch partitionAssignedLatch = new CountDownLatch(2);

		CountDownLatch listenerLatch = new CountDownLatch(1);

		@KafkaListener(id = "enforce-rebalance-grp", topics = "enforce-rebalance-topic")
		void listen(ConsumerRecord<Integer, String> ignored) {
			listenerLatch.countDown();
		}

		@Bean
		KafkaTemplate<Integer, String> template(ProducerFactory<Integer, String> pf) {
			return new KafkaTemplate<>(pf);
		}

		@Bean
		ProducerFactory<Integer, String> pf() {
			return new DefaultKafkaProducerFactory<>(KafkaTestUtils.producerProps(this.broker));
		}

		@Bean
		ConcurrentKafkaListenerContainerFactory<Integer, String> kafkaListenerContainerFactory(
				ConsumerFactory<Integer, String> cf) {
			ConcurrentKafkaListenerContainerFactory<Integer, String> factory =
					new ConcurrentKafkaListenerContainerFactory<>();
			factory.setConsumerFactory(cf);
			factory.getContainerProperties().setConsumerRebalanceListener(new ConsumerAwareRebalanceListener() {
				@Override
				public void onPartitionsAssigned(Consumer<?, ?> consumer, Collection<TopicPartition> partitions) {
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
		ConsumerFactory<Integer, String> cf() {
			return new DefaultKafkaConsumerFactory<>(
					KafkaTestUtils.consumerProps(this.broker, "enforce-rebalance-topic", false));
		}
	}

}
