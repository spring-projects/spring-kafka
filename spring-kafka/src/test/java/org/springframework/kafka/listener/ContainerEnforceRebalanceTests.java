/*
 * Copyright 2024-present the original author or authors.
 */

package org.springframework.kafka.listener;

import java.util.Collection;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
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
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.BDDMockito.given;
import static org.mockito.BDDMockito.willAnswer;
import static org.mockito.Mockito.mock;

/**
 * @author Soby Chacko
 * @author Artem Bilan
 * @author Minju Kim
 *
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
		// The test is expecting partition revoke once and assign twice.
		assertThat(config.partitionRevokedLatch.await(10, TimeUnit.SECONDS)).isTrue();
		assertThat(config.partitionAssignedLatch.await(10, TimeUnit.SECONDS)).isTrue();
		assertThat(((ConcurrentMessageListenerContainer<?, ?>) listenerContainer).enforceRebalanceRequested).isFalse();
		listenerContainer.pause();
		await().until(listenerContainer::isPauseRequested);
		await().until(listenerContainer::isContainerPaused);
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

	@Test
	void enforceRebalanceBeforeStartIsAppliedOnStart() throws InterruptedException {
		ConsumerFactory<Integer, String> consumerFactory = mock();
		Consumer<Integer, String> consumer = mock();
		given(consumerFactory.createConsumer(any(), any(), any(), any())).willReturn(consumer);
		given(consumer.poll(any())).willReturn(ConsumerRecords.empty());
		CountDownLatch enforceRebalanceLatch = new CountDownLatch(1);
		willAnswer(invocation -> {
			enforceRebalanceLatch.countDown();
			return null;
		}).given(consumer).enforceRebalance(any());

		ContainerProperties containerProperties = new ContainerProperties("enforce-rebalance-topic");
		containerProperties.setGroupId("enforce-rebalance-before-start");
		containerProperties.setMessageListener((MessageListener<Integer, String>) data -> {
		});
		ConcurrentMessageListenerContainer<Integer, String> container =
				new ConcurrentMessageListenerContainer<>(consumerFactory, containerProperties);

		container.enforceRebalance();
		assertThat(container.enforceRebalanceRequested).isTrue();

		container.start();
		try {
			assertThat(enforceRebalanceLatch.await(10, TimeUnit.SECONDS)).isTrue();
		}
		finally {
			container.stop();
		}
	}

	@Configuration(proxyBeanMethods = false)
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
			factory.getContainerProperties()
					.setConsumerRebalanceListener(new ConsumerAwareRebalanceListener() {

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
