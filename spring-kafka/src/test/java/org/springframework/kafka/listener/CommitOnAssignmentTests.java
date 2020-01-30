/*
 * Copyright 2020 the original author or authors.
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

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyMap;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.BDDMockito.given;
import static org.mockito.BDDMockito.willAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.withSettings;

import java.time.Duration;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.producer.Producer;
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
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.listener.ContainerProperties.AssignmentCommitOption;
import org.springframework.kafka.test.utils.KafkaTestUtils;
import org.springframework.kafka.transaction.KafkaTransactionManager;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.annotation.DirtiesContext.ClassMode;
import org.springframework.test.context.junit.jupiter.SpringJUnitConfig;

/**
 * @author Gary Russell
 * @since 2.3.6
 *
 */
@SpringJUnitConfig
@DirtiesContext(classMode = ClassMode.AFTER_EACH_TEST_METHOD)
public class CommitOnAssignmentTests {

	@SuppressWarnings("rawtypes")
	@Autowired
	private Consumer consumer;

	@Autowired
	private Config config;

	@Autowired
	private KafkaListenerEndpointRegistry registry;

	@Test
	void testAlways() throws InterruptedException {
		this.registry.start();
		assertThat(this.config.commitLatch.await(10, TimeUnit.SECONDS)).isTrue();
	}

	@Test
	void testNever() throws InterruptedException {
		this.registry.getListenerContainer("foo").getContainerProperties()
				.setAssignmentCommitOption(AssignmentCommitOption.NEVER);
		this.registry.start();
		assertThat(this.config.assignLatch.await(10, TimeUnit.SECONDS)).isTrue();
		this.registry.stop();
		assertThat(this.config.closeLatch.await(10, TimeUnit.SECONDS)).isTrue();
		assertThat(this.config.commitLatch.getCount()).isEqualTo(1L);
	}

	@Test
	void testLastOnly() throws InterruptedException {
		this.registry.getListenerContainer("foo").getContainerProperties()
				.setAssignmentCommitOption(AssignmentCommitOption.LATEST_ONLY);
		this.registry.start();
		assertThat(this.config.commitLatch.await(10, TimeUnit.SECONDS)).isTrue();
	}

	@SuppressWarnings({ "rawtypes", "unchecked" })
	@Test
	void testLatestOnlyTx() throws InterruptedException {
		ContainerProperties props = this.registry.getListenerContainer("foo").getContainerProperties();
		props.setAssignmentCommitOption(AssignmentCommitOption.LATEST_ONLY);
		ProducerFactory pf = mock(ProducerFactory.class, withSettings().verboseLogging());
		given(pf.transactionCapable()).willReturn(true);
		KafkaTransactionManager tm = new KafkaTransactionManager<>(pf);
		Producer producer = mock(Producer.class);
		given(pf.createProducer(any())).willReturn(producer);
		CountDownLatch latch = new CountDownLatch(1);
		willAnswer(inv -> {
			latch.countDown();
			return null;
		}).given(producer).sendOffsetsToTransaction(any(), anyString());
		props.setTransactionManager(tm);
		this.registry.start();
		assertThat(latch.await(10, TimeUnit.SECONDS)).isTrue();
	}

	@SuppressWarnings({ "rawtypes", "unchecked" })
	@Test
	void testLatestOnlyNoTx() throws InterruptedException {
		ContainerProperties props = this.registry.getListenerContainer("foo").getContainerProperties();
		props.setAssignmentCommitOption(AssignmentCommitOption.LATEST_ONLY_NO_TX);
		ProducerFactory pf = mock(ProducerFactory.class, withSettings().verboseLogging());
		given(pf.transactionCapable()).willReturn(true);
		KafkaTransactionManager tm = new KafkaTransactionManager<>(pf);
		Producer producer = mock(Producer.class);
		given(pf.createProducer(any())).willReturn(producer);
		CountDownLatch latch = new CountDownLatch(1);
		props.setTransactionManager(tm);
		this.registry.start();
		assertThat(this.config.commitLatch.await(10, TimeUnit.SECONDS)).isTrue();
		verify(producer, never()).sendOffsetsToTransaction(any(), anyString());
	}

	@Configuration
	@EnableKafka
	public static class Config {

		final CountDownLatch assignLatch = new CountDownLatch(1);

		final CountDownLatch commitLatch = new CountDownLatch(1);

		final CountDownLatch closeLatch = new CountDownLatch(1);

		private int count;

		@KafkaListener(id = "foo", topics = "foo", groupId = "grp", autoStartup = "false")
		public void foo(String in) {
		}

		@SuppressWarnings({ "rawtypes" })
		@Bean
		public ConsumerFactory consumerFactory() {
			ConsumerFactory consumerFactory = mock(ConsumerFactory.class);
			final Consumer consumer = consumer();
			given(consumerFactory.createConsumer("grp", "", "-0", KafkaTestUtils.defaultPropertyOverrides()))
				.willReturn(consumer);
			return consumerFactory;
		}

		@SuppressWarnings({ "rawtypes", "unchecked" })
		@Bean
		public Consumer consumer() {
			final Consumer consumer = mock(Consumer.class);
			final TopicPartition topicPartition0 = new TopicPartition("foo", 0);
			willAnswer(i -> {
				((ConsumerRebalanceListener) i.getArgument(1)).onPartitionsAssigned(
						Collections.singletonList(topicPartition0));
				this.assignLatch.countDown();
				return null;
			}).given(consumer).subscribe(any(Collection.class), any(ConsumerRebalanceListener.class));
			Map<TopicPartition, List<ConsumerRecord>> records1 = new LinkedHashMap<>();
			willAnswer(i -> {
				try {
					Thread.sleep(100);
				}
				catch (InterruptedException e) {
					Thread.currentThread().interrupt();
				}
				return new ConsumerRecords(records1);
			}).given(consumer).poll(Duration.ofMillis(ContainerProperties.DEFAULT_POLL_TIMEOUT));
			willAnswer(i -> {
				this.commitLatch.countDown();
				return null;
			}).given(consumer).commitSync(anyMap(), any());
			willAnswer(i -> {
				this.closeLatch.countDown();
				return null;
			}).given(consumer).close();
			return consumer;
		}

		@SuppressWarnings({ "rawtypes", "unchecked" })
		@Bean
		public ConcurrentKafkaListenerContainerFactory kafkaListenerContainerFactory() {
			ConcurrentKafkaListenerContainerFactory factory = new ConcurrentKafkaListenerContainerFactory();
			factory.setConsumerFactory(consumerFactory());
			return factory;
		}

	}

}
