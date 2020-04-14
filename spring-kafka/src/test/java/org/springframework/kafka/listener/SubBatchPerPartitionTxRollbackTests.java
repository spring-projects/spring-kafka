/*
 * Copyright 2017-2020 the original author or authors.
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
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.BDDMockito.given;
import static org.mockito.BDDMockito.willAnswer;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.record.TimestampType;
import org.junit.jupiter.api.Test;
import org.mockito.InOrder;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.listener.ContainerProperties.AckMode;
import org.springframework.kafka.support.TransactionSupport;
import org.springframework.kafka.test.utils.KafkaTestUtils;
import org.springframework.kafka.transaction.KafkaTransactionManager;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.junit.jupiter.SpringJUnitConfig;

/**
 * @author Gary Russell
 * @since 2.3.2
 *
 */
@SpringJUnitConfig
@DirtiesContext
public class SubBatchPerPartitionTxRollbackTests {

	private static final String CONTAINER_ID = "container";

	@SuppressWarnings("rawtypes")
	@Autowired
	private Consumer consumer;

	@SuppressWarnings("rawtypes")
	@Autowired
	private Producer producer;

	@Autowired
	private Config config;

	@Autowired
	private KafkaListenerEndpointRegistry registry;

	@SuppressWarnings("unchecked")
	@Test
	public void abortSecondBatch() throws Exception {
		assertThat(this.config.deliveryLatch.await(10, TimeUnit.SECONDS)).isTrue();
		assertThat(this.config.pollLatch.await(10, TimeUnit.SECONDS)).isTrue();
		this.registry.stop();
		assertThat(this.config.closeLatch.await(10, TimeUnit.SECONDS)).isTrue();
		InOrder inOrder = inOrder(this.consumer, this.producer);
		inOrder.verify(this.consumer).subscribe(any(Collection.class), any(ConsumerRebalanceListener.class));
		inOrder.verify(this.consumer).poll(Duration.ofMillis(ContainerProperties.DEFAULT_POLL_TIMEOUT));
		inOrder.verify(this.producer).beginTransaction();
		Map<TopicPartition, OffsetAndMetadata> offsets = new LinkedHashMap<>();
		offsets.put(new TopicPartition("foo", 0), new OffsetAndMetadata(2L));
		inOrder.verify(this.producer).sendOffsetsToTransaction(offsets, CONTAINER_ID);
		inOrder.verify(this.producer).commitTransaction();
		inOrder.verify(this.producer).beginTransaction();
		inOrder.verify(this.producer).abortTransaction();
		inOrder.verify(this.consumer).seek(new TopicPartition("foo", 1), 0L);
		offsets.clear();
		offsets.put(new TopicPartition("foo", 2), new OffsetAndMetadata(2L));
		inOrder.verify(this.producer).beginTransaction();
		inOrder.verify(this.producer).sendOffsetsToTransaction(offsets, CONTAINER_ID);
		inOrder.verify(this.producer).commitTransaction();
		offsets.clear();
		offsets.put(new TopicPartition("foo", 1), new OffsetAndMetadata(2L));
		inOrder.verify(this.producer).beginTransaction();
		inOrder.verify(this.producer).sendOffsetsToTransaction(offsets, CONTAINER_ID);
		inOrder.verify(this.producer).commitTransaction();
		assertThat(this.config.contents).containsExactly("foo", "bar", "baz", "qux", "fiz", "buz", "baz", "qux");
		assertThat(this.config.transactionSuffix).isNotNull();
	}

	@Configuration
	@EnableKafka
	public static class Config {

		final List<String> contents = new ArrayList<>();

		final CountDownLatch pollLatch = new CountDownLatch(2);

		final CountDownLatch deliveryLatch = new CountDownLatch(3);

		final CountDownLatch closeLatch = new CountDownLatch(1);

		boolean failSecondBatch = true;

		volatile String transactionSuffix;

		@KafkaListener(id = CONTAINER_ID, topics = "foo")
		public void foo(List<String> in) {
			this.contents.addAll(in);
			if (this.failSecondBatch && in.get(0).equals("baz")) {
				this.failSecondBatch = false;
				throw new RuntimeException("test");
			}
			this.deliveryLatch.countDown();
			this.transactionSuffix = TransactionSupport.getTransactionIdSuffix();
		}

		@SuppressWarnings({ "rawtypes" })
		@Bean
		public ConsumerFactory consumerFactory() {
			ConsumerFactory consumerFactory = mock(ConsumerFactory.class);
			final Consumer consumer = consumer();
			given(consumerFactory.createConsumer(CONTAINER_ID, "", "-0", KafkaTestUtils.defaultPropertyOverrides()))
				.willReturn(consumer);
			return consumerFactory;
		}

		@SuppressWarnings({ "rawtypes", "unchecked" })
		@Bean
		public Consumer consumer() {
			final Consumer consumer = mock(Consumer.class);
			final TopicPartition topicPartition0 = new TopicPartition("foo", 0);
			final TopicPartition topicPartition1 = new TopicPartition("foo", 1);
			final TopicPartition topicPartition2 = new TopicPartition("foo", 2);
			willAnswer(i -> {
				((ConsumerRebalanceListener) i.getArgument(1)).onPartitionsAssigned(
						Arrays.asList(topicPartition0, topicPartition1, topicPartition2));
				return null;
			}).given(consumer).subscribe(any(Collection.class), any(ConsumerRebalanceListener.class));
			Map<TopicPartition, List<ConsumerRecord>> records1 = new LinkedHashMap<>();
			records1.put(topicPartition0, Arrays.asList(
					new ConsumerRecord("foo", 0, 0L, 0L, TimestampType.NO_TIMESTAMP_TYPE, 0, 0, 0, null, "foo"),
					new ConsumerRecord("foo", 0, 1L, 0L, TimestampType.NO_TIMESTAMP_TYPE, 0, 0, 0, null, "bar")));
			records1.put(topicPartition1, Arrays.asList(
					new ConsumerRecord("foo", 1, 0L, 0L, TimestampType.NO_TIMESTAMP_TYPE, 0, 0, 0, null, "baz"),
					new ConsumerRecord("foo", 1, 1L, 0L, TimestampType.NO_TIMESTAMP_TYPE, 0, 0, 0, null, "qux")));
			records1.put(topicPartition2, Arrays.asList(
					new ConsumerRecord("foo", 2, 0L, 0L, TimestampType.NO_TIMESTAMP_TYPE, 0, 0, 0, null, "fiz"),
					new ConsumerRecord("foo", 2, 1L, 0L, TimestampType.NO_TIMESTAMP_TYPE, 0, 0, 0, null, "buz")));
			Map<TopicPartition, List<ConsumerRecord>> records2 = new LinkedHashMap<>(records1);
			records2.remove(topicPartition0);
			records2.remove(topicPartition2);
			final AtomicInteger which = new AtomicInteger();
			willAnswer(i -> {
				this.pollLatch.countDown();
				switch (which.getAndIncrement()) {
					case 0:
						return new ConsumerRecords(records1);
					case 1:
						return new ConsumerRecords(records2);
					default:
						try {
							Thread.sleep(100);
						}
						catch (@SuppressWarnings("unused") InterruptedException e) {
							Thread.currentThread().interrupt();
						}
						return new ConsumerRecords(Collections.emptyMap());
				}
			}).given(consumer).poll(Duration.ofMillis(ContainerProperties.DEFAULT_POLL_TIMEOUT));
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
			factory.getContainerProperties().setAckMode(AckMode.BATCH);
			factory.getContainerProperties().setTransactionManager(tm());
			factory.setBatchListener(true);
			factory.getContainerProperties().setSubBatchPerPartition(true);
			return factory;
		}

		@SuppressWarnings({ "unchecked", "rawtypes" })
		@Bean
		public KafkaTransactionManager tm() {
			return new KafkaTransactionManager<>(producerFactory());
		}

		@SuppressWarnings("rawtypes")
		@Bean
		public ProducerFactory producerFactory() {
			ProducerFactory pf = mock(ProducerFactory.class);
			given(pf.createProducer(isNull())).willReturn(producer());
			given(pf.transactionCapable()).willReturn(true);
			given(pf.isProducerPerConsumerPartition()).willReturn(true);
			return pf;
		}

		@SuppressWarnings("rawtypes")
		@Bean
		public Producer producer() {
			return mock(Producer.class);
		}

	}

}
