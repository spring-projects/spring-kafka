/*
 * Copyright 2017-2019 the original author or authors.
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
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.BDDMockito.given;
import static org.mockito.BDDMockito.willAnswer;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;

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
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.listener.ContainerProperties.EOSMode;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.kafka.test.utils.KafkaTestUtils;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.annotation.DirtiesContext.ClassMode;
import org.springframework.test.context.junit.jupiter.SpringJUnitConfig;
import org.springframework.transaction.PlatformTransactionManager;

/**
 * @author Gary Russell
 * @since 2.3.2
 *
 */
@SpringJUnitConfig
@DirtiesContext(classMode = ClassMode.AFTER_EACH_TEST_METHOD)
@EmbeddedKafka(topics = "sbpp")
public class SubBatchPerPartitionTests {

	private static final String CONTAINER_ID = "container";

	@SuppressWarnings("rawtypes")
	@Autowired
	private Consumer consumer;

	@Autowired
	private Config config;

	@Autowired
	private KafkaListenerEndpointRegistry registry;

	@Autowired
	private EmbeddedKafkaBroker broker;

	/*
	 * Deliver 6 records from three partitions, fail on the second record second
	 * partition.
	 */
	@SuppressWarnings("unchecked")
	@Test
	void discardRemainingRecordsFromPollAndSeek() throws Exception {
		this.registry.getListenerContainer(CONTAINER_ID).start();
		assertThat(this.config.deliveryLatch.await(10, TimeUnit.SECONDS)).isTrue();
		assertThat(this.config.commitLatch.await(10, TimeUnit.SECONDS)).isTrue();
		assertThat(this.config.pollLatch.await(10, TimeUnit.SECONDS)).isTrue();
		this.registry.stop();
		assertThat(this.config.closeLatch.await(10, TimeUnit.SECONDS)).isTrue();
		InOrder inOrder = inOrder(this.consumer);
		inOrder.verify(this.consumer).subscribe(any(Collection.class), any(ConsumerRebalanceListener.class));
		inOrder.verify(this.consumer).poll(Duration.ofMillis(ContainerProperties.DEFAULT_POLL_TIMEOUT));
		inOrder.verify(this.consumer, times(3)).commitSync(any(), eq(Duration.ofSeconds(60)));
		inOrder.verify(this.consumer, atLeastOnce()).poll(Duration.ofMillis(ContainerProperties.DEFAULT_POLL_TIMEOUT));
		assertThat(this.config.contents).contains("foo", "bar", "baz", "qux", "fiz", "buz");
		this.registry.stop();
	}

	@SuppressWarnings("unchecked")
	@Test
	void withFilter() throws Exception {
		this.registry.getListenerContainer(CONTAINER_ID + ".filtered").start();
		assertThat(this.config.deliveryLatch.await(10, TimeUnit.SECONDS)).isTrue();
		assertThat(this.config.commitLatch.await(10, TimeUnit.SECONDS)).isTrue();
		assertThat(this.config.pollLatch.await(10, TimeUnit.SECONDS)).isTrue();
		this.registry.stop();
		assertThat(this.config.closeLatch.await(10, TimeUnit.SECONDS)).isTrue();
		InOrder inOrder = inOrder(this.consumer);
		inOrder.verify(this.consumer).subscribe(any(Collection.class), any(ConsumerRebalanceListener.class));
		inOrder.verify(this.consumer).poll(Duration.ofMillis(ContainerProperties.DEFAULT_POLL_TIMEOUT));
		inOrder.verify(this.consumer, times(3)).commitSync(any(), eq(Duration.ofSeconds(60)));
		inOrder.verify(this.consumer, atLeastOnce()).poll(Duration.ofMillis(ContainerProperties.DEFAULT_POLL_TIMEOUT));
		assertThat(this.config.filtered).contains("bar", "qux", "buz");
		this.registry.stop();
	}

	@Test
	void defaults() {
		Map<String, Object> props = KafkaTestUtils.consumerProps("sbpp", "false", this.broker);
		ConsumerFactory<Integer, String> cf = new DefaultKafkaConsumerFactory<>(props);
		ContainerProperties containerProps = new ContainerProperties("sbpp");
		containerProps.setMessageListener(mock(MessageListener.class));
		KafkaMessageListenerContainer<Integer, String> container =
				new KafkaMessageListenerContainer<>(cf, containerProps);
		container.start();
		assertThat(KafkaTestUtils.getPropertyValue(container, "listenerConsumer.subBatchPerPartition"))
				.isEqualTo(Boolean.FALSE);
		container.stop();

		containerProps = new ContainerProperties("sbpp");
		containerProps.setMessageListener(mock(MessageListener.class));
		containerProps.setSubBatchPerPartition(true);
		container = new KafkaMessageListenerContainer<>(cf, containerProps);
		container.start();
		assertThat(KafkaTestUtils.getPropertyValue(container, "listenerConsumer.subBatchPerPartition"))
				.isEqualTo(Boolean.TRUE);
		container.stop();

		containerProps = new ContainerProperties("sbpp");
		containerProps.setMessageListener(mock(MessageListener.class));
		containerProps.setTransactionManager(mock(PlatformTransactionManager.class));
		containerProps.setEosMode(EOSMode.ALPHA);
		container = new KafkaMessageListenerContainer<>(cf, containerProps);
		container.start();
		assertThat(KafkaTestUtils.getPropertyValue(container, "listenerConsumer.subBatchPerPartition"))
				.isEqualTo(Boolean.TRUE);
		container.stop();

		containerProps = new ContainerProperties("sbpp");
		containerProps.setMessageListener(mock(MessageListener.class));
		containerProps.setTransactionManager(mock(PlatformTransactionManager.class));
		containerProps.setEosMode(EOSMode.BETA);
		container = new KafkaMessageListenerContainer<>(cf, containerProps);
		container.start();
		assertThat(KafkaTestUtils.getPropertyValue(container, "listenerConsumer.subBatchPerPartition"))
				.isEqualTo(Boolean.FALSE);
		container.stop();

		// default is BETA
		containerProps = new ContainerProperties("sbpp");
		containerProps.setMessageListener(mock(MessageListener.class));
		containerProps.setTransactionManager(mock(PlatformTransactionManager.class));
		container = new KafkaMessageListenerContainer<>(cf, containerProps);
		container.start();
		assertThat(KafkaTestUtils.getPropertyValue(container, "listenerConsumer.subBatchPerPartition"))
				.isEqualTo(Boolean.FALSE);
		container.stop();
	}

	@Configuration
	@EnableKafka
	public static class Config {

		private final List<String> contents = new ArrayList<>();

		private final List<String> filtered = new ArrayList<>();

		private final CountDownLatch pollLatch = new CountDownLatch(2);

		private final CountDownLatch deliveryLatch = new CountDownLatch(3);

		private final CountDownLatch commitLatch = new CountDownLatch(3);

		private final CountDownLatch closeLatch = new CountDownLatch(1);

		@KafkaListener(id = CONTAINER_ID, topics = "foo", autoStartup = "false")
		public void foo(List<String> in) {
			contents.addAll(in);
			this.deliveryLatch.countDown();
		}

		@KafkaListener(id = CONTAINER_ID + ".filtered", topics = "foo", autoStartup = "false",
				containerFactory = "filteredFactory")
		public void filtered(List<String> in) {
			filtered.addAll(in);
			this.deliveryLatch.countDown();
		}

		@SuppressWarnings({ "rawtypes" })
		@Bean
		public ConsumerFactory consumerFactory() {
			ConsumerFactory consumerFactory = mock(ConsumerFactory.class);
			final Consumer consumer = consumer();
			given(consumerFactory.createConsumer(any(), eq(""), eq("-0"),
					eq(KafkaTestUtils.defaultPropertyOverrides())))
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
			final ThreadLocal<AtomicInteger> which = new ThreadLocal<>();
			willAnswer(i -> {
				this.pollLatch.countDown();
				if (which.get() == null) {
					which.set(new AtomicInteger());
				}
				switch (which.get().getAndIncrement()) {
					case 0:
						return new ConsumerRecords(records1);
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
			factory.setBatchListener(true);
			factory.getContainerProperties().setMissingTopicsFatal(false);
			factory.getContainerProperties().setSubBatchPerPartition(true);
			return factory;
		}

		@SuppressWarnings({ "rawtypes", "unchecked" })
		@Bean
		public ConcurrentKafkaListenerContainerFactory filteredFactory() {
			ConcurrentKafkaListenerContainerFactory factory = new ConcurrentKafkaListenerContainerFactory();
			factory.setConsumerFactory(consumerFactory());
			factory.setBatchListener(true);
			factory.getContainerProperties().setMissingTopicsFatal(false);
			factory.getContainerProperties().setSubBatchPerPartition(true);
			factory.setRecordFilterStrategy(rec -> rec.offset() == 0);
			return factory;
		}

	}

}
