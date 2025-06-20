/*
 * Copyright 2022-present the original author or authors.
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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerGroupMetadata;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.header.internals.RecordHeaders;
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
import org.springframework.kafka.test.utils.KafkaTestUtils;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.junit.jupiter.SpringJUnitConfig;
import org.springframework.util.backoff.FixedBackOff;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyMap;
import static org.mockito.BDDMockito.given;
import static org.mockito.BDDMockito.willAnswer;
import static org.mockito.BDDMockito.willReturn;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;

/**
 * @author Gary Russell
 * @author Wang Zhiyang
 * @since 2.9
 *
 */
@SpringJUnitConfig
@DirtiesContext
public class DefaultErrorHandlerNoSeeksBatchListenerTests {

	private static final String CONTAINER_ID = "container";

	private static final String CONTAINER_ID_2 = "container2";

	@SuppressWarnings("rawtypes")
	@Autowired
	private Consumer consumer;

	@Autowired
	private Config config;

	@Autowired
	private KafkaListenerEndpointRegistry registry;

	/*
	 * Deliver 6 records from three partitions, fail on the second record second
	 * partition.
	 */
	@SuppressWarnings("unchecked")
	@Test
	void retriesWithNoSeeksBatchListener() throws Exception {
		assertThat(this.config.deliveryLatch.await(10, TimeUnit.SECONDS)).isTrue();
		assertThat(this.config.pollLatch.await(10, TimeUnit.SECONDS)).isTrue();
		assertThat(this.config.commitLatch.await(10, TimeUnit.SECONDS)).isTrue();
		this.registry.stop();
		assertThat(this.config.closeLatch.await(10, TimeUnit.SECONDS)).isTrue();
		InOrder inOrder = inOrder(this.consumer);
		inOrder.verify(this.consumer).subscribe(any(Collection.class), any(ConsumerRebalanceListener.class));
		inOrder.verify(this.consumer).poll(Duration.ofMillis(ContainerProperties.DEFAULT_POLL_TIMEOUT));
		Map<TopicPartition, OffsetAndMetadata> offsets = new LinkedHashMap<>();
		offsets.put(new TopicPartition("foo", 0), new OffsetAndMetadata(2L));
		offsets.put(new TopicPartition("foo", 1), new OffsetAndMetadata(1L));
		inOrder.verify(this.consumer).commitSync(offsets, Duration.ofSeconds(60));
		inOrder.verify(this.consumer).pause(any());
		inOrder.verify(this.consumer).poll(Duration.ZERO);
		offsets = new LinkedHashMap<>();
		offsets.put(new TopicPartition("foo", 1), new OffsetAndMetadata(2L));
		offsets.put(new TopicPartition("foo", 2), new OffsetAndMetadata(2L));
		inOrder.verify(this.consumer).commitSync(offsets, Duration.ofSeconds(60));
		inOrder.verify(this.consumer).resume(any());
		assertThat(this.config.ehException).isInstanceOf(ListenerExecutionFailedException.class);
		assertThat(((ListenerExecutionFailedException) this.config.ehException).getGroupId()).isEqualTo(CONTAINER_ID);
		assertThat(this.config.contents).contains("foo", "bar", "baz", "qux", "qux", "qux", "fiz", "buz");
	}

	/*
	 * Deliver 6 records from three partitions, fail on the last record
	 */
	@Test
	void retriesWithNoSeeksAndBatchListener2() throws Exception {
		assertThat(this.config.pollLatch2.await(10, TimeUnit.SECONDS)).isTrue();
		assertThat(this.config.fooLatch2.await(10, TimeUnit.SECONDS)).isTrue();
		assertThat(this.config.deliveryCount.get()).isEqualTo(4);
		assertThat(this.config.ehException2).isInstanceOf(ListenerExecutionFailedException.class);
		assertThat(((ListenerExecutionFailedException) this.config.ehException2).getGroupId()).isEqualTo(CONTAINER_ID_2);
	}

	@Configuration
	@EnableKafka
	public static class Config {

		final CountDownLatch pollLatch = new CountDownLatch(1);

		final CountDownLatch pollLatch2 = new CountDownLatch(1);

		final CountDownLatch deliveryLatch = new CountDownLatch(2);

		final CountDownLatch closeLatch = new CountDownLatch(1);

		final CountDownLatch commitLatch = new CountDownLatch(2);

		final AtomicInteger deliveryCount = new AtomicInteger(0);

		final CountDownLatch fooLatch2 = new CountDownLatch(1);

		final AtomicBoolean fail = new AtomicBoolean(true);

		final List<String> contents = new ArrayList<>();

		volatile Exception ehException;

		volatile Exception ehException2;

		@KafkaListener(id = CONTAINER_ID, topics = "foo", containerFactory = "kafkaListenerContainerFactory")
		public void foo(List<String> in) {
			this.contents.addAll(in);
			this.deliveryLatch.countDown();
			if (this.fail.getAndSet(false)) {
				throw new BatchListenerFailedException("test", 3);
			}
		}

		@KafkaListener(id = CONTAINER_ID_2, topics = "foo2", containerFactory = "kafkaListenerContainerFactory2")
		public void foo2(List<String> in) {
			deliveryCount.incrementAndGet();
			int index = 0;
			for (String str : in) {
				if ("qux".equals(str)) {
					throw new BatchListenerFailedException("test", index);
				}
				index++;
			}
			fooLatch2.countDown();
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
			Map<TopicPartition, List<ConsumerRecord>> records1 = createRecords(consumer, "foo");
			final AtomicInteger which = new AtomicInteger();
			willAnswer(i -> {
				this.pollLatch.countDown();
				switch (which.getAndIncrement()) {
					case 0:
						return new ConsumerRecords(records1, Map.of());
					default:
						try {
							Thread.sleep(50);
						}
						catch (InterruptedException e) {
							Thread.currentThread().interrupt();
						}
						return new ConsumerRecords(Collections.emptyMap(), Map.of());
				}
			}).given(consumer).poll(any());
			willAnswer(i -> {
				this.commitLatch.countDown();
				return null;
			}).given(consumer).commitSync(anyMap(), any());
			willAnswer(i -> {
				this.closeLatch.countDown();
				return null;
			}).given(consumer).close();
			willReturn(new ConsumerGroupMetadata(CONTAINER_ID)).given(consumer).groupMetadata();
			return consumer;
		}

		@SuppressWarnings({ "rawtypes", "unchecked" })
		@Bean
		public Consumer consumer2() {
			final Consumer consumer = mock(Consumer.class);
			Map<TopicPartition, List<ConsumerRecord>> records1 = createRecords(consumer, "foo2");
			final TopicPartition topicPartition0 = new TopicPartition("foo2", 0);
			Map<TopicPartition, List<ConsumerRecord>> records2 = new LinkedHashMap<>();
			records2.put(topicPartition0, List.of(
					new ConsumerRecord("foo2", 1, 2L, 0L, TimestampType.NO_TIMESTAMP_TYPE, 0, 0, null, "foo",
							new RecordHeaders(), Optional.empty())));
			final AtomicInteger which = new AtomicInteger();
			willAnswer(i -> {
				this.pollLatch2.countDown();
				switch (which.getAndIncrement()) {
					case 0:
						return new ConsumerRecords(records1, Map.of());
					case 3:  // after backoff
						return new ConsumerRecords(records2, Map.of());
					default:
						try {
							Thread.sleep(0);
						}
						catch (InterruptedException e) {
							Thread.currentThread().interrupt();
						}
						return new ConsumerRecords(Collections.emptyMap(), Map.of());
				}
			}).given(consumer).poll(any());
			willReturn(new ConsumerGroupMetadata(CONTAINER_ID_2)).given(consumer).groupMetadata();
			return consumer;
		}

		@SuppressWarnings({ "rawtypes" })
		@Bean
		public ConsumerFactory consumerFactory2() {
			ConsumerFactory consumerFactory = mock(ConsumerFactory.class);
			final Consumer consumer = consumer2();
			given(consumerFactory.createConsumer(CONTAINER_ID_2, "", "-0", KafkaTestUtils.defaultPropertyOverrides()))
					.willReturn(consumer);
			return consumerFactory;
		}

		@SuppressWarnings({ "rawtypes"})
		@Bean
		public ConcurrentKafkaListenerContainerFactory kafkaListenerContainerFactory() {
			return createConcurrentKafkaListenerContainerFactory(consumerFactory(), CONTAINER_ID);
		}

		@SuppressWarnings({ "rawtypes"})
		@Bean
		public ConcurrentKafkaListenerContainerFactory kafkaListenerContainerFactory2() {
			return createConcurrentKafkaListenerContainerFactory(consumerFactory2(), CONTAINER_ID_2);
		}

		@SuppressWarnings({ "rawtypes", "unchecked" })
		private ConcurrentKafkaListenerContainerFactory createConcurrentKafkaListenerContainerFactory(
				ConsumerFactory consumerFactory, String id) {
			ConcurrentKafkaListenerContainerFactory factory = new ConcurrentKafkaListenerContainerFactory();
			factory.setConsumerFactory(consumerFactory);
			factory.setBatchListener(true);
			factory.getContainerProperties().setPollTimeoutWhilePaused(Duration.ZERO);
			DefaultErrorHandler eh = new DefaultErrorHandler() {

				@Override
				public <K, V> ConsumerRecords<K, V> handleBatchAndReturnRemaining(Exception thrownException,
						ConsumerRecords<?, ?> data, Consumer<?, ?> consumer, MessageListenerContainer container,
						Runnable invokeListener) {

					if (id.equals(CONTAINER_ID)) {
						Config.this.ehException = thrownException;
					}
					else {
						Config.this.ehException2 = thrownException;
					}
					return super.handleBatchAndReturnRemaining(thrownException, data, consumer, container, invokeListener);
				}

			};
			eh.setSeekAfterError(false);
			if (id.equals(CONTAINER_ID_2)) {
				eh.setBackOffFunction((rc, ex) -> new FixedBackOff(0, 2));
			}
			factory.setCommonErrorHandler(eh);
			return factory;
		}

		@SuppressWarnings({ "rawtypes", "unchecked" })
		private Map<TopicPartition, List<ConsumerRecord>> createRecords(Consumer consumer, String topic) {
			final TopicPartition topicPartition0 = new TopicPartition(topic, 0);
			final TopicPartition topicPartition1 = new TopicPartition(topic, 1);
			final TopicPartition topicPartition2 = new TopicPartition(topic, 2);
			willAnswer(i -> {
				((ConsumerRebalanceListener) i.getArgument(1)).onPartitionsAssigned(
						Collections.singletonList(topicPartition1));
				return null;
			}).given(consumer).subscribe(any(Collection.class), any(ConsumerRebalanceListener.class));
			Map<TopicPartition, List<ConsumerRecord>> records1 = new LinkedHashMap<>();
			records1.put(topicPartition0, Arrays.asList(
					new ConsumerRecord(topic, 0, 0L, 0L, TimestampType.NO_TIMESTAMP_TYPE, 0, 0, null, "foo",
							new RecordHeaders(), Optional.empty()),
					new ConsumerRecord(topic, 0, 1L, 0L, TimestampType.NO_TIMESTAMP_TYPE, 0, 0, null, "bar",
							new RecordHeaders(), Optional.empty())));
			records1.put(topicPartition1, Arrays.asList(
					new ConsumerRecord(topic, 1, 0L, 0L, TimestampType.NO_TIMESTAMP_TYPE, 0, 0, null, "baz",
							new RecordHeaders(), Optional.empty()),
					new ConsumerRecord(topic, 1, 1L, 0L, TimestampType.NO_TIMESTAMP_TYPE, 0, 0, null, "qux",
							new RecordHeaders(), Optional.empty())));
			records1.put(topicPartition2, Arrays.asList(
					new ConsumerRecord(topic, 2, 0L, 0L, TimestampType.NO_TIMESTAMP_TYPE, 0, 0, null, "fiz",
							new RecordHeaders(), Optional.empty()),
					new ConsumerRecord(topic, 2, 1L, 0L, TimestampType.NO_TIMESTAMP_TYPE, 0, 0, null, "buz",
							new RecordHeaders(), Optional.empty())));
			return records1;
		}

	}

}
