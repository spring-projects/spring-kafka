/*
 * Copyright 2020-2022 the original author or authors.
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
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.BDDMockito.given;
import static org.mockito.BDDMockito.willAnswer;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

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
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.kafka.clients.consumer.Consumer;
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
import org.springframework.kafka.KafkaException;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.test.utils.KafkaTestUtils;
import org.springframework.messaging.MessagingException;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.junit.jupiter.SpringJUnitConfig;
import org.springframework.util.backoff.FixedBackOff;

/**
 * {@link DefaultErrorHandler} tests for batch listeners.
 * Copied from {@link RecoveringBatchErrorHandlerTests} replacing error handler type.
 *
 * @author Gary Russell
 * @since 2.8
 *
 */
@SpringJUnitConfig
@DirtiesContext
public class DefaultErrorHandlerBatchTests {

	private static final String CONTAINER_ID = "container";

	@SuppressWarnings("rawtypes")
	@Autowired
	private Consumer consumer;

	@Autowired
	private Config config;

	@Autowired
	private KafkaListenerEndpointRegistry registry;

	/*
	 * Deliver 6 records; record "baz" always fails.
	 */
	@SuppressWarnings("unchecked")
	@Test
	void seekAndRecover() throws Exception {
		assertThat(this.config.deliveryLatch.await(10, TimeUnit.SECONDS)).isTrue();
		this.registry.stop();
		assertThat(this.config.closeLatch.await(10, TimeUnit.SECONDS)).isTrue();
		InOrder inOrder = inOrder(this.consumer);
		inOrder.verify(this.consumer).subscribe(any(Collection.class), any(ConsumerRebalanceListener.class));
		inOrder.verify(this.consumer).poll(Duration.ofMillis(ContainerProperties.DEFAULT_POLL_TIMEOUT));
		Map<TopicPartition, OffsetAndMetadata> offsets = new LinkedHashMap<>();
		offsets.put(new TopicPartition("foo", 0), new OffsetAndMetadata(2L));
		inOrder.verify(this.consumer).commitSync(offsets, Duration.ofMinutes(1));
		inOrder.verify(this.consumer).seek(new TopicPartition("foo", 0), 2L);
		inOrder.verify(this.consumer).poll(Duration.ofMillis(ContainerProperties.DEFAULT_POLL_TIMEOUT));
		inOrder.verify(this.consumer).seek(new TopicPartition("foo", 0), 3L);
		offsets = new LinkedHashMap<>();
		offsets.put(new TopicPartition("foo", 0), new OffsetAndMetadata(3L));
		inOrder.verify(this.consumer).commitSync(offsets, Duration.ofMinutes(1));
		inOrder.verify(this.consumer).poll(Duration.ofMillis(ContainerProperties.DEFAULT_POLL_TIMEOUT));
		offsets = new LinkedHashMap<>();
		offsets.put(new TopicPartition("foo", 0), new OffsetAndMetadata(6L));
		inOrder.verify(this.consumer).commitSync(offsets, Duration.ofMinutes(1));
		assertThat(config.received).containsExactly(
				"foo", "bar", "baz", "qux", "fiz", "buz",
				"baz", "qux", "fiz", "buz",
				"qux", "fiz", "buz");
		assertThat(this.config.recovered.value()).isEqualTo("baz");
		assertThat(this.config.listenerFailed.value()).isEqualTo("baz");
		assertThat(this.config.listenerRecovered.value()).isEqualTo("baz");
	}

	@SuppressWarnings({ "unchecked", "rawtypes" })
	@Test
	void outOfRange() {
		Consumer mockConsumer = mock(Consumer.class);
		DefaultErrorHandler beh = new DefaultErrorHandler((cr, ex) -> {
			throw new KafkaException("not recovered");
		}, new FixedBackOff(0, 0));
		TopicPartition tp = new TopicPartition("foo", 0);
		ConsumerRecords<?, ?> records = new ConsumerRecords(Collections.singletonMap(tp,
				Collections.singletonList(
						new ConsumerRecord("foo", 0, 0L, 0L, TimestampType.NO_TIMESTAMP_TYPE, 0, 0, null, "foo",
								new RecordHeaders(), Optional.empty()))));
		assertThatExceptionOfType(KafkaException.class).isThrownBy(() ->
			beh.handleBatch(new ListenerExecutionFailedException("",
					new BatchListenerFailedException("", 2)), records, mockConsumer,
						mock(MessageListenerContainer.class), () -> { }))
				.withMessageStartingWith("Seek to current after exception");
		verify(mockConsumer).seek(tp, 0L);
	}

	@SuppressWarnings({ "unchecked", "rawtypes" })
	@Test
	void wrappedBatchListenerFailedException() {
		Consumer mockConsumer = mock(Consumer.class);
		InOrder inOrder = inOrder(mockConsumer);

		MessageListenerContainer container = mock(MessageListenerContainer.class);
		ContainerProperties containerProperties = mock(ContainerProperties.class);
		given(container.getContainerProperties()).willReturn(containerProperties);
		given(containerProperties.isSyncCommits()).willReturn(true);

		Duration syncCommitTimeout = Duration.ofMillis(1000);
		given(containerProperties.getSyncCommitTimeout()).willReturn(syncCommitTimeout);

		DefaultErrorHandler beh = new DefaultErrorHandler(new FixedBackOff(0, 0));
		TopicPartition tp = new TopicPartition("foo", 0);
		ConsumerRecords<?, ?> records = new ConsumerRecords(Collections.singletonMap(tp,
			Arrays.asList(
				new ConsumerRecord("foo", 0, 0L, 0L, TimestampType.NO_TIMESTAMP_TYPE, 0, 0, null, "foo",
						new RecordHeaders(), Optional.empty()),
				new ConsumerRecord("foo", 0, 1L, 0L, TimestampType.NO_TIMESTAMP_TYPE, 0, 0, null, "bar",
						new RecordHeaders(), Optional.empty()),
				new ConsumerRecord("foo", 0, 2L, 0L, TimestampType.NO_TIMESTAMP_TYPE, 0, 0, null, "baz",
						new RecordHeaders(), Optional.empty()))
		));
		assertThatExceptionOfType(KafkaException.class).isThrownBy(() ->
			beh.handleBatch(new ListenerExecutionFailedException("", new MessagingException("",
					new BatchListenerFailedException("", 1))), records, mockConsumer, container, () -> { })
		);

		Map<TopicPartition, OffsetAndMetadata> offsets = new LinkedHashMap<>();
		offsets.put(tp, new OffsetAndMetadata(1L));
		inOrder.verify(mockConsumer).commitSync(offsets, syncCommitTimeout);

		inOrder.verify(mockConsumer).seek(tp, 2);

		offsets = new LinkedHashMap<>();
		offsets.put(tp, new OffsetAndMetadata(2L));
		inOrder.verify(mockConsumer).commitSync(offsets, syncCommitTimeout);
	}

	@SuppressWarnings({ "unchecked", "rawtypes" })
	@Test
	void missingRecordSoFallback() {
		Consumer mockConsumer = mock(Consumer.class);
		DefaultErrorHandler beh = new DefaultErrorHandler((cr, ex) -> {
			throw new KafkaException("not recovered");
		}, new FixedBackOff(0, 0));
		TopicPartition tp = new TopicPartition("foo", 0);
		ConsumerRecords<?, ?> records = new ConsumerRecords(Collections.singletonMap(tp,
				Collections.singletonList(
						new ConsumerRecord("foo", 0, 0L, 0L, TimestampType.NO_TIMESTAMP_TYPE, 0, 0, null, "foo",
								new RecordHeaders(), Optional.empty()))));
		assertThatExceptionOfType(KafkaException.class).isThrownBy(() ->
			beh.handleBatch(new ListenerExecutionFailedException("",
						new BatchListenerFailedException("",
					new ConsumerRecord("bar", 0, 0L, 0L, TimestampType.NO_TIMESTAMP_TYPE, 0, 0, null, "foo",
							new RecordHeaders(), Optional.empty()))),
					records, mockConsumer, mock(MessageListenerContainer.class), () -> { }))
				.withMessageStartingWith("Seek to current after exception");
		verify(mockConsumer).seek(tp, 0L);
	}

	@SuppressWarnings({ "unchecked", "rawtypes" })
	@Test
	void fallbackListener() {
		Consumer mockConsumer = mock(Consumer.class);
		ConsumerRecordRecoverer recoverer = mock(ConsumerRecordRecoverer.class);
		DefaultErrorHandler beh = new DefaultErrorHandler(recoverer, new FixedBackOff(0, 2));
		RetryListener retryListener = mock(RetryListener.class);
		beh.setRetryListeners(retryListener);
		TopicPartition tp = new TopicPartition("foo", 0);
		ConsumerRecords<?, ?> records = new ConsumerRecords(Collections.singletonMap(tp,
				List.of(new ConsumerRecord("foo", 0, 0L, 0L, TimestampType.NO_TIMESTAMP_TYPE, 0, 0, null, "foo",
								new RecordHeaders(), Optional.empty()),
						new ConsumerRecord("foo", 0, 1L, 0L, TimestampType.NO_TIMESTAMP_TYPE, 0, 0, null, "foo",
								new RecordHeaders(), Optional.empty()))));
		MessageListenerContainer container = mock(MessageListenerContainer.class);
		given(container.isRunning()).willReturn(true);
		beh.handleBatch(new ListenerExecutionFailedException("test"),
				records, mockConsumer, container, () -> {
					throw new ListenerExecutionFailedException("test");
				});
		verify(retryListener).failedDelivery(any(ConsumerRecords.class), any(), eq(1));
		verify(retryListener).failedDelivery(any(ConsumerRecords.class), any(), eq(2));
		verify(retryListener).failedDelivery(any(ConsumerRecords.class), any(), eq(3));
		verify(recoverer, times(2)).accept(any(), any()); // each record in batch
		verify(retryListener).recovered(any(ConsumerRecords.class), any());
	}

	@Configuration
	@EnableKafka
	public static class Config {

		final CountDownLatch deliveryLatch = new CountDownLatch(3);

		final CountDownLatch closeLatch = new CountDownLatch(1);

		final List<String> received = new ArrayList<>();

		volatile ConsumerRecord<?, ?> recovered;

		volatile ConsumerRecord<?, ?> listenerFailed;

		volatile ConsumerRecord<?, ?> listenerRecovered;

		@KafkaListener(id = CONTAINER_ID, topics = "foo")
		public void foo(List<String> in) {
			received.addAll(in);
			this.deliveryLatch.countDown();
			for (int i = 0; i < in.size(); i++) {
				if (in.get(i).equals("baz")) {
					throw new BatchListenerFailedException("fail", i);
				}
			}
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
			final TopicPartition topicPartition = new TopicPartition("foo", 0);
			willAnswer(i -> {
				((ConsumerRebalanceListener) i.getArgument(1)).onPartitionsAssigned(
						Collections.singletonList(topicPartition));
				return null;
			}).given(consumer).subscribe(any(Collection.class), any(ConsumerRebalanceListener.class));
			List<ConsumerRecord> records1 = new ArrayList<>();
			records1.add(new ConsumerRecord("foo", 0, 0L, 0L, TimestampType.NO_TIMESTAMP_TYPE, 0, 0, null, "foo",
					new RecordHeaders(), Optional.empty()));
			records1.add(new ConsumerRecord("foo", 0, 1L, 0L, TimestampType.NO_TIMESTAMP_TYPE, 0, 0, null, "bar",
					new RecordHeaders(), Optional.empty()));
			List<ConsumerRecord> records2 = new ArrayList<>();
			records2.add(new ConsumerRecord("foo", 0, 2L, 0L, TimestampType.NO_TIMESTAMP_TYPE, 0, 0, null, "baz",
					new RecordHeaders(), Optional.empty()));
			List<ConsumerRecord> records3 = new ArrayList<>();
			records3.add(new ConsumerRecord("foo", 0, 3L, 0L, TimestampType.NO_TIMESTAMP_TYPE, 0, 0, null, "qux",
					new RecordHeaders(), Optional.empty()));
			records3.add(new ConsumerRecord("foo", 0, 4L, 0L, TimestampType.NO_TIMESTAMP_TYPE, 0, 0, null, "fiz",
					new RecordHeaders(), Optional.empty()));
			records3.add(new ConsumerRecord("foo", 0, 5L, 0L, TimestampType.NO_TIMESTAMP_TYPE, 0, 0, null, "buz",
					new RecordHeaders(), Optional.empty()));
			List<ConsumerRecord> recordsOne = new ArrayList<>(records1);
			recordsOne.addAll(records2);
			recordsOne.addAll(records3);
			List<ConsumerRecord> recordsTwo = new ArrayList<>(records2);
			recordsTwo.addAll(records3);
			Map<TopicPartition, List<ConsumerRecord>> crs1 = Collections.singletonMap(topicPartition, recordsOne);
			Map<TopicPartition, List<ConsumerRecord>> crs2 = Collections.singletonMap(topicPartition, recordsTwo);
			Map<TopicPartition, List<ConsumerRecord>> crs3 = Collections.singletonMap(topicPartition, records3);
			final AtomicInteger which = new AtomicInteger();
			willAnswer(i -> {
				switch (which.getAndIncrement()) {
					case 0:
						return new ConsumerRecords(crs1);
					case 1:
						return new ConsumerRecords(crs2);
					case 2:
						return new ConsumerRecords(crs3);
					default:
						try {
							Thread.sleep(100);
						}
						catch (InterruptedException e) {
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
			DefaultErrorHandler errorHandler = new DefaultErrorHandler((cr, ex) -> this.recovered = cr,
					new FixedBackOff(0, 1));
			errorHandler.setRetryListeners(new RetryListener() {

				@Override
				public void failedDelivery(ConsumerRecord<?, ?> record, Exception ex, int deliveryAttempt) {
					Config.this.listenerFailed = record;
				}

				@Override
				public void recovered(ConsumerRecord<?, ?> record, Exception ex) {
					Config.this.listenerRecovered = record;
				}

			});
			factory.setCommonErrorHandler(errorHandler);
			factory.setBatchListener(true);
			factory.getContainerProperties().setSubBatchPerPartition(false);
			factory.setMissingTopicsFatal(false);
			return factory;
		}

	}

}
