/*
 * Copyright 2019 the original author or authors.
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
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.BDDMockito.given;
import static org.mockito.BDDMockito.willAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.OffsetAndTimestamp;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.KafkaResourceHolder;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.event.ConsumerFailedToStartEvent;
import org.springframework.kafka.event.ConsumerStartedEvent;
import org.springframework.kafka.event.ConsumerStartingEvent;
import org.springframework.kafka.test.utils.KafkaTestUtils;
import org.springframework.kafka.transaction.KafkaAwareTransactionManager;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;
import org.springframework.transaction.support.TransactionSynchronizationManager;

/**
 * @author Gary Russell
 * @since 2.2.4
 *
 */
public class ConcurrentMessageListenerContainerMockTests {

	@SuppressWarnings({ "rawtypes", "unchecked" })
	@Test
	void testThreadStarvation() throws InterruptedException {
		ConsumerFactory consumerFactory = mock(ConsumerFactory.class);
		final Consumer consumer = mock(Consumer.class);
		Set<String> consumerThreads = ConcurrentHashMap.newKeySet();
		CountDownLatch latch = new CountDownLatch(2);
		willAnswer(invocation -> {
			consumerThreads.add(Thread.currentThread().getName());
			latch.countDown();
			Thread.sleep(50);
			return new ConsumerRecords<>(Collections.emptyMap());
		}).given(consumer).poll(any());
		given(consumerFactory.createConsumer(anyString(), anyString(), anyString(),
				eq(KafkaTestUtils.defaultPropertyOverrides())))
						.willReturn(consumer);
		ContainerProperties containerProperties = new ContainerProperties("foo");
		containerProperties.setGroupId("grp");
		containerProperties.setMessageListener((MessageListener) record -> { });
		containerProperties.setMissingTopicsFatal(false);
		ThreadPoolTaskExecutor exec = new ThreadPoolTaskExecutor();
		exec.setCorePoolSize(1);
		exec.afterPropertiesSet();
		containerProperties.setConsumerTaskExecutor(exec);
		containerProperties.setConsumerStartTimout(Duration.ofMillis(50));
		ConcurrentMessageListenerContainer container = new ConcurrentMessageListenerContainer<>(consumerFactory,
				containerProperties);
		container.setConcurrency(2);
		CountDownLatch startedLatch = new CountDownLatch(2);
		CountDownLatch failedLatch = new CountDownLatch(1);
		container.setApplicationEventPublisher(event -> {
			if (event instanceof ConsumerStartingEvent || event instanceof ConsumerStartedEvent) {
				startedLatch.countDown();
			}
			else if (event instanceof ConsumerFailedToStartEvent) {
				failedLatch.countDown();
			}
		});
		container.start();
		assertThat(latch.await(10, TimeUnit.SECONDS)).isTrue();
		assertThat(consumerThreads).hasSize(1);
		assertThat(startedLatch.await(10, TimeUnit.SECONDS)).isTrue();
		assertThat(failedLatch.await(10, TimeUnit.SECONDS)).isTrue();
		container.stop();
		exec.destroy();
	}

	@SuppressWarnings({ "rawtypes", "unchecked" })
	@Test
	void testCorrectContainerForConsumerError() throws InterruptedException {
		ConsumerFactory consumerFactory = mock(ConsumerFactory.class);
		final Consumer consumer = mock(Consumer.class);
		AtomicBoolean first = new AtomicBoolean(true);
		willAnswer(invocation -> {
			if (first.getAndSet(false)) {
				throw new RuntimeException("planned");
			}
			Thread.sleep(100);
			return new ConsumerRecords<>(Collections.emptyMap());
		}).given(consumer).poll(any());
		given(consumerFactory.createConsumer("grp", "", "-0", KafkaTestUtils.defaultPropertyOverrides()))
			.willReturn(consumer);
		ContainerProperties containerProperties = new ContainerProperties("foo");
		containerProperties.setGroupId("grp");
		containerProperties.setMessageListener((MessageListener) record -> { });
		containerProperties.setMissingTopicsFatal(false);
		ConcurrentMessageListenerContainer container = new ConcurrentMessageListenerContainer<>(consumerFactory,
				containerProperties);
		CountDownLatch latch = new CountDownLatch(1);
		AtomicReference<MessageListenerContainer> errorContainer = new AtomicReference<>();
		container.setErrorHandler((ContainerAwareErrorHandler) (thrownException, records, consumer1, ec) -> {
			errorContainer.set(ec);
			latch.countDown();
		});
		container.start();
		assertThat(latch.await(10, TimeUnit.SECONDS)).isTrue();
		assertThat(errorContainer.get()).isSameAs(container);
		container.stop();
	}

	@SuppressWarnings({ "rawtypes", "unchecked" })
	@Test
	@DisplayName("Seek on TL callback when idle")
	void testSyncRelativeSeeks() throws InterruptedException {
		ConsumerFactory consumerFactory = mock(ConsumerFactory.class);
		final Consumer consumer = mock(Consumer.class);
		TestMessageListener1 listener = new TestMessageListener1();
		ConsumerRecords empty = new ConsumerRecords<>(Collections.emptyMap());
		willAnswer(invocation -> {
			Thread.sleep(10);
			return empty;
		}).given(consumer).poll(any());
		TopicPartition tp0 = new TopicPartition("foo", 0);
		TopicPartition tp1 = new TopicPartition("foo", 1);
		TopicPartition tp2 = new TopicPartition("foo", 2);
		TopicPartition tp3 = new TopicPartition("foo", 3);
		List<TopicPartition> assignments = Arrays.asList(tp0, tp1, tp2, tp3);
		willAnswer(invocation -> {
			((ConsumerRebalanceListener) invocation.getArgument(1))
				.onPartitionsAssigned(assignments);
			return null;
		}).given(consumer).subscribe(any(Collection.class), any());
		given(consumer.position(any())).willReturn(100L);
		given(consumer.beginningOffsets(any())).willReturn(assignments.stream()
				.collect(Collectors.toMap(tp -> tp, tp -> 0L)));
		given(consumer.endOffsets(any())).willReturn(assignments.stream()
				.collect(Collectors.toMap(tp -> tp, tp -> 200L)));
		given(consumerFactory.createConsumer("grp", "", "-0", KafkaTestUtils.defaultPropertyOverrides()))
			.willReturn(consumer);
		ContainerProperties containerProperties = new ContainerProperties("foo");
		containerProperties.setGroupId("grp");
		containerProperties.setMessageListener(listener);
		containerProperties.setIdleEventInterval(10L);
		containerProperties.setMissingTopicsFatal(false);
		ConcurrentMessageListenerContainer container = new ConcurrentMessageListenerContainer(consumerFactory,
				containerProperties);
		container.start();
		assertThat(listener.latch.await(10, TimeUnit.SECONDS)).isTrue();
		verify(consumer).seek(tp0, 60L);
		verify(consumer).seek(tp1, 140L);
		verify(consumer).seek(tp2, 160L);
		verify(consumer).seek(tp3, 40L);
		container.stop();
	}

	@SuppressWarnings({ "rawtypes", "unchecked" })
	@Test
	@DisplayName("Seek from activeListener")
	void testAsyncRelativeSeeks() throws InterruptedException {
		ConsumerFactory consumerFactory = mock(ConsumerFactory.class);
		final Consumer consumer = mock(Consumer.class);
		TestMessageListener1 listener = new TestMessageListener1();
		CountDownLatch latch = new CountDownLatch(2);
		TopicPartition tp0 = new TopicPartition("foo", 0);
		TopicPartition tp1 = new TopicPartition("foo", 1);
		TopicPartition tp2 = new TopicPartition("foo", 2);
		TopicPartition tp3 = new TopicPartition("foo", 3);
		List<TopicPartition> assignments = Arrays.asList(tp0, tp1, tp2, tp3);
		Map<TopicPartition, List<ConsumerRecord<String, String>>> recordMap = new HashMap<>();
		recordMap.put(tp0, Collections.singletonList(new ConsumerRecord("foo", 0, 0, null, "bar")));
		recordMap.put(tp1, Collections.singletonList(new ConsumerRecord("foo", 1, 0, null, "bar")));
		recordMap.put(tp2, Collections.singletonList(new ConsumerRecord("foo", 2, 0, null, "bar")));
		recordMap.put(tp3, Collections.singletonList(new ConsumerRecord("foo", 3, 0, null, "bar")));
		ConsumerRecords records = new ConsumerRecords<>(recordMap);
		willAnswer(invocation -> {
			Thread.sleep(10);
			if (listener.latch.getCount() <= 0) {
				latch.countDown();
			}
			return records;
		}).given(consumer).poll(any());
		willAnswer(invocation -> {
			((ConsumerRebalanceListener) invocation.getArgument(1))
				.onPartitionsAssigned(assignments);
			return null;
		}).given(consumer).subscribe(any(Collection.class), any());
		given(consumer.position(any())).willReturn(100L);
		given(consumer.beginningOffsets(any())).willReturn(assignments.stream()
				.collect(Collectors.toMap(tp -> tp, tp -> 0L)));
		given(consumer.endOffsets(any())).willReturn(assignments.stream()
				.collect(Collectors.toMap(tp -> tp, tp -> 200L)));
		given(consumerFactory.createConsumer("grp", "", "-0", KafkaTestUtils.defaultPropertyOverrides()))
			.willReturn(consumer);
		ContainerProperties containerProperties = new ContainerProperties("foo");
		containerProperties.setGroupId("grp");
		containerProperties.setMessageListener(listener);
		containerProperties.setMissingTopicsFatal(false);
		ConcurrentMessageListenerContainer container = new ConcurrentMessageListenerContainer(consumerFactory,
				containerProperties);
		container.start();
		assertThat(latch.await(10, TimeUnit.SECONDS)).isTrue();
		verify(consumer).seek(tp0, 70L);
		verify(consumer).seek(tp1, 130L);
		verify(consumer).seekToEnd(Collections.singletonList(tp2));
		verify(consumer).seek(tp2, 70L); // position - 30 (seekToEnd ignored by mock)
		verify(consumer).seekToBeginning(Collections.singletonList(tp3));
		verify(consumer).seek(tp3, 30L);
		container.stop();
	}

	@SuppressWarnings({ "rawtypes", "unchecked" })
	@Test
	@DisplayName("Seek timestamp TL callback when idle")
	void testSyncTimestampSeeks() throws InterruptedException {
		ConsumerFactory consumerFactory = mock(ConsumerFactory.class);
		final Consumer consumer = mock(Consumer.class);
		TestMessageListener2 listener = new TestMessageListener2();
		ConsumerRecords empty = new ConsumerRecords<>(Collections.emptyMap());
		willAnswer(invocation -> {
			Thread.sleep(10);
			return empty;
		}).given(consumer).poll(any());
		TopicPartition tp0 = new TopicPartition("foo", 0);
		TopicPartition tp1 = new TopicPartition("foo", 1);
		TopicPartition tp2 = new TopicPartition("foo", 2);
		TopicPartition tp3 = new TopicPartition("foo", 3);
		List<TopicPartition> assignments = Arrays.asList(tp0, tp1, tp2, tp3);
		willAnswer(invocation -> {
			((ConsumerRebalanceListener) invocation.getArgument(1))
				.onPartitionsAssigned(assignments);
			return null;
		}).given(consumer).subscribe(any(Collection.class), any());
		given(consumer.position(any())).willReturn(100L);
		given(consumer.beginningOffsets(any())).willReturn(assignments.stream()
				.collect(Collectors.toMap(tp -> tp, tp -> 0L)));
		given(consumer.endOffsets(any())).willReturn(assignments.stream()
				.collect(Collectors.toMap(tp -> tp, tp -> 200L)));
		given(consumer.offsetsForTimes(Collections.singletonMap(tp0, 42L)))
				.willReturn(Collections.singletonMap(tp0, new OffsetAndTimestamp(63L, 42L)));
		given(consumer.offsetsForTimes(assignments.stream().collect(Collectors.toMap(tp -> tp, tp -> 43L))))
				.willReturn(assignments.stream()
						.collect(Collectors.toMap(tp -> tp, tp -> new OffsetAndTimestamp(82L, 43L))));
		given(consumerFactory.createConsumer("grp", "", "-0", KafkaTestUtils.defaultPropertyOverrides()))
			.willReturn(consumer);
		ContainerProperties containerProperties = new ContainerProperties("foo");
		containerProperties.setGroupId("grp");
		containerProperties.setMessageListener(listener);
		containerProperties.setIdleEventInterval(10L);
		containerProperties.setMissingTopicsFatal(false);
		ConcurrentMessageListenerContainer container = new ConcurrentMessageListenerContainer(consumerFactory,
				containerProperties);
		container.start();
		assertThat(listener.latch.await(10, TimeUnit.SECONDS)).isTrue();
		verify(consumer).seek(tp0, 63L);
		verify(consumer).seek(tp0, 82L);
		verify(consumer).seek(tp1, 82L);
		verify(consumer).seek(tp2, 82L);
		verify(consumer).seek(tp3, 82L);
		container.stop();
	}

	@SuppressWarnings({ "rawtypes", "unchecked" })
	@Test
	@DisplayName("Timestamp seek from activeListener")
	void testAsyncTimestampSeeks() throws InterruptedException {
		ConsumerFactory consumerFactory = mock(ConsumerFactory.class);
		final Consumer consumer = mock(Consumer.class);
		TestMessageListener2 listener = new TestMessageListener2();
		CountDownLatch latch = new CountDownLatch(2);
		TopicPartition tp0 = new TopicPartition("foo", 0);
		TopicPartition tp1 = new TopicPartition("foo", 1);
		TopicPartition tp2 = new TopicPartition("foo", 2);
		TopicPartition tp3 = new TopicPartition("foo", 3);
		List<TopicPartition> assignments = Arrays.asList(tp0, tp1, tp2, tp3);
		Map<TopicPartition, List<ConsumerRecord<String, String>>> recordMap = new HashMap<>();
		recordMap.put(tp0, Collections.singletonList(new ConsumerRecord("foo", 0, 0, null, "bar")));
		recordMap.put(tp1, Collections.singletonList(new ConsumerRecord("foo", 1, 0, null, "bar")));
		recordMap.put(tp2, Collections.singletonList(new ConsumerRecord("foo", 2, 0, null, "bar")));
		recordMap.put(tp3, Collections.singletonList(new ConsumerRecord("foo", 3, 0, null, "bar")));
		ConsumerRecords records = new ConsumerRecords<>(recordMap);
		willAnswer(invocation -> {
			Thread.sleep(10);
			if (listener.latch.getCount() <= 0) {
				latch.countDown();
			}
			return records;
		}).given(consumer).poll(any());
		willAnswer(invocation -> {
			((ConsumerRebalanceListener) invocation.getArgument(1))
				.onPartitionsAssigned(assignments);
			return null;
		}).given(consumer).subscribe(any(Collection.class), any());
		given(consumer.position(any())).willReturn(100L);
		given(consumer.beginningOffsets(any())).willReturn(assignments.stream()
				.collect(Collectors.toMap(tp -> tp, tp -> 0L)));
		given(consumer.endOffsets(any())).willReturn(assignments.stream()
				.collect(Collectors.toMap(tp -> tp, tp -> 200L)));
		given(consumer.offsetsForTimes(Collections.singletonMap(tp0, 42L)))
				.willReturn(Collections.singletonMap(tp0, new OffsetAndTimestamp(73L, 42L)));
		given(consumer.offsetsForTimes(any()))
				.willReturn(assignments.stream()
					.collect(Collectors.toMap(tp -> tp,
							tp -> new OffsetAndTimestamp(tp.equals(tp0) ? 73L : 92L, 43L))));
		given(consumerFactory.createConsumer("grp", "", "-0", KafkaTestUtils.defaultPropertyOverrides()))
			.willReturn(consumer);
		ContainerProperties containerProperties = new ContainerProperties("foo");
		containerProperties.setGroupId("grp");
		containerProperties.setMessageListener(listener);
		containerProperties.setMissingTopicsFatal(false);
		ConcurrentMessageListenerContainer container = new ConcurrentMessageListenerContainer(consumerFactory,
				containerProperties);
		container.start();
		assertThat(latch.await(10, TimeUnit.SECONDS)).isTrue();
		verify(consumer).seek(tp0, 73L);
		verify(consumer).seek(tp1, 92L);
		verify(consumer).seek(tp2, 92L);
		verify(consumer).seek(tp3, 92L);
		container.stop();
	}

	@Test
	@DisplayName("Intercept after tx start")
	void testInterceptAfterTx() throws InterruptedException {
		testIntercept(false);
	}

	@Test
	@DisplayName("Intercept before tx start")
	void testInterceptBeforeTx() throws InterruptedException {
		testIntercept(true);
	}

	@SuppressWarnings({ "rawtypes", "unchecked" })
	void testIntercept(boolean beforeTx) throws InterruptedException {
		ConsumerFactory consumerFactory = mock(ConsumerFactory.class);
		final Consumer consumer = mock(Consumer.class);
		TopicPartition tp0 = new TopicPartition("foo", 0);
		ConsumerRecord record = new ConsumerRecord("foo", 0, 0L, "bar", "baz");
		ConsumerRecords records = new ConsumerRecords(Collections.singletonMap(tp0, Collections.singletonList(record)));
		ConsumerRecords empty = new ConsumerRecords<>(Collections.emptyMap());
		AtomicBoolean first = new AtomicBoolean(true);
		willAnswer(invocation -> {
			Thread.sleep(10);
			return first.getAndSet(false) ? records : empty;
		}).given(consumer).poll(any());
		List<TopicPartition> assignments = Arrays.asList(tp0);
		willAnswer(invocation -> {
			((ConsumerRebalanceListener) invocation.getArgument(1))
				.onPartitionsAssigned(assignments);
			return null;
		}).given(consumer).subscribe(any(Collection.class), any());
		given(consumer.position(any())).willReturn(0L);
		given(consumerFactory.createConsumer("grp", "", "-0", KafkaTestUtils.defaultPropertyOverrides()))
			.willReturn(consumer);
		ContainerProperties containerProperties = new ContainerProperties("foo");
		containerProperties.setGroupId("grp");
		containerProperties.setMessageListener((MessageListener) rec -> { });
		containerProperties.setMissingTopicsFatal(false);
		KafkaAwareTransactionManager tm = mock(KafkaAwareTransactionManager.class);
		ProducerFactory pf = mock(ProducerFactory.class);
		given(tm.getProducerFactory()).willReturn(pf);
		Producer producer = mock(Producer.class);
		given(pf.createProducer()).willReturn(producer);
		containerProperties.setTransactionManager(tm);
		List<String> order = new ArrayList<>();
		CountDownLatch latch = new CountDownLatch(3);
		willAnswer(inv -> {
			order.add("tx");
			TransactionSynchronizationManager.bindResource(pf,
					new KafkaResourceHolder<>(producer, Duration.ofSeconds(5L)));
			latch.countDown();
			return null;
		}).given(tm).getTransaction(any());
		ConcurrentMessageListenerContainer container = new ConcurrentMessageListenerContainer(consumerFactory,
				containerProperties);
		container.setRecordInterceptor(rec -> {
			order.add("interceptor");
			latch.countDown();
			return rec;
		});
		container.setInterceptBeforeTx(beforeTx);
		container.start();
		try {
			assertThat(latch.await(10, TimeUnit.SECONDS)).isTrue();
			if (beforeTx) {
				assertThat(order).containsExactly("tx", "interceptor", "tx"); // first one is on assignment
			}
			else {
				assertThat(order).containsExactly("tx", "tx", "interceptor");
			}
		}
		finally {
			container.stop();
		}
	}

	public static class TestMessageListener1 implements MessageListener<String, String>, ConsumerSeekAware {

		private static ThreadLocal<ConsumerSeekCallback> callbacks = new ThreadLocal<>();

		CountDownLatch latch = new CountDownLatch(1);

		@Override
		public void onMessage(ConsumerRecord<String, String> data) {
			ConsumerSeekCallback callback = callbacks.get();
			if (latch.getCount() > 0) {
				callback.seekRelative("foo", 0, -30, true);
				callback.seekRelative("foo", 1, 30, true);
				callback.seekRelative("foo", 2, -30, false);
				callback.seekRelative("foo", 3, 30, false);
			}
			this.latch.countDown();
		}

		@Override
		public void registerSeekCallback(ConsumerSeekCallback callback) {
			callbacks.set(callback);
		}

		@Override
		public void onIdleContainer(Map<TopicPartition, Long> assignments, ConsumerSeekCallback callback) {
			if (latch.getCount() > 0) {
				callback.seekRelative("foo", 0, -40, true);
				callback.seekRelative("foo", 1, 40, true);
				callback.seekRelative("foo", 2, -40, false);
				callback.seekRelative("foo", 3, 40, false);
			}
			this.latch.countDown();
		}

	}

	public static class TestMessageListener2 implements MessageListener<String, String>, ConsumerSeekAware {

		private static ThreadLocal<ConsumerSeekCallback> callbacks = new ThreadLocal<>();

		CountDownLatch latch = new CountDownLatch(1);

		private Collection<TopicPartition> partitions;

		@Override
		public void onMessage(ConsumerRecord<String, String> data) {
			ConsumerSeekCallback callback = callbacks.get();
			if (latch.getCount() > 0) {
				callback.seekToTimestamp("foo", 0, 42L);
				callback.seekToTimestamp(this.partitions.stream()
						.filter(tp -> !tp.equals(new TopicPartition("foo", 0)))
						.collect(Collectors.toList()), 43L);
			}
			this.latch.countDown();
		}

		@Override
		public void registerSeekCallback(ConsumerSeekCallback callback) {
			callbacks.set(callback);
		}

		@Override
		public void onPartitionsAssigned(Map<TopicPartition, Long> assignments, ConsumerSeekCallback callback) {
			this.partitions = assignments.keySet();
		}

		@Override
		public void onIdleContainer(Map<TopicPartition, Long> assignments, ConsumerSeekCallback callback) {
			if (latch.getCount() > 0) {
				callback.seekToTimestamp("foo", 0, 42L);
				callback.seekToTimestamp(assignments.keySet(), 43L);
			}
			this.latch.countDown();
		}

	}

}
