/*
 * Copyright 2026-present the original author or authors.
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

import java.lang.reflect.Method;
import java.time.Duration;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import org.aopalliance.intercept.MethodInterceptor;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.MockProducer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import org.springframework.aop.framework.ProxyFactory;
import org.springframework.kafka.KafkaException;
import org.springframework.kafka.annotation.KafkaListenerAnnotationBeanPostProcessor;
import org.springframework.kafka.listener.adapter.AsyncRepliesAware;
import org.springframework.kafka.listener.adapter.BatchMessagingMessageListenerAdapter;
import org.springframework.kafka.listener.adapter.FilteringBatchMessageListenerAdapter;
import org.springframework.kafka.listener.adapter.HandlerAdapter;
import org.springframework.kafka.mock.MockProducerFactory;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.kafka.transaction.KafkaTransactionManager;
import org.springframework.transaction.support.AbstractPlatformTransactionManager;
import org.springframework.transaction.support.TransactionSynchronizationManager;
import org.springframework.transaction.support.TransactionTemplate;
import org.springframework.util.backoff.FixedBackOff;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.BDDMockito.given;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

/**
 * @author Goutam Adwant
 */
class BatchToRecordFallbackAdviceTests {

	@Test
	void identifiesFailedRecord() {
		List<ConsumerRecord<String, String>> records = List.of(record(0), record(1), record(2));
		List<List<ConsumerRecord<String, String>>> calls = new ArrayList<>();
		BatchMessageListener<String, String> listener = batch -> {
			calls.add(batch);
			if (batch.contains(records.get(1))) {
				throw new IllegalStateException("Failed to process record");
			}
		};
		assertThatExceptionOfType(BatchListenerFailedException.class)
				.isThrownBy(() -> advised(listener).onMessage(records))
				.satisfies(ex -> assertThat(ex.getRecord()).isSameAs(records.get(1)));
		assertThat(calls).containsExactly(records, List.of(records.get(0)), List.of(records.get(1)));
	}

	@Test
	void successfulBatchIsInvokedOnlyOnce() {
		AtomicInteger calls = new AtomicInteger();
		advised(batch -> calls.incrementAndGet()).onMessage(List.of(record(0), record(1)));
		assertThat(calls).hasValue(1);
	}

	@Test
	void transientBatchFailureFallsBackToAllRecords() {
		List<List<ConsumerRecord<String, String>>> calls = new ArrayList<>();
		List<ConsumerRecord<String, String>> records = List.of(record(0), record(1));
		advised(batch -> {
			calls.add(batch);
			if (batch.size() > 1) {
				throw new IllegalStateException("Batch failed");
			}
		}).onMessage(records);
		assertThat(calls).containsExactly(records, List.of(records.get(0)), List.of(records.get(1)));
	}

	@Test
	void singleRecordFailureDoesNotInvokeListenerAgain() {
		ConsumerRecord<String, String> record = record(0);
		AtomicInteger calls = new AtomicInteger();
		assertThatExceptionOfType(BatchListenerFailedException.class).isThrownBy(() -> advised(batch -> {
			calls.incrementAndGet();
			throw new IllegalStateException("Record failed");
		}).onMessage(List.of(record))).satisfies(ex -> assertThat(ex.getRecord()).isSameAs(record));
		assertThat(calls).hasValue(1);
	}

	@Test
	void knownFailureIsPropagatedWithoutFallback() {
		assertNoFallback(new BatchListenerFailedException("Known failure", 1));
		assertNoFallback(new ListenerExecutionFailedException("Listener failed",
				new BatchListenerFailedException("Known failure", 1)));
	}

	@Test
	void errorsArePropagatedWithoutFallback() {
		assertNoFallback(new AssertionError("Fatal failure"));
		assertNoFallback(new ListenerExecutionFailedException("Listener failed", new AssertionError("Fatal failure")));
	}

	@Test
	void interruptedFailureIsPropagatedWithoutFallback() {
		assertNoFallback(new ListenerExecutionFailedException("Interrupted", new InterruptedException()));
	}

	@Test
	void interruptedThreadDoesNotEnterFallback() {
		Thread.currentThread().interrupt();
		try {
			assertNoFallback(new IllegalStateException("Consumer is stopping"));
			assertThat(Thread.currentThread().isInterrupted()).isTrue();
		}
		finally {
			Thread.interrupted();
		}
	}

	@Test
	void emptyBatchFailureIsPropagated() {
		IllegalStateException failure = new IllegalStateException("Empty batch");
		assertThatThrownBy(() -> advised(batch -> {
			throw failure;
		}).onMessage(List.of())).isSameAs(failure);
	}

	@Test
	void downstreamAdviceRunsForEveryInvocationAndArgumentsAreRestored() {
		List<Integer> sizes = new ArrayList<>();
		List<Integer> outerSizes = new ArrayList<>();
		BatchMessageListener<String, String> listener = batch -> {
			if (batch.size() > 1) {
				throw new IllegalStateException("Batch failed");
			}
		};
		ContainerProperties properties = new ContainerProperties("orders");
		properties.setAdviceChain((MethodInterceptor) invocation -> {
			Object result = invocation.proceed();
			outerSizes.add(((List<?>) invocation.getArguments()[0]).size());
			return result;
		}, new BatchToRecordFallbackAdvice(), (MethodInterceptor) invocation -> {
			if ("onMessage".equals(invocation.getMethod().getName())) {
				sizes.add(((List<?>) invocation.getArguments()[0]).size());
			}
			return invocation.proceed();
		});
		properties.setMessageListener(listener);
		@SuppressWarnings("unchecked")
		BatchMessageListener<String, String> proxy =
				(BatchMessageListener<String, String>) properties.getMessageListener();
		proxy.onMessage(List.of(record(0), record(1)));
		assertThat(sizes).containsExactly(2, 1, 1);
		assertThat(outerSizes).containsExactly(2);
	}

	@Test
	void manualAcknowledgmentIsNotReusedForSingletons() {
		AtomicInteger calls = new AtomicInteger();
		Acknowledgment acknowledgment = mock(Acknowledgment.class);
		IllegalStateException failure = new IllegalStateException("Manual batch failed");
		BatchAcknowledgingMessageListener<String, String> listener = (batch, ack) -> {
			calls.incrementAndGet();
			assertThat(ack).isSameAs(acknowledgment);
			throw failure;
		};
		assertThatThrownBy(() -> advised(listener).onMessage(List.of(record(0), record(1)), acknowledgment))
				.isSameAs(failure);
		assertThat(calls).hasValue(1);
	}

	@ParameterizedTest
	@ValueSource(booleans = { false, true })
	void activeTransactionIsNotRetried(boolean synchronize) {
		MockProducer<String, String> producer = new MockProducer<>(true, null, new StringSerializer(), new StringSerializer());
		producer.initTransactions();
		KafkaTransactionManager<String, String> manager =
				new KafkaTransactionManager<>(new MockProducerFactory<>((tx, id) -> producer, null));
		if (synchronize) {
			manager.setTransactionSynchronization(AbstractPlatformTransactionManager.SYNCHRONIZATION_ALWAYS);
		}
		AtomicInteger calls = new AtomicInteger();
		IllegalStateException failure = new IllegalStateException("Transaction failed");
		assertThatThrownBy(() -> new TransactionTemplate(manager).executeWithoutResult(status ->
				advised(batch -> {
					calls.incrementAndGet();
					assertThat(TransactionSynchronizationManager.getResourceMap()).isNotEmpty();
					throw failure;
				}).onMessage(List.of(record(0), record(1)))))
				.isSameAs(failure);
		assertThat(calls).hasValue(1);
		assertThat(producer.transactionAborted()).isTrue();
		assertThat(TransactionSynchronizationManager.isActualTransactionActive()).isFalse();
	}

	@Test
	void boundResourcesDisableFallbackConservatively() {
		Object key = new Object();
		TransactionSynchronizationManager.bindResource(key, new Object());
		try {
			assertNoFallback(new IllegalStateException("Batch failed with bound resource"));
		}
		finally {
			TransactionSynchronizationManager.unbindResource(key);
		}
	}

	@Test
	void asynchronousDelegateRetainsItsBehaviorThroughFilters() {
		AsyncListener target = new AsyncListener();
		BatchMessageListener<String, String> listener = new FilteringBatchMessageListenerAdapter<>(
				new FilteringBatchMessageListenerAdapter<>(target, record -> false), record -> false);
		assertThatThrownBy(() -> advised(listener).onMessage(new ArrayList<>(List.of(record(0), record(1))), null, null))
				.isSameAs(target.failure);
		assertThat(target.calls).hasValue(1);
	}

	@SuppressWarnings("unchecked")
	@Test
	void pollResultListenerRetainsItsBehavior() {
		IllegalStateException failure = new IllegalStateException("Poll result failed");
		BatchMessageListener<String, String> listener = new BatchMessageListener<>() {

			@Override
			public void onMessage(List<ConsumerRecord<String, String>> data) {
				throw new AssertionError("List invocation not expected");
			}

			@Override
			public void onMessage(ConsumerRecords<String, String> records, Acknowledgment ack,
					Consumer<String, String> consumer) {

				throw failure;
			}

		};
		assertThatThrownBy(() -> advised(listener).onMessage(new ConsumerRecords<>(Map.of(), Map.of()), null,
				mock(Consumer.class))).isSameAs(failure);
	}

	@Test
	void filteringCanModifySingletonLists() {
		List<Long> processed = new ArrayList<>();
		AtomicInteger filters = new AtomicInteger();
		BatchMessageListener<String, String> listener = batch -> {
			if (batch.size() > 1) {
				throw new IllegalStateException("Batch failed");
			}
			batch.forEach(record -> processed.add(record.offset()));
		};
		FilteringBatchMessageListenerAdapter<String, String> filtering =
				new FilteringBatchMessageListenerAdapter<>(listener, record -> {
					filters.incrementAndGet();
					return record.offset() == 1 && filters.get() > 2;
				});
		advised(filtering).onMessage(new ArrayList<>(List.of(record(0), record(1))), null, null);
		assertThat(processed).containsExactly(0L);
	}

	@Test
	void convertedListenerIdentifiesRecordForRecoveryAndSeeksRemainder() throws Exception {
		PayloadListener target = new PayloadListener();
		Method method = PayloadListener.class.getDeclaredMethod("listen", List.class);
		KafkaListenerAnnotationBeanPostProcessor<String, String> bpp = new KafkaListenerAnnotationBeanPostProcessor<>();
		BatchMessagingMessageListenerAdapter<String, String> adapter =
				new BatchMessagingMessageListenerAdapter<>(target, method);
		adapter.setHandlerMethod(new HandlerAdapter(
				bpp.getMessageHandlerMethodFactory().createInvocableHandlerMethod(target, method)));
		List<ConsumerRecord<String, String>> records = List.of(
				new ConsumerRecord<>("orders", 0, 0, "order", "accepted"),
				new ConsumerRecord<>("orders", 1, 5, "order", "rejected"),
				new ConsumerRecord<>("orders", 1, 6, "order", "remaining"));
		List<ConsumerRecord<?, ?>> recovered = new ArrayList<>();
		DefaultErrorHandler errorHandler = new DefaultErrorHandler((record, ex) -> recovered.add(record),
				new FixedBackOff(0, 0));
		Consumer<?, ?> consumer = mock(Consumer.class);
		MessageListenerContainer container = mock(MessageListenerContainer.class);
		ContainerProperties properties = new ContainerProperties("orders");
		properties.setSyncCommitTimeout(Duration.ofSeconds(60));
		given(container.getContainerProperties()).willReturn(properties);
		TopicPartition partition = new TopicPartition("orders", 0);
		TopicPartition failedPartition = new TopicPartition("orders", 1);
		Map<TopicPartition, List<ConsumerRecord<String, String>>> polled = new LinkedHashMap<>();
		polled.put(partition, records.subList(0, 1));
		polled.put(failedPartition, records.subList(1, 3));
		assertThatExceptionOfType(BatchListenerFailedException.class)
				.isThrownBy(() -> advised(adapter).onMessage(records, null, null))
				.satisfies(failure -> {
					assertThat(failure.getRecord()).isSameAs(records.get(1));
					assertThatThrownBy(() -> errorHandler.handleBatch(failure,
							new ConsumerRecords<>(polled, Map.of()), consumer, container,
							() -> { })).isInstanceOf(KafkaException.class);
				});
		assertThat(target.calls).containsExactly(List.of("accepted", "rejected", "remaining"),
				List.of("accepted"), List.of("rejected"));
		assertThat(recovered).containsExactly(records.get(1));
		verify(consumer).commitSync(Map.of(partition, new OffsetAndMetadata(1)), Duration.ofSeconds(60));
		verify(consumer).commitSync(Map.of(failedPartition, new OffsetAndMetadata(6)), Duration.ofSeconds(60));
		verify(consumer).seek(failedPartition, 6);
	}

	private void assertNoFallback(Throwable failure) {
		AtomicInteger calls = new AtomicInteger();
		assertThatThrownBy(() -> advised(batch -> {
			calls.incrementAndGet();
			if (failure instanceof Error error) {
				throw error;
			}
			throw (RuntimeException) failure;
		}).onMessage(List.of(record(0), record(1)))).isSameAs(failure);
		assertThat(calls).hasValue(1);
	}

	@SuppressWarnings("unchecked")
	private BatchMessageListener<String, String> advised(BatchMessageListener<String, String> listener) {
		ProxyFactory factory = new ProxyFactory(listener);
		factory.addAdvice(new BatchToRecordFallbackAdvice());
		return (BatchMessageListener<String, String>) factory.getProxy();
	}

	private ConsumerRecord<String, String> record(long offset) {
		return new ConsumerRecord<>("orders", 0, offset, "order", "value");
	}

	static class AsyncListener implements BatchMessageListener<String, String>, AsyncRepliesAware {

		final AtomicInteger calls = new AtomicInteger();

		final IllegalStateException failure = new IllegalStateException("Async invocation failed");

		@Override
		public boolean isAsyncReplies() {
			return true;
		}

		@Override
		public void onMessage(List<ConsumerRecord<String, String>> data) {
			this.calls.incrementAndGet();
			throw this.failure;
		}

	}

	static class PayloadListener {

		final List<List<String>> calls = new ArrayList<>();

		void listen(List<String> values) {
			this.calls.add(values);
			if (values.contains("rejected")) {
				throw new IllegalStateException("Failed to process order");
			}
		}

	}

}
