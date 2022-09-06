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
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.BDDMockito.given;
import static org.mockito.BDDMockito.willAnswer;
import static org.mockito.BDDMockito.willThrow;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.Test;
import org.mockito.InOrder;

import org.springframework.kafka.KafkaException;
import org.springframework.util.ReflectionUtils;
import org.springframework.util.backoff.FixedBackOff;

/**
 * @author Gary Russell
 * @since 2.3.7
 *
 */
public class FallbackBatchErrorHandlerTests {

	private int invoked;

	@SuppressWarnings("deprecation")
	@Test
	void recover() {
		this.invoked = 0;
		List<ConsumerRecord<?, ?>> recovered = new ArrayList<>();
		FallbackBatchErrorHandler eh = new FallbackBatchErrorHandler(new FixedBackOff(0L, 3L), (cr, ex) ->  {
			recovered.add(cr);
		});
		Map<TopicPartition, List<ConsumerRecord<Object, Object>>> map = new HashMap<>();
		map.put(new TopicPartition("foo", 0),
				Collections.singletonList(new ConsumerRecord<>("foo", 0, 0L, "foo", "bar")));
		map.put(new TopicPartition("foo", 1),
				Collections.singletonList(new ConsumerRecord<>("foo", 1, 0L, "foo", "bar")));
		ConsumerRecords<?, ?> records = new ConsumerRecords<>(map);
		Consumer<?, ?> consumer = mock(Consumer.class);
		MessageListenerContainer container = mock(MessageListenerContainer.class);
		given(container.isRunning()).willReturn(true);
		eh.handle(new RuntimeException(), records, consumer, container, () -> {
			this.invoked++;
			throw new RuntimeException();
		});
		assertThat(this.invoked).isEqualTo(3);
		assertThat(recovered).hasSize(2);
		verify(consumer).pause(any());
		verify(consumer, times(3)).poll(any());
		verify(consumer).resume(any());
		verify(consumer, times(2)).assignment();
		verifyNoMoreInteractions(consumer);
	}

	@SuppressWarnings("deprecation")
	@Test
	void successOnRetry() {
		this.invoked = 0;
		List<ConsumerRecord<?, ?>> recovered = new ArrayList<>();
		FallbackBatchErrorHandler eh = new FallbackBatchErrorHandler(new FixedBackOff(0L, 3L), (cr, ex) ->  {
			recovered.add(cr);
		});
		Map<TopicPartition, List<ConsumerRecord<Object, Object>>> map = new HashMap<>();
		map.put(new TopicPartition("foo", 0),
				Collections.singletonList(new ConsumerRecord<>("foo", 0, 0L, "foo", "bar")));
		map.put(new TopicPartition("foo", 1),
				Collections.singletonList(new ConsumerRecord<>("foo", 1, 0L, "foo", "bar")));
		ConsumerRecords<?, ?> records = new ConsumerRecords<>(map);
		Consumer<?, ?> consumer = mock(Consumer.class);
		MessageListenerContainer container = mock(MessageListenerContainer.class);
		given(container.isRunning()).willReturn(true);
		eh.handle(new RuntimeException(), records, consumer, container, () -> this.invoked++);
		assertThat(this.invoked).isEqualTo(1);
		assertThat(recovered).hasSize(0);
		verify(consumer).pause(any());
		verify(consumer).poll(any());
		verify(consumer).resume(any());
		verify(consumer, times(2)).assignment();
		verifyNoMoreInteractions(consumer);
	}

	@SuppressWarnings("deprecation")
	@Test
	void recoveryFails() {
		this.invoked = 0;
		List<ConsumerRecord<?, ?>> recovered = new ArrayList<>();
		FallbackBatchErrorHandler eh = new FallbackBatchErrorHandler(new FixedBackOff(0L, 3L), (cr, ex) ->  {
			recovered.add(cr);
			throw new RuntimeException("can't recover");
		});
		Map<TopicPartition, List<ConsumerRecord<Object, Object>>> map = new HashMap<>();
		map.put(new TopicPartition("foo", 0),
				Collections.singletonList(new ConsumerRecord<>("foo", 0, 0L, "foo", "bar")));
		map.put(new TopicPartition("foo", 1),
				Collections.singletonList(new ConsumerRecord<>("foo", 1, 0L, "foo", "bar")));
		ConsumerRecords<?, ?> records = new ConsumerRecords<>(map);
		Consumer<?, ?> consumer = mock(Consumer.class);
		MessageListenerContainer container = mock(MessageListenerContainer.class);
		given(container.isRunning()).willReturn(true);
		assertThatExceptionOfType(RuntimeException.class).isThrownBy(() ->
		eh.handle(new RuntimeException(), records, consumer, container, () -> {
			this.invoked++;
			throw new RuntimeException();
		}));
		assertThat(this.invoked).isEqualTo(3);
		assertThat(recovered).hasSize(1);
		verify(consumer).pause(any());
		verify(consumer, times(3)).poll(any());
		verify(consumer).resume(any());
		verify(consumer, times(2)).assignment();
		verify(consumer).seek(new TopicPartition("foo", 0), 0L);
		verify(consumer).seek(new TopicPartition("foo", 1), 0L);
	}

	@Test
	void exitOnContainerStop() {
		this.invoked = 0;
		List<ConsumerRecord<?, ?>> recovered = new ArrayList<>();
		FallbackBatchErrorHandler eh = new FallbackBatchErrorHandler(new FixedBackOff(0, 99999), (cr, ex) ->  {
			recovered.add(cr);
		});
		Map<TopicPartition, List<ConsumerRecord<Object, Object>>> map = new HashMap<>();
		map.put(new TopicPartition("foo", 0),
				Collections.singletonList(new ConsumerRecord<>("foo", 0, 0L, "foo", "bar")));
		map.put(new TopicPartition("foo", 1),
				Collections.singletonList(new ConsumerRecord<>("foo", 1, 0L, "foo", "bar")));
		ConsumerRecords<?, ?> records = new ConsumerRecords<>(map);
		Consumer<?, ?> consumer = mock(Consumer.class);
		MessageListenerContainer container = mock(MessageListenerContainer.class);
		AtomicBoolean stopped = new AtomicBoolean(true);
		willAnswer(inv -> stopped.get()).given(container).isRunning();
		assertThatExceptionOfType(KafkaException.class).isThrownBy(() ->
			eh.handle(new RuntimeException(), records, consumer, container, () -> {
				this.invoked++;
				stopped.set(false);
				throw new RuntimeException();
			})
		).withMessage("Container stopped during retries");
		assertThat(this.invoked).isEqualTo(1);
	}

	@Test
	void rePauseOnRebalance() {
		this.invoked = 0;
		List<ConsumerRecord<?, ?>> recovered = new ArrayList<>();
		FallbackBatchErrorHandler eh = new FallbackBatchErrorHandler(new FixedBackOff(0L, 1L), (cr, ex) ->  {
			recovered.add(cr);
		});
		Map<TopicPartition, List<ConsumerRecord<Object, Object>>> map = new HashMap<>();
		map.put(new TopicPartition("foo", 0),
				Collections.singletonList(new ConsumerRecord<>("foo", 0, 0L, "foo", "bar")));
		map.put(new TopicPartition("foo", 1),
				Collections.singletonList(new ConsumerRecord<>("foo", 1, 0L, "foo", "bar")));
		ConsumerRecords<?, ?> records = new ConsumerRecords<>(map);
		Consumer<?, ?> consumer = mock(Consumer.class);
		given(consumer.assignment()).willReturn(map.keySet());
		AtomicBoolean pubPauseCalled = new AtomicBoolean();
		willAnswer(inv -> {
			eh.onPartitionsAssigned(consumer, List.of(new TopicPartition("foo", 0), new TopicPartition("foo", 1)),
					() -> pubPauseCalled.set(true));
			return records;
		}).given(consumer).poll(any());
		KafkaMessageListenerContainer<?, ?> container = mock(KafkaMessageListenerContainer.class);
		given(container.isRunning()).willReturn(true);
		eh.handle(new RuntimeException(), records, consumer, container, () -> {
			this.invoked++;
			throw new RuntimeException();
		});
		assertThat(this.invoked).isEqualTo(1);
		assertThat(recovered).hasSize(2);
		InOrder inOrder = inOrder(consumer, container);
		inOrder.verify(consumer).pause(any());
		inOrder.verify(container).publishConsumerPausedEvent(map.keySet(), "For batch retry");
		inOrder.verify(consumer).poll(any());
		inOrder.verify(consumer).pause(any());
		inOrder.verify(consumer).resume(any());
		inOrder.verify(container).publishConsumerResumedEvent(map.keySet());
		verify(consumer, times(3)).assignment();
		verifyNoMoreInteractions(consumer);
		assertThat(pubPauseCalled.get()).isTrue();
	}

	@Test
	void resetRetryingFlagOnExceptionFromRetryBatch() {
		FallbackBatchErrorHandler eh = new FallbackBatchErrorHandler(new FixedBackOff(0L, 1L), (consumerRecord, e) -> { });

		Consumer<?, ?> consumer = mock(Consumer.class);
		// KafkaException could be thrown from SeekToCurrentBatchErrorHandler, but it is hard to mock
		KafkaException exception = new KafkaException("Failed consumer.resume()");
		willThrow(exception).given(consumer).resume(any());

		MessageListenerContainer container = mock(MessageListenerContainer.class);
		given(container.isRunning()).willReturn(true);

		Map<TopicPartition, List<ConsumerRecord<Object, Object>>> map = new HashMap<>();
		map.put(new TopicPartition("foo", 0),
				Collections.singletonList(new ConsumerRecord<>("foo", 0, 0L, "foo", "bar")));
		ConsumerRecords<?, ?> records = new ConsumerRecords<>(map);

		assertThatThrownBy(() -> eh.handle(new RuntimeException(), records, consumer, container, () -> { }))
				.isSameAs(exception);

		assertThat(getRetryingFieldValue(eh))
				.withFailMessage("retrying field was not reset to false")
				.isFalse();
	}

	private boolean getRetryingFieldValue(FallbackBatchErrorHandler errorHandler) {
		Field field = ReflectionUtils.findField(FallbackBatchErrorHandler.class, "retrying");
		ReflectionUtils.makeAccessible(field);
		@SuppressWarnings("unchecked")
		ThreadLocal<Boolean> value = (ThreadLocal<Boolean>) ReflectionUtils.getField(field, errorHandler);
		return value.get();
	}

}
