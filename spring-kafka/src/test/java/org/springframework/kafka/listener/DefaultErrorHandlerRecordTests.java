/*
 * Copyright 2019-2022 the original author or authors.
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
import static org.assertj.core.api.Assertions.assertThatIllegalStateException;
import static org.mockito.BDDMockito.given;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.SerializationException;
import org.junit.jupiter.api.Test;
import org.mockito.InOrder;

import org.springframework.kafka.KafkaException;
import org.springframework.kafka.support.converter.ConversionException;
import org.springframework.kafka.support.serializer.DeserializationException;
import org.springframework.util.backoff.FixedBackOff;

/**
 * {@link DefaultErrorHandler} tests for record listeners.
 * Copied from {@link SeekToCurrentErrorHandlerTests} changing the error handler type.
 *
 * @author Gary Russell
 * @since 2.8
 *
 */
public class DefaultErrorHandlerRecordTests {

	@Test
	public void testClassifier() {
		AtomicReference<ConsumerRecord<?, ?>> recovered = new AtomicReference<>();
		AtomicBoolean recovererShouldFail = new AtomicBoolean(false);
		DefaultErrorHandler handler = new DefaultErrorHandler((r, t) -> {
			if (recovererShouldFail.getAndSet(false)) {
				throw new RuntimeException("test recoverer failure");
			}
			recovered.set(r);
		});
		AtomicInteger failedDeliveryAttempt = new AtomicInteger();
		AtomicReference<Exception> recoveryFailureEx = new AtomicReference<>();
		AtomicBoolean isRecovered = new AtomicBoolean();
		handler.setRetryListeners(new RetryListener() {

			@Override
			public void failedDelivery(ConsumerRecord<?, ?> record, Exception ex, int deliveryAttempt) {
				failedDeliveryAttempt.set(deliveryAttempt);
			}

			@Override
			public void recovered(ConsumerRecord<?, ?> record, Exception ex) {
				isRecovered.set(true);
			}

			@Override
			public void recoveryFailed(ConsumerRecord<?, ?> record, Exception original, Exception failure) {
				recoveryFailureEx.set(failure);
			}

		});
		ConsumerRecord<String, String> record1 = new ConsumerRecord<>("foo", 0, 0L, "foo", "bar");
		ConsumerRecord<String, String> record2 = new ConsumerRecord<>("foo", 1, 1L, "foo", "bar");
		List<ConsumerRecord<?, ?>> records = Arrays.asList(record1, record2);
		IllegalStateException illegalState = new IllegalStateException();
		Consumer<?, ?> consumer = mock(Consumer.class);
		assertThatExceptionOfType(KafkaException.class).isThrownBy(() -> handler.handleRemaining(illegalState, records,
					consumer, mock(MessageListenerContainer.class)))
				.withCause(illegalState);
		handler.handleRemaining(new DeserializationException("intended", null, false, illegalState), records,
				consumer, mock(MessageListenerContainer.class));
		assertThat(recovered.get()).isSameAs(record1);
		recovered.set(null);
		handler.handleRemaining(new ConversionException("intended", null), records,
				consumer, mock(MessageListenerContainer.class));
		assertThat(recovered.get()).isSameAs(record1);
		handler.addNotRetryableExceptions(IllegalStateException.class);
		recovered.set(null);
		recovererShouldFail.set(true);
		assertThatExceptionOfType(RuntimeException.class).isThrownBy(() ->
			handler.handleRemaining(illegalState, records, consumer, mock(MessageListenerContainer.class)));
		handler.handleRemaining(illegalState, records, consumer, mock(MessageListenerContainer.class));
		assertThat(recovered.get()).isSameAs(record1);
		InOrder inOrder = inOrder(consumer);
		inOrder.verify(consumer).seek(new TopicPartition("foo", 0), 0L); // not recovered so seek
		inOrder.verify(consumer, times(3)).seek(new TopicPartition("foo", 1), 1L);
		inOrder.verify(consumer).seek(new TopicPartition("foo", 0), 0L); // recovery failed
		inOrder.verify(consumer, times(2)).seek(new TopicPartition("foo", 1), 1L);
		inOrder.verifyNoMoreInteractions();
		assertThat(failedDeliveryAttempt.get()).isEqualTo(1);
		assertThat(recoveryFailureEx.get())
				.isInstanceOf(RuntimeException.class)
				.extracting(ex -> ex.getMessage())
				.isEqualTo("test recoverer failure");
		assertThat(isRecovered.get()).isTrue();
	}

	@Test
	void testSerializationException() {
		DefaultErrorHandler handler = new DefaultErrorHandler();
		SerializationException thrownException = new SerializationException();
		assertThatIllegalStateException().isThrownBy(() -> handler.handleRemaining(thrownException, null, null, null))
				.withCause(thrownException);
	}

	@Test
	void testNotRetryableWithNoRecords() {
		DefaultErrorHandler handler = new DefaultErrorHandler();
		ClassCastException thrownException = new ClassCastException();
		assertThatIllegalStateException().isThrownBy(
					() -> handler.handleRemaining(thrownException, Collections.emptyList(), null, null))
				.withCause(thrownException);
	}

	@Test
	void testEarlyExitBackOff() {
		DefaultErrorHandler handler = new DefaultErrorHandler(new FixedBackOff(1, 10_000));
		Consumer<?, ?> consumer = mock(Consumer.class);
		ConsumerRecord<String, String> record1 = new ConsumerRecord<>("foo", 0, 0L, "foo", "bar");
		ConsumerRecord<String, String> record2 = new ConsumerRecord<>("foo", 1, 1L, "foo", "bar");
		List<ConsumerRecord<?, ?>> records = Arrays.asList(record1, record2);
		IllegalStateException illegalState = new IllegalStateException();
		MessageListenerContainer container = mock(MessageListenerContainer.class);
		given(container.isRunning()).willReturn(false);
		long t1 = System.currentTimeMillis();
		assertThatExceptionOfType(KafkaException.class).isThrownBy(() -> handler.handleRemaining(illegalState,
						records, consumer, container));
		assertThat(System.currentTimeMillis() < t1 + 5_000);
	}

	@Test
	void testNoEarlyExitBackOff() {
		DefaultErrorHandler handler = new DefaultErrorHandler(new FixedBackOff(1, 200));
		Consumer<?, ?> consumer = mock(Consumer.class);
		ConsumerRecord<String, String> record1 = new ConsumerRecord<>("foo", 0, 0L, "foo", "bar");
		ConsumerRecord<String, String> record2 = new ConsumerRecord<>("foo", 1, 1L, "foo", "bar");
		List<ConsumerRecord<?, ?>> records = Arrays.asList(record1, record2);
		IllegalStateException illegalState = new IllegalStateException();
		MessageListenerContainer container = mock(MessageListenerContainer.class);
		given(container.isRunning()).willReturn(true);
		long t1 = System.currentTimeMillis();
		assertThatExceptionOfType(KafkaException.class).isThrownBy(() -> handler.handleRemaining(illegalState,
						records, consumer, container));
		assertThat(System.currentTimeMillis() >= t1 + 200);
	}

}
