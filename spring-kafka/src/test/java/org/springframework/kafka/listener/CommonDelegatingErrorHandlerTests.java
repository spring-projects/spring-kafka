/*
 * Copyright 2021-present the original author or authors.
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

import java.io.IOException;
import java.time.Duration;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import org.springframework.kafka.KafkaException;
import org.springframework.kafka.core.KafkaProducerException;
import org.springframework.kafka.test.utils.KafkaTestUtils;
import org.springframework.util.backoff.FixedBackOff;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.same;
import static org.mockito.BDDMockito.given;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;

/**
 * Tests for {@link CommonDelegatingErrorHandler}.
 *
 * @author Gary Russell
 * @author Adrian Chlebosz
 * @author Antonin Arquey
 * @author Dan Blackney
 * @author Burak Kalayci
 * @author Cobi Eun
 * @since 2.8
 *
 */
public class CommonDelegatingErrorHandlerTests {

	private static final TopicPartition TP = new TopicPartition("topic", 0);

	@Test
	void handleBatchAndReturnRemainingShouldReturnRemainingFromDefaultHandlerWhenNoDelegateMatches() {
		DefaultErrorHandler fallback = spy(noSeekErrorHandler());
		DefaultErrorHandler other = spy(noSeekErrorHandler());
		CommonDelegatingErrorHandler delegating = new CommonDelegatingErrorHandler(fallback);
		delegating.addDelegate(IllegalStateException.class, other);
		Consumer<?, ?> consumer = mock(Consumer.class);
		MessageListenerContainer container = mock(MessageListenerContainer.class);
		ContainerProperties properties = new ContainerProperties("topic");
		properties.setSyncCommitTimeout(Duration.ofSeconds(1));
		given(container.getContainerProperties()).willReturn(properties);
		given(container.isRunning()).willReturn(true);
		BatchListenerFailedException exception = new BatchListenerFailedException("failure", 1);
		ConsumerRecords<String, String> data = threeRecords();
		Runnable invokeListener = () -> { };

		try {
			ConsumerRecords<String, String> remaining = delegating.handleBatchAndReturnRemaining(
					exception, data, consumer, container, invokeListener);
			assertThat(remaining.count()).isEqualTo(2);
			assertThat(remaining.records(TP)).extracting(ConsumerRecord::offset).containsExactly(1L, 2L);
			verify(fallback).handleBatchAndReturnRemaining(exception, data, consumer, container, invokeListener);
			verify(other, never()).handleBatchAndReturnRemaining(any(), any(), any(), any(), any());
			verify(consumer, never()).seek(any(), anyLong());
			verify(consumer, never()).seek(any(), any(OffsetAndMetadata.class));
			ArgumentCaptor<Map<TopicPartition, OffsetAndMetadata>> offsets = ArgumentCaptor.captor();
			verify(consumer).commitSync(offsets.capture(), eq(Duration.ofSeconds(1)));
			assertThat(offsets.getValue().get(TP).offset()).isEqualTo(1L);
		}
		finally {
			delegating.clearThreadState();
		}
	}

	@Test
	void handleBatchAndReturnRemainingShouldReturnRemainingFromMatchedDelegate() {
		DefaultErrorHandler fallback = spy(noSeekErrorHandler());
		DefaultErrorHandler selected = spy(noSeekErrorHandler());
		CommonDelegatingErrorHandler delegating = new CommonDelegatingErrorHandler(fallback);
		delegating.addDelegate(BatchListenerFailedException.class, selected);
		Consumer<?, ?> consumer = mock(Consumer.class);
		MessageListenerContainer container = mock(MessageListenerContainer.class);
		ContainerProperties properties = new ContainerProperties("topic");
		properties.setSyncCommitTimeout(Duration.ofSeconds(1));
		given(container.getContainerProperties()).willReturn(properties);
		given(container.isRunning()).willReturn(true);
		BatchListenerFailedException exception = new BatchListenerFailedException("failure", 1);
		ConsumerRecords<String, String> data = threeRecords();
		Runnable invokeListener = () -> { };

		try {
			ConsumerRecords<String, String> remaining = delegating.handleBatchAndReturnRemaining(
					exception, data, consumer, container, invokeListener);
			assertThat(remaining.count()).isEqualTo(2);
			assertThat(remaining.records(TP)).extracting(ConsumerRecord::offset).containsExactly(1L, 2L);
			verify(selected).handleBatchAndReturnRemaining(exception, data, consumer, container, invokeListener);
			verify(fallback, never()).handleBatchAndReturnRemaining(any(), any(), any(), any(), any());
			verify(consumer, never()).seek(any(), anyLong());
			verify(consumer, never()).seek(any(), any(OffsetAndMetadata.class));
			ArgumentCaptor<Map<TopicPartition, OffsetAndMetadata>> offsets = ArgumentCaptor.captor();
			verify(consumer).commitSync(offsets.capture(), eq(Duration.ofSeconds(1)));
			assertThat(offsets.getValue().get(TP).offset()).isEqualTo(1L);
		}
		finally {
			delegating.clearThreadState();
		}
	}

	@Test
	void handleBatchAndReturnRemainingShouldRouteLikeHandleBatchAndReturnResultAsIs() {
		CommonErrorHandler def = mock(CommonErrorHandler.class);
		CommonErrorHandler one = mock(CommonErrorHandler.class);
		CommonErrorHandler two = mock(CommonErrorHandler.class);
		CommonErrorHandler three = mock(CommonErrorHandler.class);
		CommonDelegatingErrorHandler delegating = new CommonDelegatingErrorHandler(def);
		delegating.setErrorHandlers(Map.of(IllegalStateException.class, one, IllegalArgumentException.class, two));
		delegating.addDelegate(RuntimeException.class, three);
		ConsumerRecords<String, String> data = threeRecords();
		Consumer<?, ?> consumer = mock(Consumer.class);
		MessageListenerContainer container = mock(MessageListenerContainer.class);
		Runnable invokeListener = mock(Runnable.class);
		List<Exception> exceptions = List.of(wrap(new IOException()), wrap(new KafkaException("test")),
				wrap(new IllegalArgumentException()), wrap(new IllegalStateException()));
		List<CommonErrorHandler> handlers = List.of(def, three, two, one);
		List<ConsumerRecords<String, String>> results = List.of(threeRecords(), threeRecords(),
				threeRecords(), threeRecords());

		for (int i = 0; i < exceptions.size(); i++) {
			Exception exception = exceptions.get(i);
			CommonErrorHandler selected = handlers.get(i);
			given(selected.<String, String>handleBatchAndReturnRemaining(
					exception, data, consumer, container, invokeListener)).willReturn(results.get(i));

			assertThat(delegating.handleBatchAndReturnRemaining(exception, data, consumer, container, invokeListener))
					.isSameAs(results.get(i));
			verify(selected).handleBatchAndReturnRemaining(same(exception), same(data), same(consumer), same(container),
					same(invokeListener));
			for (CommonErrorHandler handler : handlers) {
				if (handler != selected) {
					verify(handler, never()).handleBatchAndReturnRemaining(same(exception), any(), any(), any(), any());
				}
			}
		}
	}

	@Test
	void testHandleRemainingDelegates() {
		var def = mock(CommonErrorHandler.class);
		var one = mock(CommonErrorHandler.class);
		var two = mock(CommonErrorHandler.class);
		var three = mock(CommonErrorHandler.class);
		var eh = new CommonDelegatingErrorHandler(def);
		eh.setErrorHandlers(Map.of(IllegalStateException.class, one, IllegalArgumentException.class, two));
		eh.addDelegate(RuntimeException.class, three);

		eh.handleRemaining(wrap(new IOException()), Collections.emptyList(), mock(Consumer.class),
				mock(MessageListenerContainer.class));
		verify(def).handleRemaining(any(), any(), any(), any());
		eh.handleRemaining(wrap(new KafkaException("test")), Collections.emptyList(), mock(Consumer.class),
				mock(MessageListenerContainer.class));
		verify(three).handleRemaining(any(), any(), any(), any());
		eh.handleRemaining(wrap(new IllegalArgumentException()), Collections.emptyList(), mock(Consumer.class),
				mock(MessageListenerContainer.class));
		verify(two).handleRemaining(any(), any(), any(), any());
		eh.handleRemaining(wrap(new IllegalStateException()), Collections.emptyList(), mock(Consumer.class),
				mock(MessageListenerContainer.class));
		verify(one).handleRemaining(any(), any(), any(), any());
	}

	@Test
	void testHandleBatchDelegates() {
		var def = mock(CommonErrorHandler.class);
		var one = mock(CommonErrorHandler.class);
		var two = mock(CommonErrorHandler.class);
		var three = mock(CommonErrorHandler.class);
		var eh = new CommonDelegatingErrorHandler(def);
		eh.setErrorHandlers(Map.of(IllegalStateException.class, one, IllegalArgumentException.class, two));
		eh.addDelegate(RuntimeException.class, three);

		eh.handleBatch(wrap(new IOException()), mock(ConsumerRecords.class), mock(Consumer.class),
				mock(MessageListenerContainer.class), mock(Runnable.class));
		verify(def).handleBatch(any(), any(), any(), any(), any());
		eh.handleBatch(wrap(new KafkaException("test")), mock(ConsumerRecords.class), mock(Consumer.class),
				mock(MessageListenerContainer.class), mock(Runnable.class));
		verify(three).handleBatch(any(), any(), any(), any(), any());
		eh.handleBatch(wrap(new IllegalArgumentException()), mock(ConsumerRecords.class), mock(Consumer.class),
				mock(MessageListenerContainer.class), mock(Runnable.class));
		verify(two).handleBatch(any(), any(), any(), any(), any());
		eh.handleBatch(wrap(new IllegalStateException()), mock(ConsumerRecords.class), mock(Consumer.class),
				mock(MessageListenerContainer.class), mock(Runnable.class));
		verify(one).handleBatch(any(), any(), any(), any(), any());
	}

	@Test
	void testHandleOtherExceptionDelegates() {
		var def = mock(CommonErrorHandler.class);
		var one = mock(CommonErrorHandler.class);
		var two = mock(CommonErrorHandler.class);
		var three = mock(CommonErrorHandler.class);
		var eh = new CommonDelegatingErrorHandler(def);
		eh.setErrorHandlers(Map.of(IllegalStateException.class, one, IllegalArgumentException.class, two));
		eh.addDelegate(RuntimeException.class, three);

		eh.handleOtherException(wrap(new IOException()), mock(Consumer.class),
				mock(MessageListenerContainer.class), true);
		verify(def).handleOtherException(any(), any(), any(), anyBoolean());
		eh.handleOtherException(wrap(new KafkaException("test")), mock(Consumer.class),
				mock(MessageListenerContainer.class), true);
		verify(three).handleOtherException(any(), any(), any(), anyBoolean());
		eh.handleOtherException(wrap(new IllegalArgumentException()), mock(Consumer.class),
				mock(MessageListenerContainer.class), true);
		verify(two).handleOtherException(any(), any(), any(), anyBoolean());
		eh.handleOtherException(wrap(new IllegalStateException()), mock(Consumer.class),
				mock(MessageListenerContainer.class), true);
		verify(one).handleOtherException(any(), any(), any(), anyBoolean());
	}

	@Test
	void testHandleOneDelegates() {
		var def = mock(CommonErrorHandler.class);
		var one = mock(CommonErrorHandler.class);
		var two = mock(CommonErrorHandler.class);
		var three = mock(CommonErrorHandler.class);
		var eh = new CommonDelegatingErrorHandler(def);
		eh.setErrorHandlers(Map.of(IllegalStateException.class, one, IllegalArgumentException.class, two));
		eh.addDelegate(RuntimeException.class, three);

		eh.handleOne(wrap(new IOException()), mock(ConsumerRecord.class), mock(Consumer.class),
				mock(MessageListenerContainer.class));
		verify(def).handleOne(any(), any(), any(), any());
		eh.handleOne(wrap(new KafkaException("test")), mock(ConsumerRecord.class), mock(Consumer.class),
				mock(MessageListenerContainer.class));
		verify(three).handleOne(any(), any(), any(), any());
		eh.handleOne(wrap(new IllegalArgumentException()), mock(ConsumerRecord.class), mock(Consumer.class),
				mock(MessageListenerContainer.class));
		verify(two).handleOne(any(), any(), any(), any());
		eh.handleOne(wrap(new IllegalStateException()), mock(ConsumerRecord.class), mock(Consumer.class),
				mock(MessageListenerContainer.class));
		verify(one).handleOne(any(), any(), any(), any());
	}

	@Test
	void testDelegateForThrowableIsAppliedWhenCauseTraversingIsEnabled() {
		var defaultHandler = mock(CommonErrorHandler.class);

		var directCauseErrorHandler = mock(CommonErrorHandler.class);
		var directCauseExc = new IllegalArgumentException();
		var errorHandler = mock(CommonErrorHandler.class);
		var exc = new UnsupportedOperationException(directCauseExc);

		var delegatingErrorHandler = new CommonDelegatingErrorHandler(defaultHandler);
		delegatingErrorHandler.setCauseChainTraversing(true);
		delegatingErrorHandler.setErrorHandlers(Map.of(
			exc.getClass(), errorHandler,
			directCauseExc.getClass(), directCauseErrorHandler
		));

		delegatingErrorHandler.handleRemaining(directCauseExc, Collections.emptyList(), mock(Consumer.class),
			mock(MessageListenerContainer.class));

		verify(directCauseErrorHandler).handleRemaining(any(), any(), any(), any());
		verify(errorHandler, never()).handleRemaining(any(), any(), any(), any());
	}

	@Test
	void testDelegateForThrowableCauseIsAppliedWhenCauseTraversingIsEnabled() {
		var defaultHandler = mock(CommonErrorHandler.class);

		var directCauseErrorHandler = mock(CommonErrorHandler.class);
		var directCauseExc = new IllegalArgumentException();
		var exc = new UnsupportedOperationException(directCauseExc);

		var delegatingErrorHandler = new CommonDelegatingErrorHandler(defaultHandler);
		delegatingErrorHandler.setCauseChainTraversing(true);
		delegatingErrorHandler.setErrorHandlers(Map.of(
			directCauseExc.getClass(), directCauseErrorHandler
		));

		delegatingErrorHandler.handleRemaining(exc, Collections.emptyList(), mock(Consumer.class),
			mock(MessageListenerContainer.class));

		verify(directCauseErrorHandler).handleRemaining(any(), any(), any(), any());
	}

	@Test
	@SuppressWarnings({ "ConstantConditions", "unchecked" })
	void testDelegateForClassifiableThrowableCauseIsAppliedWhenCauseTraversingIsEnabled() {
		var defaultHandler = mock(CommonErrorHandler.class);

		var directCauseErrorHandler = mock(CommonErrorHandler.class);
		var directCauseExc = new KafkaProducerException(null, null, null);
		var exc = new UnsupportedOperationException(directCauseExc);

		var delegatingErrorHandler = new CommonDelegatingErrorHandler(defaultHandler);
		delegatingErrorHandler.setCauseChainTraversing(true);
		delegatingErrorHandler.setErrorHandlers(Map.of(
			KafkaException.class, directCauseErrorHandler
		));
		delegatingErrorHandler.addDelegate(IllegalStateException.class, mock(CommonErrorHandler.class));
		assertThat(KafkaTestUtils.getPropertyValue(delegatingErrorHandler, "exceptionMatcher.entries", Map.class).keySet())
				.contains(IllegalStateException.class);


		delegatingErrorHandler.handleRemaining(exc, Collections.emptyList(), mock(Consumer.class),
			mock(MessageListenerContainer.class));

		verify(directCauseErrorHandler).handleRemaining(any(), any(), any(), any());
	}

	@Test
	@SuppressWarnings("ConstantConditions")
	void testDefaultDelegateIsApplied() {
		var defaultHandler = mock(CommonErrorHandler.class);
		var delegatingErrorHandler = new CommonDelegatingErrorHandler(defaultHandler);
		delegatingErrorHandler.setCauseChainTraversing(true);

		delegatingErrorHandler.handleRemaining(null, Collections.emptyList(), mock(Consumer.class),
			mock(MessageListenerContainer.class));

		verify(defaultHandler).handleRemaining(any(), any(), any(), any());
	}

	@Test
	void testAddIncompatibleAckAfterHandleDelegate() {
		var defaultHandler = mock(CommonErrorHandler.class);
		given(defaultHandler.isAckAfterHandle()).willReturn(true);
		var delegatingErrorHandler = new CommonDelegatingErrorHandler(defaultHandler);
		var delegate = mock(CommonErrorHandler.class);
		given(delegate.isAckAfterHandle()).willReturn(false);

		assertThatThrownBy(() -> delegatingErrorHandler.addDelegate(IllegalStateException.class, delegate))
				.isInstanceOf(IllegalArgumentException.class)
				.hasMessage("All delegates must return the same value when calling 'isAckAfterHandle()'");
	}

	@Test
	void testAddIncompatibleSeeksAfterHandlingDelegate() {
		var defaultHandler = mock(CommonErrorHandler.class);
		given(defaultHandler.seeksAfterHandling()).willReturn(true);
		var delegatingErrorHandler = new CommonDelegatingErrorHandler(defaultHandler);
		var delegate = mock(CommonErrorHandler.class);
		given(delegate.seeksAfterHandling()).willReturn(false);

		assertThatThrownBy(() -> delegatingErrorHandler.addDelegate(IllegalStateException.class, delegate))
				.isInstanceOf(IllegalArgumentException.class)
				.hasMessage("All delegates must return the same value when calling 'seeksAfterHandling()'");
	}

	@Test
	void testAddMultipleDelegatesWithOneIncompatible() {
		var defaultHandler = mock(CommonErrorHandler.class);
		given(defaultHandler.seeksAfterHandling()).willReturn(true);
		var delegatingErrorHandler = new CommonDelegatingErrorHandler(defaultHandler);
		var one = mock(CommonErrorHandler.class);
		given(one.seeksAfterHandling()).willReturn(true);
		var two = mock(CommonErrorHandler.class);
		given(one.seeksAfterHandling()).willReturn(false);
		Map<Class<? extends Throwable>, CommonErrorHandler> delegates = Map.of(IllegalStateException.class, one, IOException.class, two);

		assertThatThrownBy(() -> delegatingErrorHandler.setErrorHandlers(delegates))
				.isInstanceOf(IllegalArgumentException.class)
				.hasMessage("All delegates must return the same value when calling 'seeksAfterHandling()'");
	}

	@Test
	void onPartitionsAssignedIsForwardedToDefaultAndDelegates() {
		var defaultHandler = mock(CommonErrorHandler.class);
		var one = mock(CommonErrorHandler.class);
		var two = mock(CommonErrorHandler.class);
		var eh = new CommonDelegatingErrorHandler(defaultHandler);
		eh.setErrorHandlers(Map.of(IllegalStateException.class, one, IllegalArgumentException.class, two));

		Consumer<?, ?> consumer = mock(Consumer.class);
		Collection<TopicPartition> partitions = List.of(new TopicPartition("topic", 0));
		AtomicInteger publishPauseInvocations = new AtomicInteger();
		Runnable publishPause = publishPauseInvocations::incrementAndGet;

		eh.onPartitionsAssigned(consumer, partitions, publishPause);

		verify(defaultHandler).onPartitionsAssigned(same(consumer), eq(partitions), same(publishPause));
		verify(one).onPartitionsAssigned(same(consumer), eq(partitions), same(publishPause));
		verify(two).onPartitionsAssigned(same(consumer), eq(partitions), same(publishPause));
		assertThat(publishPauseInvocations).hasValue(0);
	}

	private static DefaultErrorHandler noSeekErrorHandler() {
		DefaultErrorHandler handler = new DefaultErrorHandler((record, exception) -> {
			throw new AssertionError("Unexpected recovery");
		}, new FixedBackOff(0, 9));
		handler.setSeekAfterError(false);
		return handler;
	}

	private static ConsumerRecords<String, String> threeRecords() {
		return new ConsumerRecords<>(Map.of(TP, List.of(
				new ConsumerRecord<>("topic", 0, 0L, "k0", "v0"),
				new ConsumerRecord<>("topic", 0, 1L, "k1", "v1"),
				new ConsumerRecord<>("topic", 0, 2L, "k2", "v2"))), Map.of());
	}

	private Exception wrap(Exception ex) {
		return new ListenerExecutionFailedException("test", ex);
	}

}
