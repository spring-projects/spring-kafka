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
import java.util.Collections;
import java.util.Map;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.junit.jupiter.api.Test;

import org.springframework.kafka.KafkaException;
import org.springframework.kafka.core.KafkaProducerException;
import org.springframework.kafka.test.utils.KafkaTestUtils;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.BDDMockito.given;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

/**
 * Tests for {@link CommonDelegatingErrorHandler}.
 *
 * @author Gary Russell
 * @author Adrian Chlebosz
 * @author Antonin Arquey
 * @author Dan Blackney
 * @since 2.8
 *
 */
public class CommonDelegatingErrorHandlerTests {

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

	private Exception wrap(Exception ex) {
		return new ListenerExecutionFailedException("test", ex);
	}

}
