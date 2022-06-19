/*
 * Copyright 2021-2022 the original author or authors.
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

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

import java.io.IOException;
import java.util.Collections;
import java.util.Map;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.junit.jupiter.api.Test;

import org.springframework.kafka.KafkaException;
import org.springframework.kafka.core.KafkaProducerException;

/**
 * Tests for {@link CommonDelegatingErrorHandler}.
 *
 * @author Gary Russell
 * @author Adrian Chlebosz
 * @since 2.8
 *
 */
public class CommonDelegatingErrorHandlerTests {

	@Test
	void testRecordDelegates() {
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
	void testBatchDelegates() {
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
	@SuppressWarnings("ConstantConditions")
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

	private Exception wrap(Exception ex) {
		return new ListenerExecutionFailedException("test", ex);
	}

}
