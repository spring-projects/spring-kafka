/*
 * Copyright 2021 the original author or authors.
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
import static org.mockito.Mockito.verify;

import java.io.IOException;
import java.util.Collections;
import java.util.Map;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.junit.jupiter.api.Test;

import org.springframework.kafka.KafkaException;

/**
 * @author Gary Russell
 * @since 2.7.4
 *
 */
public class ConditionalDelegatingErrorHandlerTests {

	@Test
	void testRecordDelegates() {
		var def = mock(ContainerAwareErrorHandler.class);
		var one = mock(ContainerAwareErrorHandler.class);
		var two = mock(ContainerAwareErrorHandler.class);
		var three = mock(ContainerAwareErrorHandler.class);
		var eh = new ConditionalDelegatingErrorHandler(def);
		eh.setErrorHandlers(Map.of(IllegalStateException.class, one, IllegalArgumentException.class, two));
		eh.addDelegate(RuntimeException.class, three);

		eh.handle(wrap(new IOException()), Collections.emptyList(), mock(Consumer.class),
				mock(MessageListenerContainer.class));
		verify(def).handle(any(), any(), any(), any());
		eh.handle(wrap(new KafkaException("test")), Collections.emptyList(), mock(Consumer.class),
				mock(MessageListenerContainer.class));
		verify(three).handle(any(), any(), any(), any());
		eh.handle(wrap(new IllegalArgumentException()), Collections.emptyList(), mock(Consumer.class),
				mock(MessageListenerContainer.class));
		verify(two).handle(any(), any(), any(), any());
		eh.handle(wrap(new IllegalStateException()), Collections.emptyList(), mock(Consumer.class),
				mock(MessageListenerContainer.class));
		verify(one).handle(any(), any(), any(), any());
	}

	@Test
	void testBatchDelegates() {
		var def = mock(ContainerAwareBatchErrorHandler.class);
		var one = mock(ContainerAwareBatchErrorHandler.class);
		var two = mock(ContainerAwareBatchErrorHandler.class);
		var three = mock(ContainerAwareBatchErrorHandler.class);
		var eh = new ConditionalDelegatingBatchErrorHandler(def);
		eh.setErrorHandlers(Map.of(IllegalStateException.class, one, IllegalArgumentException.class, two));
		eh.addDelegate(RuntimeException.class, three);

		eh.handle(wrap(new IOException()), mock(ConsumerRecords.class), mock(Consumer.class),
				mock(MessageListenerContainer.class), mock(Runnable.class));
		verify(def).handle(any(), any(), any(), any(), any());
		eh.handle(wrap(new KafkaException("test")), mock(ConsumerRecords.class), mock(Consumer.class),
				mock(MessageListenerContainer.class), mock(Runnable.class));
		verify(three).handle(any(), any(), any(), any(), any());
		eh.handle(wrap(new IllegalArgumentException()), mock(ConsumerRecords.class), mock(Consumer.class),
				mock(MessageListenerContainer.class), mock(Runnable.class));
		verify(two).handle(any(), any(), any(), any(), any());
		eh.handle(wrap(new IllegalStateException()), mock(ConsumerRecords.class), mock(Consumer.class),
				mock(MessageListenerContainer.class), mock(Runnable.class));
		verify(one).handle(any(), any(), any(), any(), any());
	}

	private Exception wrap(Exception ex) {
		return new ListenerExecutionFailedException("test", ex);
	}

}
