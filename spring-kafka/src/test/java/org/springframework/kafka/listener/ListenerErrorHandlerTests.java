/*
 * Copyright 2022 the original author or authors.
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
import static org.mockito.BDDMockito.willThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

import java.lang.reflect.Method;
import java.util.Collections;
import java.util.List;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.Test;

import org.springframework.kafka.listener.adapter.BatchMessagingMessageListenerAdapter;
import org.springframework.kafka.listener.adapter.HandlerAdapter;
import org.springframework.kafka.listener.adapter.RecordMessagingMessageListenerAdapter;
import org.springframework.kafka.support.Acknowledgment;

/**
 * @author Gary Russell
 * @since 2.9
 *
 */
public class ListenerErrorHandlerTests {

	private static Method test1;

	private static Method test2;

	static {
		try {
			test1 = TestListener.class.getDeclaredMethod("test1", String.class, Acknowledgment.class);
			test2 = TestListener.class.getDeclaredMethod("test2", List.class, Acknowledgment.class);
		}
		catch (NoSuchMethodException | SecurityException e) {
			throw new IllegalStateException(e);
		}
	}

	@SuppressWarnings({ "rawtypes", "unchecked" })
	@Test
	void record() throws Exception {
		RecordMessagingMessageListenerAdapter adapter = new RecordMessagingMessageListenerAdapter(getClass(), test1,
				(ManualAckListenerErrorHandler) (msg, ex, cons, ack) -> {
					ack.acknowledge();
					return null;
				});
		HandlerAdapter handler = mock(HandlerAdapter.class);
		willThrow(new RuntimeException("test")).given(handler).invoke(any(), any());
		adapter.setHandlerMethod(handler);
		Acknowledgment ack = mock(Acknowledgment.class);
		adapter.onMessage(mock(ConsumerRecord.class), ack, mock(Consumer.class));
		verify(ack).acknowledge();
	}

	@SuppressWarnings({ "rawtypes", "unchecked" })
	@Test
	void batch() throws Exception {
		BatchMessagingMessageListenerAdapter adapter = new BatchMessagingMessageListenerAdapter(getClass(), test2,
				(ManualAckListenerErrorHandler) (msg, ex, cons, ack) -> {
					ack.acknowledge();
					return null;
				});
		HandlerAdapter handler = mock(HandlerAdapter.class);
		willThrow(new RuntimeException("test")).given(handler).invoke(any(), any());
		adapter.setHandlerMethod(handler);
		Acknowledgment ack = mock(Acknowledgment.class);
		adapter.onMessage(Collections.emptyList(), ack, mock(Consumer.class));
		verify(ack).acknowledge();
	}

	private static class TestListener {

		void test1(String foo, Acknowledgment ack) {
		}

		void test2(List<String> foo, Acknowledgment ack) {
		}

	}

}
