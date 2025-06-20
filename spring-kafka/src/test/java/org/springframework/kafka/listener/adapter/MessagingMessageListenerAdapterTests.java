/*
 * Copyright 2016-present the original author or authors.
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

package org.springframework.kafka.listener.adapter;

import java.lang.reflect.Method;
import java.util.concurrent.CompletableFuture;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Mono;

import org.springframework.kafka.annotation.KafkaListenerAnnotationBeanPostProcessor;
import org.springframework.kafka.listener.AcknowledgingMessageListener;
import org.springframework.kafka.listener.ListenerExecutionFailedException;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.kafka.support.converter.RecordMessageConverter;
import org.springframework.messaging.support.GenericMessage;

import static org.assertj.core.api.Assertions.assertThatExceptionOfType;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.BDDMockito.willReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

/**
 * @author Gary Russell
 * @since 1.1.2
 *
 */
public class MessagingMessageListenerAdapterTests {

	@Test
	void testFallbackType() {
		final class MyAdapter extends MessagingMessageListenerAdapter<String, String>
				implements AcknowledgingMessageListener<String, String> {

			private MyAdapter() {
				super(null, null);
			}

			@Override
			public void onMessage(ConsumerRecord<String, String> data, Acknowledgment acknowledgment) {
				toMessagingMessage(data, acknowledgment, null);
			}

		}

		MyAdapter adapter = new MyAdapter();
		adapter.setFallbackType(String.class);
		RecordMessageConverter converter = mock(RecordMessageConverter.class);
		ConsumerRecord<String, String> cr = new ConsumerRecord<>("foo", 1, 1L, null, null);
		Acknowledgment ack = mock(Acknowledgment.class);
		willReturn(new GenericMessage<>("foo")).given(converter).toMessage(cr, ack, null, String.class);
		adapter.setMessageConverter(converter);
		adapter.onMessage(cr, ack);
		verify(converter).toMessage(cr, ack, null, String.class);
	}

	@Test
	public void testCompletableFutureReturn() throws NoSuchMethodException {

		Method method = getClass().getDeclaredMethod("future", String.class, Acknowledgment.class);
		testAsyncResult(method, "bar");
	}

	@Test
	public void testMonoReturn() throws NoSuchMethodException {

		Method method = getClass().getDeclaredMethod("mono", String.class, Acknowledgment.class);
		testAsyncResult(method, "baz");
	}

	private void testAsyncResult(Method method, String topic) {

		KafkaListenerAnnotationBeanPostProcessor<String, String> bpp = new KafkaListenerAnnotationBeanPostProcessor<>();
		RecordMessagingMessageListenerAdapter<String, String> adapter =
				spy(new RecordMessagingMessageListenerAdapter<>(this, method));
		adapter.setHandlerMethod(
				new HandlerAdapter(bpp.getMessageHandlerMethodFactory().createInvocableHandlerMethod(this, method)));
		ConsumerRecord<String, String> cr = new ConsumerRecord<>(topic, 0, 0L, null, "foo");
		Acknowledgment ack = mock(Acknowledgment.class);
		RecordMessageConverter converter = mock(RecordMessageConverter.class);
		willReturn(new GenericMessage<>("foo")).given(converter).toMessage(cr, ack, null, String.class);
		adapter.setMessageConverter(converter);
		adapter.onMessage(cr, ack, null);
		verify(adapter, times(1)).asyncSuccess(any(), any(), any(), anyBoolean());
		verify(adapter, times(1)).acknowledge(any());
	}

	@Test
	void testMissingAck() throws NoSuchMethodException, SecurityException {
		KafkaListenerAnnotationBeanPostProcessor<String, String> bpp = new KafkaListenerAnnotationBeanPostProcessor<>();
		Method method = getClass().getDeclaredMethod("test", Acknowledgment.class);
		RecordMessagingMessageListenerAdapter<String, String> adapter =
				new RecordMessagingMessageListenerAdapter<>(this, method);
		adapter.setHandlerMethod(
				new HandlerAdapter(bpp.getMessageHandlerMethodFactory().createInvocableHandlerMethod(this, method)));
		assertThatExceptionOfType(ListenerExecutionFailedException.class).isThrownBy(() -> adapter.onMessage(
						new ConsumerRecord<>("foo", 0, 0L, null, "foo"), null, null))
				.withCauseExactlyInstanceOf(IllegalStateException.class)
				.withStackTraceContaining("MANUAL");
	}

	public void test(Acknowledgment ack) {

	}

	public CompletableFuture<String> future(String data, Acknowledgment ack) {

		return CompletableFuture.completedFuture("processed" + data);
	}

	public Mono<String> mono(String data, Acknowledgment ack) {

		return Mono.just(data);
	}

}
