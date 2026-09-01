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
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.jspecify.annotations.NonNull;
import org.jspecify.annotations.Nullable;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Mono;

import org.springframework.kafka.annotation.KafkaListenerAnnotationBeanPostProcessor;
import org.springframework.kafka.listener.AcknowledgingMessageListener;
import org.springframework.kafka.listener.ListenerExecutionFailedException;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.kafka.support.converter.RecordMessageConverter;
import org.springframework.messaging.support.GenericMessage;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;
import static org.assertj.core.api.Assertions.assertThatNoException;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.BDDMockito.willReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

/**
 * @author Gary Russell
 * @author Abhishek Moondra
 * @author Nikita Kibitkin
 * @since 1.1.2
 *
 */
public class MessagingMessageListenerAdapterTests {

	private final CompletableFuture<String> pendingFuture = new CompletableFuture<>();

	private final AtomicBoolean monoCancelled = new AtomicBoolean();

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
	void asyncResultCallbackCompletesAfterAck() throws NoSuchMethodException {
		Method method = getClass().getDeclaredMethod("pendingFuture", String.class, Acknowledgment.class);
		Consumer<?, ?> consumer = mock(Consumer.class);
		AtomicReference<CompletableFuture<Void>> inFlight = new AtomicReference<>();
		RecordMessagingMessageListenerAdapter<String, String> adapter = asyncAdapter(method);
		adapter.addCallbackForAsyncResult(consumer, inFlight::set);
		Acknowledgment ack = mock(Acknowledgment.class);
		adapter.onMessage(new ConsumerRecord<>("foo", 0, 0L, null, "foo"), ack, consumer);
		assertThat(inFlight.get()).isNotNull().isNotDone();
		verify(ack, never()).acknowledge();
		this.pendingFuture.complete("done");
		assertThat(inFlight.get()).isCompleted();
		verify(ack).acknowledge();
	}

	@Test
	void asyncResultCallbackOnlyForRegisteredConsumer() throws NoSuchMethodException {
		Method method = getClass().getDeclaredMethod("pendingFuture", String.class, Acknowledgment.class);
		AtomicReference<CompletableFuture<Void>> inFlight = new AtomicReference<>();
		RecordMessagingMessageListenerAdapter<String, String> adapter = asyncAdapter(method);
		adapter.addCallbackForAsyncResult(mock(Consumer.class), inFlight::set);
		adapter.onMessage(new ConsumerRecord<>("foo", 0, 0L, null, "foo"), mock(Acknowledgment.class),
				mock(Consumer.class));
		assertThat(inFlight.get()).isNull();
	}

	@Test
	void cancelledFutureIsNeitherAckedNorPassedToErrorHandler() throws NoSuchMethodException {
		Method method = getClass().getDeclaredMethod("pendingFuture", String.class, Acknowledgment.class);
		Consumer<?, ?> consumer = mock(Consumer.class);
		AtomicReference<CompletableFuture<Void>> inFlight = new AtomicReference<>();
		RecordMessagingMessageListenerAdapter<String, String> adapter = asyncAdapter(method);
		adapter.addCallbackForAsyncResult(consumer, inFlight::set);
		AtomicBoolean retried = new AtomicBoolean();
		adapter.setCallbackForAsyncFailure((record, ex) -> retried.set(true));
		Acknowledgment ack = mock(Acknowledgment.class);
		adapter.onMessage(new ConsumerRecord<>("foo", 0, 0L, null, "foo"), ack, consumer);
		inFlight.get().cancel(true);
		assertThat(this.pendingFuture).isCancelled();
		verify(adapter, never()).asyncFailure(any(), any(), any(), any(), any());
		verify(ack, never()).acknowledge();
		assertThat(retried).isFalse();
	}

	@Test
	void cancelledMonoIsDisposed() throws NoSuchMethodException {
		Method method = getClass().getDeclaredMethod("pendingMono", String.class, Acknowledgment.class);
		Consumer<?, ?> consumer = mock(Consumer.class);
		AtomicReference<CompletableFuture<Void>> inFlight = new AtomicReference<>();
		RecordMessagingMessageListenerAdapter<String, String> adapter = asyncAdapter(method);
		adapter.addCallbackForAsyncResult(consumer, inFlight::set);
		Acknowledgment ack = mock(Acknowledgment.class);
		adapter.onMessage(new ConsumerRecord<>("foo", 0, 0L, null, "foo"), ack, consumer);
		assertThat(this.monoCancelled).isFalse();
		inFlight.get().cancel(true);
		assertThat(this.monoCancelled).isTrue();
		verify(adapter, never()).asyncFailure(any(), any(), any(), any(), any());
		verify(ack, never()).acknowledge();
	}

	@Test
	void asyncResultCallbackNotInvokedForSyncResult() throws NoSuchMethodException {
		Method method = getClass().getDeclaredMethod("sync", String.class, Acknowledgment.class);
		Consumer<?, ?> consumer = mock(Consumer.class);
		AtomicReference<CompletableFuture<Void>> inFlight = new AtomicReference<>();
		RecordMessagingMessageListenerAdapter<String, String> adapter = asyncAdapter(method);
		adapter.addCallbackForAsyncResult(consumer, inFlight::set);
		adapter.onMessage(new ConsumerRecord<>("foo", 0, 0L, null, "foo"), mock(Acknowledgment.class), consumer);
		assertThat(inFlight.get()).isNull();
	}

	private RecordMessagingMessageListenerAdapter<String, String> asyncAdapter(Method method) {
		KafkaListenerAnnotationBeanPostProcessor<String, String> bpp = new KafkaListenerAnnotationBeanPostProcessor<>();
		RecordMessagingMessageListenerAdapter<String, String> adapter =
				spy(new RecordMessagingMessageListenerAdapter<>(this, method));
		adapter.setHandlerMethod(
				new HandlerAdapter(bpp.getMessageHandlerMethodFactory().createInvocableHandlerMethod(this, method)));
		RecordMessageConverter converter = mock(RecordMessageConverter.class);
		willReturn(new GenericMessage<>("foo")).given(converter).toMessage(any(), any(), any(), any());
		adapter.setMessageConverter(converter);
		return adapter;
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

	@Test
	void noOpAckWhenAcknowledgmentParameterIsNonNull() throws NoSuchMethodException {
		KafkaListenerAnnotationBeanPostProcessor<String, String> bpp = new KafkaListenerAnnotationBeanPostProcessor<>();
		Method method = getClass().getDeclaredMethod("testNonNullAck", Acknowledgment.class);
		RecordMessagingMessageListenerAdapter<String, String> adapter =
				new RecordMessagingMessageListenerAdapter<>(this, method);
		adapter.setHandlerMethod(
				new HandlerAdapter(bpp.getMessageHandlerMethodFactory().createInvocableHandlerMethod(this, method)));
		// A non-null Acknowledgment parameter must substitute a no-op ack, not fail with "No Acknowledgment available"
		assertThatNoException().isThrownBy(() -> adapter.onMessage(
				new ConsumerRecord<>("foo", 0, 0L, null, "foo"), null, null));
	}

	public void test(@Nullable Acknowledgment ack) {

	}

	public void testNonNullAck(@NonNull Acknowledgment ack) {

	}

	public CompletableFuture<String> future(String data, Acknowledgment ack) {

		return CompletableFuture.completedFuture("processed" + data);
	}

	public Mono<String> mono(String data, Acknowledgment ack) {

		return Mono.just(data);
	}

	public CompletableFuture<String> pendingFuture(String data, Acknowledgment ack) {

		return this.pendingFuture;
	}

	public Mono<String> pendingMono(String data, Acknowledgment ack) {

		return Mono.<String>never().doOnCancel(() -> this.monoCancelled.set(true));
	}

	public String sync(String data, Acknowledgment ack) {

		return data;
	}

}
