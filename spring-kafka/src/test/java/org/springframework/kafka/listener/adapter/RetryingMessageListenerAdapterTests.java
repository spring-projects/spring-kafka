/*
 * Copyright 2017 the original author or authors.
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

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;

import java.util.concurrent.atomic.AtomicReference;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.Test;

import org.springframework.kafka.listener.AcknowledgingMessageListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.retry.RetryContext;
import org.springframework.retry.support.RetryTemplate;

/**
 * @author Gary Russell
 * @since 1.0.7
 *
 */
public class RetryingMessageListenerAdapterTests {

	@Test
	public void testRecoveryCallbackSimple() {
		final AtomicReference<RetryContext> context = new AtomicReference<>();
		RetryingMessageListenerAdapter<String, String> adapter = new RetryingMessageListenerAdapter<>(
				r -> {
					throw new RuntimeException();
				}, new RetryTemplate(), c -> {
					context.set(c);
					return null;
				});
		@SuppressWarnings("unchecked")
		ConsumerRecord<String, String> record = mock(ConsumerRecord.class);
		adapter.onMessage(record);
		assertThat(context.get()).isNotNull();
		assertThat(context.get().getAttribute(RetryingMessageListenerAdapter.CONTEXT_ACKNOWLEDGMENT)).isNull();
		assertThat(context.get().getAttribute(RetryingMessageListenerAdapter.CONTEXT_RECORD)).isSameAs(record);
	}

	@Test
	public void testRecoveryCallbackAckOnly() {
		final AtomicReference<RetryContext> context = new AtomicReference<>();
		RetryingAcknowledgingMessageListenerAdapter<String, String> adapter =
			new RetryingAcknowledgingMessageListenerAdapter<>(
				(AcknowledgingMessageListener<String, String>) (r, a) -> {
					throw new RuntimeException();
				}, new RetryTemplate(), c -> {
					context.set(c);
					return null;
				});
		@SuppressWarnings("unchecked")
		ConsumerRecord<String, String> record = mock(ConsumerRecord.class);
		Acknowledgment ack = mock(Acknowledgment.class);
		adapter.onMessage(record, ack);
		assertThat(context.get()).isNotNull();
		assertThat(context.get().getAttribute(RetryingMessageListenerAdapter.CONTEXT_ACKNOWLEDGMENT)).isSameAs(ack);
		assertThat(context.get().getAttribute(RetryingMessageListenerAdapter.CONTEXT_RECORD)).isSameAs(record);
	}

}
