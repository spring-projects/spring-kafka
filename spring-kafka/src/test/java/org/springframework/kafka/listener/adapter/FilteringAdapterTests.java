/*
 * Copyright 2017 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.springframework.kafka.listener.adapter;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.BDDMockito.willAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.Test;

import org.springframework.kafka.listener.BatchAcknowledgingMessageListener;
import org.springframework.kafka.support.Acknowledgment;

/**
 * @author Gary Russell
 * @since 2.0
 *
 */
public class FilteringAdapterTests {

	@SuppressWarnings("unchecked")
	@Test
	public void testBatchFilter() throws Exception {
		BatchAcknowledgingMessageListener<String, String> listener = mock(BatchAcknowledgingMessageListener.class);
		FilteringBatchMessageListenerAdapter<String, String> adapter =
				new FilteringBatchMessageListenerAdapter<String, String>(listener, r -> false);
		List<ConsumerRecord<String, String>> consumerRecords = new ArrayList<>();
		final CountDownLatch latch = new CountDownLatch(1);
		willAnswer(i -> {
			latch.countDown();
			return null;
		}).given(listener).onMessage(any(List.class), any(Acknowledgment.class));
		Acknowledgment ack = mock(Acknowledgment.class);
		adapter.onMessage(consumerRecords, ack, null);
		assertThat(latch.await(10, TimeUnit.SECONDS)).isTrue();
		verify(ack, never()).acknowledge();
	}

	@SuppressWarnings("unchecked")
	@Test
	public void testBatchFilterAckDiscard() throws Exception {
		BatchAcknowledgingMessageListener<String, String> listener = mock(BatchAcknowledgingMessageListener.class);
		FilteringBatchMessageListenerAdapter<String, String> adapter =
				new FilteringBatchMessageListenerAdapter<String, String>(listener, r -> false, true);
		List<ConsumerRecord<String, String>> consumerRecords = new ArrayList<>();
		final CountDownLatch latch = new CountDownLatch(1);
		adapter.onMessage(consumerRecords, () -> latch.countDown(), null);
		assertThat(latch.await(10, TimeUnit.SECONDS)).isTrue();
		verify(listener, never()).onMessage(any(List.class), any(Acknowledgment.class));
	}

}
