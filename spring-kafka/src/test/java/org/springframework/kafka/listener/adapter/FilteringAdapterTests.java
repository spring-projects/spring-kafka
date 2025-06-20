/*
 * Copyright 2017-present the original author or authors.
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

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.junit.jupiter.api.Test;

import org.springframework.kafka.listener.BatchAcknowledgingMessageListener;
import org.springframework.kafka.support.Acknowledgment;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.BDDMockito.willAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.only;
import static org.mockito.Mockito.verify;

/**
 * @author Gary Russell
 * @author Sanghyeok An
 * @since 2.0
 *
 */
public class FilteringAdapterTests {

	@SuppressWarnings("unchecked")
	@Test
	public void testBatchFilter() throws Exception {
		BatchAcknowledgingMessageListener<String, String> listener = mock(BatchAcknowledgingMessageListener.class);
		FilteringBatchMessageListenerAdapter<String, String> adapter =
				new FilteringBatchMessageListenerAdapter<>(listener, r -> false);
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
				new FilteringBatchMessageListenerAdapter<>(listener, r -> false, true);
		List<ConsumerRecord<String, String>> consumerRecords = new ArrayList<>();
		final CountDownLatch latch = new CountDownLatch(1);
		adapter.onMessage(consumerRecords, latch::countDown, null);
		assertThat(latch.await(10, TimeUnit.SECONDS)).isTrue();
		verify(listener, never()).onMessage(any(List.class), any(Acknowledgment.class));
	}

	@SuppressWarnings("unchecked")
	@Test
	public void listener_should_not_be_invoked_on_emptyList_and_ignoreEmptyBatch_true() {
		// Given :
		RecordFilterStrategy<String, String> filter = new RecordFilterStrategy<>() {

			@Override
			public boolean filter(ConsumerRecord<String, String> consumerRecord) {
				return true;
			}

			@Override
			public List<ConsumerRecord<String, String>> filterBatch(
					List<ConsumerRecord<String, String>> consumerRecords) {
				return List.of();
			}

			@Override
			public boolean ignoreEmptyBatch() {
				return true;
			}
		};

		BatchAcknowledgingMessageListener<String, String> listener = mock();
		FilteringBatchMessageListenerAdapter<String, String> adapter =
				new FilteringBatchMessageListenerAdapter<>(listener, filter);
		List<ConsumerRecord<String, String>> consumerRecords = new ArrayList<>();
		Acknowledgment ack = mock();

		// When :
		adapter.onMessage(consumerRecords, ack, null);

		// Then
		verify(ack, only()).acknowledge();
		verify(listener, never()).onMessage(any(List.class), any(Acknowledgment.class), any(KafkaConsumer.class));
		verify(listener, never()).onMessage(any(List.class), any(Acknowledgment.class));
		verify(listener, never()).onMessage(any(List.class), any(KafkaConsumer.class));
		verify(listener, never()).onMessage(any(List.class));
	}

	@SuppressWarnings("unchecked")
	@Test
	public void listener_should_be_invoked_on_notEmptyList_and_ignoreEmptyBatch_true() throws Exception {
		// Given :
		RecordFilterStrategy<String, String> filter = new RecordFilterStrategy<>() {

			@Override
			public boolean filter(ConsumerRecord<String, String> consumerRecord) {
				return true;
			}

			@Override
			public List<ConsumerRecord<String, String>> filterBatch(
					List<ConsumerRecord<String, String>> consumerRecords) {
				return consumerRecords;
			}

			@Override
			public boolean ignoreEmptyBatch() {
				return true;
			}
		};

		BatchAcknowledgingMessageListener<String, String> listener = mock();
		FilteringBatchMessageListenerAdapter<String, String> adapter =
				new FilteringBatchMessageListenerAdapter<>(listener, filter);
		List<ConsumerRecord<String, String>> consumerRecords =
				List.of(new ConsumerRecord<>("hello-topic", 1, 1, "hello-key", "hello-value"));
		Acknowledgment ack = mock();

		CountDownLatch latch = new CountDownLatch(1);
		willAnswer(i -> {
			latch.countDown();
			return null;
		}).given(listener).onMessage(any(List.class), any(Acknowledgment.class));

		// When :
		adapter.onMessage(consumerRecords, ack, null);

		// Then
		assertThat(latch.await(10, TimeUnit.SECONDS)).isTrue();
		verify(ack, never()).acknowledge();
	}

	@SuppressWarnings("unchecked")
	@Test
	public void listener_should_be_invoked_on_emptyList_and_ignoreEmptyBatch_false() throws Exception {
		// Given :
		RecordFilterStrategy<String, String> filter = new RecordFilterStrategy<>() {

			@Override
			public boolean filter(ConsumerRecord<String, String> consumerRecord) {
				return true;
			}

			@Override
			public List<ConsumerRecord<String, String>> filterBatch(
					List<ConsumerRecord<String, String>> consumerRecords) {
				return List.of();
			}
		};

		BatchAcknowledgingMessageListener<String, String> listener = mock();
		FilteringBatchMessageListenerAdapter<String, String> adapter =
				new FilteringBatchMessageListenerAdapter<>(listener, filter);
		List<ConsumerRecord<String, String>> consumerRecords = new ArrayList<>();
		Acknowledgment ack = mock();

		CountDownLatch latch = new CountDownLatch(1);
		willAnswer(i -> {
			latch.countDown();
			return null;
		}).given(listener).onMessage(any(List.class), any(Acknowledgment.class));

		// When :
		adapter.onMessage(consumerRecords, ack, null);

		// Then
		assertThat(latch.await(10, TimeUnit.SECONDS)).isTrue();
		verify(ack, never()).acknowledge();
	}

	@SuppressWarnings("unchecked")
	@Test
	public void listener_should_be_invoked_on_notEmptyList_and_ignoreEmptyBatch_false() throws Exception {
		// Given :
		RecordFilterStrategy<String, String> filter = new RecordFilterStrategy<>() {

			@Override
			public boolean filter(ConsumerRecord<String, String> consumerRecord) {
				return true;
			}

			@Override
			public List<ConsumerRecord<String, String>> filterBatch(
					// System Under Test
					List<ConsumerRecord<String, String>> consumerRecords) {
				return consumerRecords;
			}
		};

		BatchAcknowledgingMessageListener<String, String> listener = mock();
		FilteringBatchMessageListenerAdapter<String, String> adapter =
				new FilteringBatchMessageListenerAdapter<>(listener, filter);
		List<ConsumerRecord<String, String>> consumerRecords =
				List.of(new ConsumerRecord<>("hello-topic", 1, 1, "hello-key", "hello-value"));
		Acknowledgment ack = mock();

		final CountDownLatch latch = new CountDownLatch(1);
		willAnswer(i -> {
			latch.countDown();
			return null;
		}).given(listener).onMessage(any(List.class), any(Acknowledgment.class));

		// When :
		adapter.onMessage(consumerRecords, ack, null);

		// Then
		assertThat(latch.await(10, TimeUnit.SECONDS)).isTrue();
		verify(ack, never()).acknowledge();
	}

}
