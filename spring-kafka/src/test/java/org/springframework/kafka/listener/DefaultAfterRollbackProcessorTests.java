/*
 * Copyright 2019-2021 the original author or authors.
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

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyMap;
import static org.mockito.BDDMockito.given;
import static org.mockito.BDDMockito.willAnswer;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerGroupMetadata;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.Test;
import org.mockito.InOrder;

import org.springframework.kafka.core.KafkaOperations;
import org.springframework.kafka.listener.ContainerProperties.EOSMode;
import org.springframework.kafka.support.serializer.DeserializationException;
import org.springframework.util.backoff.BackOff;
import org.springframework.util.backoff.BackOffExecution;
import org.springframework.util.backoff.FixedBackOff;

/**
 * @author Gary Russell
 * @since 2.3.1
 *
 */
public class DefaultAfterRollbackProcessorTests {

	@SuppressWarnings("deprecation")
	@Test
	void testClassifier() {
		AtomicReference<ConsumerRecord<?, ?>> recovered = new AtomicReference<>();
		AtomicBoolean recovererShouldFail = new AtomicBoolean(false);
		@SuppressWarnings("unchecked")
		KafkaOperations<String, String> template = mock(KafkaOperations.class);
		given(template.isTransactional()).willReturn(true);
		DefaultAfterRollbackProcessor<String, String> processor = new DefaultAfterRollbackProcessor<>((r, t) -> {
			if (recovererShouldFail.getAndSet(false)) {
				throw new RuntimeException("test recoverer failure");
			}
			recovered.set(r);
		}, SeekUtils.DEFAULT_BACK_OFF, template, true);
		ConsumerRecord<String, String> record1 = new ConsumerRecord<>("foo", 0, 0L, "foo", "bar");
		ConsumerRecord<String, String> record2 = new ConsumerRecord<>("foo", 1, 1L, "foo", "bar");
		List<ConsumerRecord<String, String>> records = Arrays.asList(record1, record2);
		IllegalStateException illegalState = new IllegalStateException();
		@SuppressWarnings("unchecked")
		Consumer<String, String> consumer = mock(Consumer.class);
		given(consumer.groupMetadata()).willReturn(new ConsumerGroupMetadata("foo"));
		MessageListenerContainer container = mock(MessageListenerContainer.class);
		processor.process(records, consumer, container, illegalState, true, EOSMode.V1);
		processor.process(records, consumer, container,
				new DeserializationException("intended", null, false, illegalState), true, EOSMode.V1);
		verify(template).sendOffsetsToTransaction(anyMap());
		verify(template, never()).sendOffsetsToTransaction(anyMap(), any(ConsumerGroupMetadata.class));
		assertThat(recovered.get()).isSameAs(record1);
		processor.addNotRetryableExceptions(IllegalStateException.class);
		recovered.set(null);
		recovererShouldFail.set(true);
		processor.process(records, consumer, container, illegalState, true, EOSMode.V1);
		verify(template, times(1)).sendOffsetsToTransaction(anyMap()); // recovery failed
		processor.process(records, consumer, container, illegalState, true, EOSMode.V2);
		verify(template, times(1)).sendOffsetsToTransaction(anyMap(), any(ConsumerGroupMetadata.class));
		assertThat(recovered.get()).isSameAs(record1);
		InOrder inOrder = inOrder(consumer);
		inOrder.verify(consumer).seek(new TopicPartition("foo", 0), 0L); // not recovered so seek
		inOrder.verify(consumer, times(2)).seek(new TopicPartition("foo", 1), 1L);
		inOrder.verify(consumer).seek(new TopicPartition("foo", 0), 0L); // recovery failed
		inOrder.verify(consumer, times(2)).seek(new TopicPartition("foo", 1), 1L);
		inOrder.verify(consumer).groupMetadata();
		inOrder.verifyNoMoreInteractions();
	}

	@Test
	void testBatchBackOff() {
		AtomicReference<ConsumerRecord<?, ?>> recovered = new AtomicReference<>();
		@SuppressWarnings("unchecked")
		KafkaOperations<String, String> template = mock(KafkaOperations.class);
		given(template.isTransactional()).willReturn(true);
		BackOff backOff = spy(new FixedBackOff(0, 1));
		AtomicReference<BackOffExecution> execution = new AtomicReference<>();
		willAnswer(inv -> {
			BackOffExecution exec = spy((BackOffExecution) inv.callRealMethod());
			execution.set(exec);
			return exec;
		}).given(backOff).start();
		ConsumerRecordRecoverer recoverer = mock(ConsumerRecordRecoverer.class);
		DefaultAfterRollbackProcessor<String, String> processor = new DefaultAfterRollbackProcessor<>(recoverer,
				backOff, template, false);
		ConsumerRecord<String, String> record1 = new ConsumerRecord<>("foo", 0, 0L, "foo", "bar");
		ConsumerRecord<String, String> record2 = new ConsumerRecord<>("foo", 1, 1L, "foo", "bar");
		List<ConsumerRecord<String, String>> records = Arrays.asList(record1, record2);
		IllegalStateException illegalState = new IllegalStateException();
		@SuppressWarnings("unchecked")
		Consumer<String, String> consumer = mock(Consumer.class);
		given(consumer.groupMetadata()).willReturn(new ConsumerGroupMetadata("foo"));
		MessageListenerContainer container = mock(MessageListenerContainer.class);
		given(container.isRunning()).willReturn(true);
		processor.process(records, consumer, container, illegalState, false, EOSMode.V2);
		processor.process(records, consumer, container, illegalState, false, EOSMode.V2);
		verify(backOff, times(2)).start();
		verify(execution.get(), times(2)).nextBackOff();
		processor.clearThreadState();
		processor.process(records, consumer, container, illegalState, false, EOSMode.V2);
		verify(backOff, times(3)).start();
	}

	void testEarlyExitBackOff() {
		DefaultAfterRollbackProcessor<String, String> processor = new DefaultAfterRollbackProcessor<>(
				new FixedBackOff(1, 10_000));
		@SuppressWarnings("unchecked")
		Consumer<String, String> consumer = mock(Consumer.class);
		ConsumerRecord<String, String> record1 = new ConsumerRecord<>("foo", 0, 0L, "foo", "bar");
		ConsumerRecord<String, String> record2 = new ConsumerRecord<>("foo", 1, 1L, "foo", "bar");
		List<ConsumerRecord<String, String>> records = Arrays.asList(record1, record2);
		IllegalStateException illegalState = new IllegalStateException();
		MessageListenerContainer container = mock(MessageListenerContainer.class);
		given(container.isRunning()).willReturn(false);
		long t1 = System.currentTimeMillis();
		processor.process(records, consumer, container, illegalState, true, EOSMode.V2);
		assertThat(System.currentTimeMillis() < t1 + 5_000);
	}

	@Test
	void testNoEarlyExitBackOff() {
		DefaultAfterRollbackProcessor<String, String> processor = new DefaultAfterRollbackProcessor<>(
				new FixedBackOff(1, 200));
		@SuppressWarnings("unchecked")
		Consumer<String, String> consumer = mock(Consumer.class);
		ConsumerRecord<String, String> record1 = new ConsumerRecord<>("foo", 0, 0L, "foo", "bar");
		ConsumerRecord<String, String> record2 = new ConsumerRecord<>("foo", 1, 1L, "foo", "bar");
		List<ConsumerRecord<String, String>> records = Arrays.asList(record1, record2);
		IllegalStateException illegalState = new IllegalStateException();
		MessageListenerContainer container = mock(MessageListenerContainer.class);
		given(container.isRunning()).willReturn(true);
		long t1 = System.currentTimeMillis();
		processor.process(records, consumer, container, illegalState, true, EOSMode.V2);
		assertThat(System.currentTimeMillis() >= t1 + 200);
	}

}
