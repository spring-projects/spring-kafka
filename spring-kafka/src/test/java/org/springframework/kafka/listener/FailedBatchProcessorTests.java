/*
 * Copyright 2023-present the original author or authors.
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

import java.util.List;
import java.util.Map;
import java.util.function.BiConsumer;
import java.util.function.Supplier;

import org.apache.commons.logging.LogFactory;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.RebalanceInProgressException;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import org.springframework.core.log.LogAccessor;
import org.springframework.data.util.DirectFieldAccessFallbackBeanWrapper;
import org.springframework.util.backoff.BackOff;
import org.springframework.util.backoff.FixedBackOff;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;
import static org.assertj.core.api.Assertions.assertThatIllegalStateException;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyMap;
import static org.mockito.BDDMockito.willReturn;
import static org.mockito.BDDMockito.willThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;

/**
 * @author Gary Russell
 * @author Francois Rosiere
 * @author Soby Chacko
 * @since 3.0.3
 *
 */
public class FailedBatchProcessorTests {

	@SuppressWarnings({ "rawtypes", "unchecked" })
	@Test
	void indexOutOfBounds() {
		CommonErrorHandler mockEH = mock(CommonErrorHandler.class);
		willThrow(new IllegalStateException("fallback")).given(mockEH).handleBatch(any(), any(), any(), any(), any());

		TestFBP testFBP = new TestFBP((rec, ex) -> { }, new FixedBackOff(0L, 0L), mockEH);
		LogAccessor logger = spy(new LogAccessor(LogFactory.getLog("test")));
		new DirectFieldAccessFallbackBeanWrapper(testFBP).setPropertyValue("logger", logger);


		ConsumerRecords records = new ConsumerRecords(Map.of(new TopicPartition("topic", 0),
				List.of(mock(ConsumerRecord.class), mock(ConsumerRecord.class))), Map.of());
		assertThatIllegalStateException().isThrownBy(() -> testFBP.handle(new BatchListenerFailedException("test", 3),
					records, mock(Consumer.class), mock(MessageListenerContainer.class), mock(Runnable.class)))
				.withMessage("fallback");
		ArgumentCaptor<Supplier<String>> captor = ArgumentCaptor.forClass(Supplier.class);
		verify(logger).warn(any(BatchListenerFailedException.class), captor.capture());
		String output = captor.getValue().get();
		assertThat(output).contains("Record not found in batch, index 3 out of bounds (0, 1);");
	}

	@SuppressWarnings({ "rawtypes", "unchecked" })
	@Test
	void recordNotPresent() {
		CommonErrorHandler mockEH = mock(CommonErrorHandler.class);
		willThrow(new IllegalStateException("fallback")).given(mockEH).handleBatch(any(), any(), any(), any(), any());

		TestFBP testFBP = new TestFBP((rec, ex) -> { }, new FixedBackOff(0L, 0L), mockEH);
		LogAccessor logger = spy(new LogAccessor(LogFactory.getLog("test")));
		new DirectFieldAccessFallbackBeanWrapper(testFBP).setPropertyValue("logger", logger);


		ConsumerRecord rec1 = new ConsumerRecord("topic", 0, 0L, null, null);
		ConsumerRecord rec2 = new ConsumerRecord("topic", 0, 1L, null, null);
		ConsumerRecords records = new ConsumerRecords(Map.of(new TopicPartition("topic", 0), List.of(rec1, rec2)),
				Map.of());
		ConsumerRecord unknownRecord = new ConsumerRecord("topic", 42, 123L, null, null);
		assertThatIllegalStateException().isThrownBy(() ->
					testFBP.handle(new BatchListenerFailedException("topic", unknownRecord),
							records, mock(Consumer.class), mock(MessageListenerContainer.class), mock(Runnable.class)))
				.withMessage("fallback");
		ArgumentCaptor<Supplier<String>> captor = ArgumentCaptor.forClass(Supplier.class);
		verify(logger).warn(any(BatchListenerFailedException.class), captor.capture());
		String output = captor.getValue().get();
		assertThat(output).contains("Record not found in batch: topic-42@123;");
	}

	@SuppressWarnings({ "rawtypes", "unchecked" })
	@Test
	void testExceptionDuringCommit() {
		CommonErrorHandler mockEH = mock(CommonErrorHandler.class);
		willThrow(new IllegalStateException("ise")).given(mockEH).handleBatch(any(), any(), any(), any(), any());

		ConsumerRecord rec1 = new ConsumerRecord("topic", 0, 0L, null, null);
		ConsumerRecord rec2 = new ConsumerRecord("topic", 0, 1L, null, null);
		ConsumerRecord rec3 = new ConsumerRecord("topic", 0, 2L, null, null);

		ConsumerRecords records = new ConsumerRecords(Map.of(new TopicPartition("topic", 0), List.of(rec1, rec2, rec3)),
				Map.of());
		TestFBP testFBP = new TestFBP((rec, ex) -> { }, new FixedBackOff(2L, 2L), mockEH);
		final Consumer consumer = mock(Consumer.class);
		willThrow(new RebalanceInProgressException("rebalance in progress")).given(consumer).commitSync(anyMap(), any());
		final MessageListenerContainer mockMLC = mock(MessageListenerContainer.class);
		willReturn(new ContainerProperties("topic")).given(mockMLC).getContainerProperties();
		assertThatExceptionOfType(RecordInRetryException.class).isThrownBy(() ->
				testFBP.handle(new BatchListenerFailedException("topic", rec2),
						records, consumer, mockMLC, mock(Runnable.class))
		).withMessage("Record in retry and not yet recovered");
	}

	static class TestFBP extends FailedBatchProcessor {

		TestFBP(BiConsumer<ConsumerRecord<?, ?>, Exception> recoverer, BackOff backOff,
				CommonErrorHandler fallbackHandler) {

			super(recoverer, backOff, fallbackHandler);
		}

	}
}
