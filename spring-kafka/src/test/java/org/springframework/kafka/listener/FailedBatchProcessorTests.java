/*
 * Copyright 2023 the original author or authors.
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
import static org.assertj.core.api.Assertions.assertThatIllegalStateException;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.BDDMockito.willThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;

import java.util.List;
import java.util.Map;
import java.util.function.BiConsumer;
import java.util.function.Supplier;

import org.apache.commons.logging.LogFactory;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import org.springframework.core.log.LogAccessor;
import org.springframework.data.util.DirectFieldAccessFallbackBeanWrapper;
import org.springframework.util.backoff.BackOff;
import org.springframework.util.backoff.FixedBackOff;

/**
 * @author Gary Russell
 * @since 3.0.3
 *
 */
public class FailedBatchProcessorTests {

	@SuppressWarnings({ "rawtypes", "unchecked" })
	@Test
	void indexOutOfBounds() {
		class TestFBP extends FailedBatchProcessor {

			TestFBP(BiConsumer<ConsumerRecord<?, ?>, Exception> recoverer, BackOff backOff,
					CommonErrorHandler fallbackHandler) {

				super(recoverer, backOff, fallbackHandler);
			}

		}
		CommonErrorHandler mockEH = mock(CommonErrorHandler.class);
		willThrow(new IllegalStateException("fallback")).given(mockEH).handleBatch(any(), any(), any(), any(), any());

		TestFBP testFBP = new TestFBP((rec, ex) -> { }, new FixedBackOff(0L, 0L), mockEH);
		LogAccessor logger = spy(new LogAccessor(LogFactory.getLog("test")));
		new DirectFieldAccessFallbackBeanWrapper(testFBP).setPropertyValue("logger", logger);


		ConsumerRecords records = new ConsumerRecords(Map.of(new TopicPartition("topic", 0),
				List.of(mock(ConsumerRecord.class), mock(ConsumerRecord.class))));
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
		class TestFBP extends FailedBatchProcessor {

			TestFBP(BiConsumer<ConsumerRecord<?, ?>, Exception> recoverer, BackOff backOff,
					CommonErrorHandler fallbackHandler) {

				super(recoverer, backOff, fallbackHandler);
			}

		}
		CommonErrorHandler mockEH = mock(CommonErrorHandler.class);
		willThrow(new IllegalStateException("fallback")).given(mockEH).handleBatch(any(), any(), any(), any(), any());

		TestFBP testFBP = new TestFBP((rec, ex) -> { }, new FixedBackOff(0L, 0L), mockEH);
		LogAccessor logger = spy(new LogAccessor(LogFactory.getLog("test")));
		new DirectFieldAccessFallbackBeanWrapper(testFBP).setPropertyValue("logger", logger);


		ConsumerRecord rec1 = new ConsumerRecord("topic", 0, 0L, null, null);
		ConsumerRecord rec2 = new ConsumerRecord("topic", 0, 1L, null, null);
		ConsumerRecords records = new ConsumerRecords(Map.of(new TopicPartition("topic", 0), List.of(rec1, rec2)));
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

}
