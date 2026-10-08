/*
 * Copyright 2021-present the original author or authors.
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

import java.time.Duration;
import java.util.List;
import java.util.Map;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import org.springframework.util.backoff.FixedBackOff;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.same;
import static org.mockito.BDDMockito.given;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.verifyNoMoreInteractions;

/**
 * @author Gary Russell
 * @author Cobi Eun
 * @since 2.8
 *
 */
public class CommonMixedErrorHandlerTests {

	private static final TopicPartition TP = new TopicPartition("topic", 0);

	@Test
	void handleBatchAndReturnRemainingShouldReturnRemainingFromBatchHandler() {
		DefaultErrorHandler recordHandler = noSeekErrorHandler();
		DefaultErrorHandler batchHandler = noSeekErrorHandler();
		CommonMixedErrorHandler mixed = new CommonMixedErrorHandler(recordHandler, batchHandler);
		Consumer<?, ?> consumer = mock(Consumer.class);
		MessageListenerContainer container = mock(MessageListenerContainer.class);
		ContainerProperties properties = new ContainerProperties("topic");
		properties.setSyncCommitTimeout(Duration.ofSeconds(1));
		given(container.getContainerProperties()).willReturn(properties);
		given(container.isRunning()).willReturn(true);

		try {
			ConsumerRecords<String, String> remaining = mixed.handleBatchAndReturnRemaining(
					new BatchListenerFailedException("failure", 1), threeRecords(), consumer, container, () -> { });
			assertThat(remaining.count()).isEqualTo(2);
			assertThat(remaining.records(TP)).extracting(ConsumerRecord::offset).containsExactly(1L, 2L);
			verify(consumer, never()).seek(any(), anyLong());
			verify(consumer, never()).seek(any(), any(OffsetAndMetadata.class));
			ArgumentCaptor<Map<TopicPartition, OffsetAndMetadata>> offsets = ArgumentCaptor.captor();
			verify(consumer).commitSync(offsets.capture(), eq(Duration.ofSeconds(1)));
			assertThat(offsets.getValue().get(TP).offset()).isEqualTo(1L);
		}
		finally {
			mixed.clearThreadState();
		}
	}

	@Test
	void handleBatchAndReturnRemainingShouldDelegateToBatchHandlerOnlyAndReturnItsResultAsIs() {
		CommonErrorHandler record = mock(CommonErrorHandler.class);
		CommonErrorHandler batch = mock(CommonErrorHandler.class);
		CommonMixedErrorHandler mixed = new CommonMixedErrorHandler(record, batch);
		Exception exception = new BatchListenerFailedException("failure", 1);
		ConsumerRecords<String, String> data = threeRecords();
		ConsumerRecords<String, String> remaining = threeRecords();
		Consumer<?, ?> consumer = mock(Consumer.class);
		MessageListenerContainer container = mock(MessageListenerContainer.class);
		Runnable invokeListener = mock(Runnable.class);
		given(batch.<String, String>handleBatchAndReturnRemaining(exception, data, consumer, container, invokeListener))
				.willReturn(remaining);

		assertThat(mixed.handleBatchAndReturnRemaining(exception, data, consumer, container, invokeListener))
				.isSameAs(remaining);
		verify(batch).handleBatchAndReturnRemaining(same(exception), same(data), same(consumer), same(container),
				same(invokeListener));
		verifyNoInteractions(record);
	}

	@Test
	void testMixed() {
		CommonErrorHandler record = mock(CommonErrorHandler.class);
		CommonErrorHandler batch = mock(CommonErrorHandler.class);
		CommonMixedErrorHandler mixed = new CommonMixedErrorHandler(record, batch);
		mixed.handleBatch(null, null, null, null, null);
		verify(batch).handleBatch(null, null, null, null, null);
		mixed.handleOne(null, null, null, null);
		verify(record).handleOne(null, null, null, null);
		mixed.handleRemaining(null, null, null, null);
		verify(record).handleRemaining(null, null, null, null);
		mixed.handleOtherException(null, null, null, false);
		verify(record).handleOtherException(null, null, null, false);
		mixed.handleOtherException(null, null, null, true);
		verify(batch).handleOtherException(null, null, null, true);
		verifyNoMoreInteractions(record);
		verifyNoMoreInteractions(batch);
	}

	private static DefaultErrorHandler noSeekErrorHandler() {
		DefaultErrorHandler handler = new DefaultErrorHandler((record, exception) -> {
			throw new AssertionError("Unexpected recovery");
		}, new FixedBackOff(0, 9));
		handler.setSeekAfterError(false);
		return handler;
	}

	private static ConsumerRecords<String, String> threeRecords() {
		return new ConsumerRecords<>(Map.of(TP, List.of(
				new ConsumerRecord<>("topic", 0, 0L, "k0", "v0"),
				new ConsumerRecord<>("topic", 0, 1L, "k1", "v1"),
				new ConsumerRecord<>("topic", 0, 2L, "k2", "v2"))), Map.of());
	}

}
