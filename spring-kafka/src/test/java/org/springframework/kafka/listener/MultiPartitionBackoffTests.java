/*
 * Copyright 2026-present the original author or authors.
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

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.Test;

import org.springframework.kafka.support.TopicPartitionOffset;
import org.springframework.util.backoff.FixedBackOff;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.BDDMockito.given;
import static org.mockito.Mockito.mock;

/**
 * Tests for partition-specific state clearing in multi-partition environments.
 * @since 4.1
 */
public class MultiPartitionBackoffTests {

	@Test
	void testErrorHandlerClearTopicPartitionState() {
		DefaultErrorHandler errorHandler = new DefaultErrorHandler(new FixedBackOff(1000L, 10));
		ConsumerRecord<String, String> record0 = new ConsumerRecord<>("foo", 0, 0L, "foo", "bar");
		ConsumerRecord<String, String> record1 = new ConsumerRecord<>("foo", 1, 0L, "foo", "bar");
		Consumer<?, ?> consumer = mock(Consumer.class);
		MessageListenerContainer container = mock(MessageListenerContainer.class);

		// Fail partition 0
		errorHandler.handleOne(new RuntimeException(), record0, consumer, container);
		// Fail partition 1
		errorHandler.handleOne(new RuntimeException(), record1, consumer, container);

		assertThat(errorHandler.deliveryAttempt(new TopicPartitionOffset("foo", 0, 0L))).isEqualTo(2);
		assertThat(errorHandler.deliveryAttempt(new TopicPartitionOffset("foo", 1, 0L))).isEqualTo(2);

		// Clear only partition 1
		errorHandler.clearTopicPartitionState(new TopicPartition("foo", 1));

		assertThat(errorHandler.deliveryAttempt(new TopicPartitionOffset("foo", 0, 0L))).isEqualTo(2); // Still 2
		assertThat(errorHandler.deliveryAttempt(new TopicPartitionOffset("foo", 1, 0L))).isEqualTo(1); // Reset to 1

		// Clear thread state
		errorHandler.clearThreadState();
		assertThat(errorHandler.deliveryAttempt(new TopicPartitionOffset("foo", 0, 0L))).isEqualTo(1);
	}

	@Test
	void testAfterRollbackProcessorClearTopicPartitionState() {
		DefaultAfterRollbackProcessor<String, String> processor = new DefaultAfterRollbackProcessor<>(new FixedBackOff(1000L, 10));
		ConsumerRecord<String, String> record0 = new ConsumerRecord<>("foo", 0, 0L, "foo", "bar");
		ConsumerRecord<String, String> record1 = new ConsumerRecord<>("foo", 1, 0L, "foo", "bar");
		@SuppressWarnings("unchecked")
		Consumer<String, String> consumer = mock(Consumer.class);
		MessageListenerContainer container = mock(MessageListenerContainer.class);
		given(container.getContainerProperties()).willReturn(new ContainerProperties("foo"));

		// Use process to simulate failure and tracking
		processor.process(List.of(record0), consumer, container, new RuntimeException(), true, ContainerProperties.EOSMode.V2);
		processor.process(List.of(record1), consumer, container, new RuntimeException(), true, ContainerProperties.EOSMode.V2);

		assertThat(processor.deliveryAttempt(new TopicPartitionOffset("foo", 0, 0L))).isEqualTo(2);
		assertThat(processor.deliveryAttempt(new TopicPartitionOffset("foo", 1, 0L))).isEqualTo(2);

		// Clear only partition 0
		processor.clearTopicPartitionState(new TopicPartition("foo", 0));

		assertThat(processor.deliveryAttempt(new TopicPartitionOffset("foo", 0, 0L))).isEqualTo(1);
		assertThat(processor.deliveryAttempt(new TopicPartitionOffset("foo", 1, 0L))).isEqualTo(2);

		// Clear thread state
		processor.clearThreadState();
		assertThat(processor.deliveryAttempt(new TopicPartitionOffset("foo", 1, 0L))).isEqualTo(1);
	}

}
