/*
 * Copyright 2020 the original author or authors.
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
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.Test;

import org.springframework.util.backoff.FixedBackOff;

/**
 * @author Gary Russell
 * @since 2.3.7
 *
 */
public class RetryingBatchErrorHandlerTests {

	private int invoked;

	@Test
	void recover() {
		this.invoked = 0;
		List<ConsumerRecord<?, ?>> recovered = new ArrayList<>();
		RetryingBatchErrorHandler eh = new RetryingBatchErrorHandler(new FixedBackOff(0L, 3L), (cr, ex) ->  {
			recovered.add(cr);
		});
		Map<TopicPartition, List<ConsumerRecord<Object, Object>>> map = new HashMap<>();
		map.put(new TopicPartition("foo", 0),
				Collections.singletonList(new ConsumerRecord<>("foo", 0, 0L, "foo", "bar")));
		map.put(new TopicPartition("foo", 1),
				Collections.singletonList(new ConsumerRecord<>("foo", 1, 0L, "foo", "bar")));
		ConsumerRecords<?, ?> records = new ConsumerRecords<>(map);
		Consumer<?, ?> consumer = mock(Consumer.class);
		MessageListenerContainer container = mock(MessageListenerContainer.class);
		eh.handle(new RuntimeException(), records, consumer, container, () -> {
			this.invoked++;
			throw new RuntimeException();
		});
		assertThat(this.invoked).isEqualTo(3);
		assertThat(recovered).hasSize(2);
		verify(consumer).pause(any());
		verify(consumer, times(3)).poll(any());
		verify(consumer).resume(any());
		verify(consumer, times(2)).assignment();
		verifyNoMoreInteractions(consumer);
	}

	@Test
	void successOnRetry() {
		this.invoked = 0;
		List<ConsumerRecord<?, ?>> recovered = new ArrayList<>();
		RetryingBatchErrorHandler eh = new RetryingBatchErrorHandler(new FixedBackOff(0L, 3L), (cr, ex) ->  {
			recovered.add(cr);
		});
		Map<TopicPartition, List<ConsumerRecord<Object, Object>>> map = new HashMap<>();
		map.put(new TopicPartition("foo", 0),
				Collections.singletonList(new ConsumerRecord<>("foo", 0, 0L, "foo", "bar")));
		map.put(new TopicPartition("foo", 1),
				Collections.singletonList(new ConsumerRecord<>("foo", 1, 0L, "foo", "bar")));
		ConsumerRecords<?, ?> records = new ConsumerRecords<>(map);
		Consumer<?, ?> consumer = mock(Consumer.class);
		MessageListenerContainer container = mock(MessageListenerContainer.class);
		eh.handle(new RuntimeException(), records, consumer, container, () -> this.invoked++);
		assertThat(this.invoked).isEqualTo(1);
		assertThat(recovered).hasSize(0);
		verify(consumer).pause(any());
		verify(consumer).poll(any());
		verify(consumer).resume(any());
		verify(consumer, times(2)).assignment();
		verifyNoMoreInteractions(consumer);
	}

	@Test
	void recoveryFails() {
		this.invoked = 0;
		List<ConsumerRecord<?, ?>> recovered = new ArrayList<>();
		RetryingBatchErrorHandler eh = new RetryingBatchErrorHandler(new FixedBackOff(0L, 3L), (cr, ex) ->  {
			recovered.add(cr);
			throw new RuntimeException("can't recover");
		});
		Map<TopicPartition, List<ConsumerRecord<Object, Object>>> map = new HashMap<>();
		map.put(new TopicPartition("foo", 0),
				Collections.singletonList(new ConsumerRecord<>("foo", 0, 0L, "foo", "bar")));
		map.put(new TopicPartition("foo", 1),
				Collections.singletonList(new ConsumerRecord<>("foo", 1, 0L, "foo", "bar")));
		ConsumerRecords<?, ?> records = new ConsumerRecords<>(map);
		Consumer<?, ?> consumer = mock(Consumer.class);
		MessageListenerContainer container = mock(MessageListenerContainer.class);
		assertThatExceptionOfType(RuntimeException.class).isThrownBy(() ->
		eh.handle(new RuntimeException(), records, consumer, container, () -> {
			this.invoked++;
			throw new RuntimeException();
		}));
		assertThat(this.invoked).isEqualTo(3);
		assertThat(recovered).hasSize(1);
		verify(consumer).pause(any());
		verify(consumer, times(3)).poll(any());
		verify(consumer).resume(any());
		verify(consumer, times(2)).assignment();
		verify(consumer).seek(new TopicPartition("foo", 0), 0L);
		verify(consumer).seek(new TopicPartition("foo", 1), 0L);
	}

}
