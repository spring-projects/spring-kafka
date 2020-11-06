/*
 * Copyright 2019-2020 the original author or authors.
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
import static org.mockito.BDDMockito.given;
import static org.mockito.Mockito.mock;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.Test;

import org.springframework.core.log.LogAccessor;
import org.springframework.kafka.support.TopicPartitionOffset;
import org.springframework.kafka.test.utils.KafkaTestUtils;
import org.springframework.util.backoff.BackOff;
import org.springframework.util.backoff.BackOffExecution;
import org.springframework.util.backoff.FixedBackOff;

/**
 * @author Gary Russell
 * @since 2.2.5
 *
 */
public class FailedRecordTrackerTests {

	@Test
	void testNoRetries() {
		AtomicBoolean recovered = new AtomicBoolean();
		FailedRecordTracker tracker = new FailedRecordTracker((r, e) -> {
			recovered.set(true);
		}, new FixedBackOff(0L, 0L), mock(LogAccessor.class));
		ConsumerRecord<?, ?> record = new ConsumerRecord<>("foo", 0, 0L, "bar", "baz");
		assertThat(tracker.skip(record, new RuntimeException())).isTrue();
		assertThat(recovered.get()).isTrue();
	}

	@Test
	void testThreeRetries() {
		AtomicBoolean recovered = new AtomicBoolean();
		FailedRecordTracker tracker = new FailedRecordTracker((r, e) -> {
			recovered.set(true);
		}, new FixedBackOff(0L, 3L), mock(LogAccessor.class));
		ConsumerRecord<?, ?> record = new ConsumerRecord<>("foo", 0, 0L, "bar", "baz");
		assertThat(tracker.skip(record, new RuntimeException())).isFalse();
		assertThat(tracker.skip(record, new RuntimeException())).isFalse();
		assertThat(tracker.skip(record, new RuntimeException())).isFalse();
		assertThat(tracker.skip(record, new RuntimeException())).isTrue();
		assertThat(recovered.get()).isTrue();
	}

	@Test
	void testSuccessAfterFailure() {
		FailedRecordTracker tracker = new FailedRecordTracker(null, new FixedBackOff(0L, 1L), mock(LogAccessor.class));
		ConsumerRecord<?, ?> record = new ConsumerRecord<>("foo", 0, 0L, "bar", "baz");
		assertThat(tracker.skip(record, new RuntimeException())).isFalse();
		record = new ConsumerRecord<>("bar", 0, 0L, "bar", "baz");
		assertThat(tracker.skip(record, new RuntimeException())).isFalse();
		record = new ConsumerRecord<>("bar", 1, 0L, "bar", "baz");
		assertThat(tracker.skip(record, new RuntimeException())).isFalse();
		record = new ConsumerRecord<>("bar", 1, 1L, "bar", "baz");
		assertThat(tracker.skip(record, new RuntimeException())).isFalse();
		record = new ConsumerRecord<>("bar", 1, 1L, "bar", "baz");
		assertThat(tracker.skip(record, new RuntimeException())).isTrue();
	}

	@Test
	void testDifferentOrder() {
		List<ConsumerRecord<?, ?>> records = new ArrayList<>();
		FailedRecordTracker tracker = new FailedRecordTracker((rec, ex) -> {
			records.add(rec);
		}, new FixedBackOff(0L, 2L), mock(LogAccessor.class));
		ConsumerRecord<?, ?> record1 = new ConsumerRecord<>("foo", 0, 0L, "bar", "baz");
		ConsumerRecord<?, ?> record2 = new ConsumerRecord<>("foo", 1, 0L, "bar", "baz");
		assertThat(tracker.skip(record1, new RuntimeException())).isFalse();
		assertThat(tracker.skip(record2, new RuntimeException())).isFalse();
		assertThat(tracker.skip(record1, new RuntimeException())).isFalse();
		assertThat(tracker.skip(record2, new RuntimeException())).isFalse();
		assertThat(tracker.skip(record1, new RuntimeException())).isTrue();
		assertThat(tracker.skip(record2, new RuntimeException())).isTrue();
		assertThat(records).hasSize(2);
	}

	@Test
	void multiBackOffs() {
		BackOff bo1 = mock(BackOff.class);
		BackOffExecution be1 = mock(BackOffExecution.class);
		given(bo1.start()).willReturn(be1);
		BackOff bo2 = mock(BackOff.class);
		BackOffExecution be2 = mock(BackOffExecution.class);
		given(bo2.start()).willReturn(be2);
		FailedRecordTracker tracker = new FailedRecordTracker((rec, ex) -> { }, bo1, mock(LogAccessor.class));
		tracker.setBackOffFunction((record, ex) -> {
			if (record.topic().equals("foo")) {
				return bo2;
			}
			else {
				return null;
			}
		});
		@SuppressWarnings("unchecked")
		ThreadLocal<Map<TopicPartition, Object>> failures = (ThreadLocal<Map<TopicPartition, Object>>) KafkaTestUtils
				.getPropertyValue(tracker, "failures");
		ConsumerRecord<?, ?> record1 = new ConsumerRecord<>("foo", 0, 0L, "bar", "baz");
		tracker.skip(record1, new RuntimeException());
		assertThat(KafkaTestUtils.getPropertyValue(failures.get()
					.get(new TopicPartition("foo", 0)), "backOffExecution"))
				.isSameAs(be2);
		ConsumerRecord<?, ?> record2 = new ConsumerRecord<>("bar", 0, 0L, "bar", "baz");
		tracker.skip(record2, new RuntimeException());
		assertThat(KafkaTestUtils.getPropertyValue(failures.get()
					.get(new TopicPartition("bar", 0)), "backOffExecution"))
				.isSameAs(be1);
	}

	@Test
	void exceptionChangesNoReset() {
		exceptionChanges(false);
	}

	@Test
	void exceptionChangesReset() {
		exceptionChanges(true);
	}

	void exceptionChanges(boolean reset) {
		BackOff bo1 = mock(BackOff.class);
		BackOffExecution be1 = mock(BackOffExecution.class);
		given(bo1.start()).willReturn(be1);
		BackOff bo2 = mock(BackOff.class);
		BackOffExecution be2 = mock(BackOffExecution.class);
		given(bo2.start()).willReturn(be2);
		FailedRecordTracker tracker = new FailedRecordTracker((rec, ex) -> { }, bo1, mock(LogAccessor.class));
		tracker.setBackOffFunction((record, ex) -> {
			if (ex instanceof IllegalStateException) {
				return bo1;
			}
			else {
				return bo2;
			}
		});
		if (reset) {
			tracker.setResetStateOnExceptionChange(reset);
		}
		@SuppressWarnings("unchecked")
		ThreadLocal<Map<TopicPartition, Object>> failures = (ThreadLocal<Map<TopicPartition, Object>>) KafkaTestUtils
				.getPropertyValue(tracker, "failures");
		ConsumerRecord<?, ?> record1 = new ConsumerRecord<>("foo", 0, 0L, "bar", "baz");
		tracker.skip(record1, new IllegalStateException());
		assertThat(KafkaTestUtils.getPropertyValue(failures.get()
				.get(new TopicPartition("foo", 0)), "backOffExecution"))
			.isSameAs(be1);
		TopicPartitionOffset tpo = new TopicPartitionOffset("foo", 0, 0L);
		assertThat(tracker.deliveryAttempt(tpo)).isEqualTo(2);
		tracker.skip(record1, new IllegalStateException());
		assertThat(tracker.deliveryAttempt(tpo)).isEqualTo(3);
		tracker.skip(record1, new IllegalArgumentException());
		if (reset) {
			assertThat(tracker.deliveryAttempt(tpo)).isEqualTo(2);
			assertThat(KafkaTestUtils.getPropertyValue(failures.get()
					.get(new TopicPartition("foo", 0)), "backOffExecution"))
				.isSameAs(be2);
		}
		else {
			assertThat(tracker.deliveryAttempt(tpo)).isEqualTo(4);
			assertThat(KafkaTestUtils.getPropertyValue(failures.get()
					.get(new TopicPartition("foo", 0)), "backOffExecution"))
				.isSameAs(be1);
		}
	}

}
