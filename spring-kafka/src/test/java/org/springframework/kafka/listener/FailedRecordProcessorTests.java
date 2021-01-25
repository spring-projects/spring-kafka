/*
 * Copyright 2020-2021 the original author or authors.
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

import java.util.Collections;
import java.util.List;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.Test;

import org.springframework.kafka.support.TopicPartitionOffset;
import org.springframework.util.backoff.FixedBackOff;

/**
 * @author Gary Russell
 * @since 2.3.6
 *
 */
public class FailedRecordProcessorTests {

	@Test
	void deliveryAttempts() throws InterruptedException {
		FailedRecordProcessor frp = new FailedRecordProcessor(null, new FixedBackOff(0, 2)) {
		};
		TopicPartitionOffset tpo1 = new TopicPartitionOffset("foo", 0, 0L);
		assertThat(frp.deliveryAttempt(tpo1)).isEqualTo(1);
		List<ConsumerRecord<?, ?>> records = Collections
				.singletonList(new ConsumerRecord<Object, Object>("foo", 0, 0L, null, null));
		RuntimeException exception = new RuntimeException();
		frp.getRecoveryStrategy(records, exception).recovered(records.get(0), exception, null);
		assertThat(frp.deliveryAttempt(tpo1)).isEqualTo(2);
		frp.getRecoveryStrategy(records, exception).recovered(records.get(0), exception, null);
		assertThat(frp.deliveryAttempt(tpo1)).isEqualTo(3);
		frp.getRecoveryStrategy(records, exception).recovered(records.get(0), exception, null);
		assertThat(frp.deliveryAttempt(tpo1)).isEqualTo(1);
		frp.getRecoveryStrategy(records, exception).recovered(records.get(0), exception, null);
		assertThat(frp.deliveryAttempt(tpo1)).isEqualTo(2);
		assertThat(frp.deliveryAttempt(tpo1)).isEqualTo(2);
		// new partition
		TopicPartitionOffset tpo2 = new TopicPartitionOffset("foo", 1, 0L);
		assertThat(frp.deliveryAttempt(tpo2)).isEqualTo(1);
		frp.getRecoveryStrategy(records, exception).recovered(new ConsumerRecord<Object, Object>("foo", 1, 0L, null, null),
				exception, null);
		assertThat(frp.deliveryAttempt(tpo2)).isEqualTo(2);
		// new offset
		tpo2 = new TopicPartitionOffset("foo", 1, 1L);
		assertThat(frp.deliveryAttempt(tpo2)).isEqualTo(1);
		frp.getRecoveryStrategy(records, exception).recovered(new ConsumerRecord<Object, Object>("foo", 1, 1L, null, null),
				exception, null);
		assertThat(frp.deliveryAttempt(tpo2)).isEqualTo(2);
		// back to original
		frp.getRecoveryStrategy(records, exception).recovered(records.get(0), exception, null);
		assertThat(frp.deliveryAttempt(tpo1)).isEqualTo(3);
	}

}
