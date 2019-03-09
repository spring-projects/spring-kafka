/*
 * Copyright 2019 the original author or authors.
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

package org.springframework.kafka.listener;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;

import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.commons.logging.Log;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.Test;

/**
 * @author Gary Russell
 * @since 2.2.5
 *
 */
public class FailedRecordTrackerTests {

	@Test
	public void testNoRetries() {
		AtomicBoolean recovered = new AtomicBoolean();
		FailedRecordTracker tracker = new FailedRecordTracker((r, e) -> {
			recovered.set(true);
		}, 1, mock(Log.class));
		ConsumerRecord<?, ?> record = new ConsumerRecord<>("foo", 0, 0L, "bar", "baz");
		assertThat(tracker.skip(record, new RuntimeException())).isTrue();
		assertThat(recovered.get()).isTrue();
	}

	@Test
	public void testThreeRetries() {
		AtomicBoolean recovered = new AtomicBoolean();
		FailedRecordTracker tracker = new FailedRecordTracker((r, e) -> {
			recovered.set(true);
		}, 4, mock(Log.class));
		ConsumerRecord<?, ?> record = new ConsumerRecord<>("foo", 0, 0L, "bar", "baz");
		assertThat(tracker.skip(record, new RuntimeException())).isFalse();
		assertThat(tracker.skip(record, new RuntimeException())).isFalse();
		assertThat(tracker.skip(record, new RuntimeException())).isFalse();
		assertThat(tracker.skip(record, new RuntimeException())).isTrue();
		assertThat(recovered.get()).isTrue();
	}

}
