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

import org.junit.jupiter.api.Test;

import org.springframework.kafka.test.utils.KafkaTestUtils;

/**
 * @author Gary Russell
 * @since 2.3.6
 *
 */
public class FailedRecordProcessorTests {

	@Test
	void testDefaultBackOff() {
		FailedRecordProcessor frp = new FailedRecordProcessor(null, 1) {
		};
		assertThat(KafkaTestUtils.getPropertyValue(frp, "failureTracker.backOff.interval", Long.class)).isEqualTo(0L);
		assertThat(KafkaTestUtils.getPropertyValue(frp, "failureTracker.backOff.maxAttempts", Long.class))
				.isEqualTo(0L);
		frp = new FailedRecordProcessor(null, 0) {
		};
		assertThat(KafkaTestUtils.getPropertyValue(frp, "failureTracker.backOff.interval", Long.class)).isEqualTo(0L);
		assertThat(KafkaTestUtils.getPropertyValue(frp, "failureTracker.backOff.maxAttempts", Long.class))
				.isEqualTo(0L);
		frp = new FailedRecordProcessor(null, -1) {
		};
		assertThat(KafkaTestUtils.getPropertyValue(frp, "failureTracker.backOff.interval", Long.class)).isEqualTo(0L);
		assertThat(KafkaTestUtils.getPropertyValue(frp, "failureTracker.backOff.maxAttempts", Long.class))
				.isEqualTo(Long.MAX_VALUE);
	}

}
