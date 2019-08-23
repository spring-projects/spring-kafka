/*
 * Copyright 2017-2019 the original author or authors.
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

package org.springframework.kafka.test.hamcrest;

import static org.assertj.core.api.Assertions.assertThatExceptionOfType;
import static org.junit.Assert.assertThat;
import static org.springframework.kafka.test.hamcrest.KafkaMatchers.hasKey;
import static org.springframework.kafka.test.hamcrest.KafkaMatchers.hasPartition;
import static org.springframework.kafka.test.hamcrest.KafkaMatchers.hasTimestamp;
import static org.springframework.kafka.test.hamcrest.KafkaMatchers.hasValue;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.record.TimestampType;
import org.junit.jupiter.api.Test;

/**
 * @author Biju Kunjummen
 *
 * @since 1.3
 */
public class KafkaMatchersTests {

	@Test
	public void testKeyMatcher() {
		ConsumerRecord<String, String> record = new ConsumerRecord<>("topic", 0, 10,
				1487694048607L, TimestampType.CREATE_TIME, 123L, 2, 3, "key1", "value1");
		assertThat(record, hasKey("key1"));
		assertThat(record, hasValue("value1"));
		assertThat(record, hasPartition(0));
		assertThat(record, hasTimestamp(1487694048607L));
		assertThat(record, hasTimestamp(TimestampType.CREATE_TIME, 1487694048607L));
	}

	@Test
	public void noMatchOnTimestamp() {
		ConsumerRecord<String, String> record = new ConsumerRecord<>("topic", 0, 10,
				1487694048607L, TimestampType.CREATE_TIME, 123L, 2, 3, "key1", "value1");

		assertThatExceptionOfType(AssertionError.class).isThrownBy(() -> assertThat(record, hasTimestamp(123L)))
				.withMessageContaining(
						"Expected: a ConsumerRecord with timestamp of type: <CreateTime> and value: <123L>");
	}

}
