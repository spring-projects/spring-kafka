/*
 * Copyright 2019 the original author or authors.
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

package org.springframework.kafka.test.assertj;

import static org.assertj.core.api.Assertions.allOf;
import static org.assertj.core.api.Assertions.assertThat;
import static org.springframework.kafka.test.assertj.KafkaConditions.keyValue;
import static org.springframework.kafka.test.assertj.KafkaConditions.partition;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.Test;

/**
 * @author Gary Russell
 * @since 2.2.12
 *
 */
public class KafkaConditionsTests {

	@Test
	void testKeyValue() {
		ConsumerRecord<Integer, String> record = new ConsumerRecord<>("topic", 42, 0, 23, "foo");
		assertThat(record).has(allOf(keyValue(23, "foo"), partition(42)));
		record = new ConsumerRecord<>("topic", 42, 0, 23, null);
		assertThat(record).has(keyValue(23, null));
		record = new ConsumerRecord<>("topic", 42, 0, null, "foo");
		assertThat(record).has(keyValue(null, "foo"));
		record = new ConsumerRecord<>("topic", 42, 0, null, null);
		assertThat(record).has(keyValue(null, null));
		assertThat(record).doesNotHave(keyValue(23, null));
		assertThat(record).doesNotHave(keyValue(null, "foo"));
		record = null;
		assertThat(record).doesNotHave(keyValue(null, null));
	}

}
