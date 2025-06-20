/*
 * Copyright 2020-present the original author or authors.
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

package org.springframework.kafka.support.micrometer;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.Test;

import org.springframework.kafka.support.micrometer.KafkaListenerObservation.DefaultKafkaListenerObservationConvention;

/**
 * @author Christian Fredriksson
 */
public class KafkaListenerObservationTests {

	@Test
	void lowCardinalityKeyValues() {
		ConsumerRecord<String, String> record = new ConsumerRecord<>("topic", 1, 2, "key", "value");
		KafkaRecordReceiverContext context = new KafkaRecordReceiverContext(record, "listener", () -> null);
		DefaultKafkaListenerObservationConvention.INSTANCE.getLowCardinalityKeyValues(context);
	}

	@Test
	void highCardinalityKeyValues() {
		ConsumerRecord<String, String> record = new ConsumerRecord<>("topic", 1, 2, "key", "value");
		KafkaRecordReceiverContext context = new KafkaRecordReceiverContext(record, "listener", () -> null);
		DefaultKafkaListenerObservationConvention.INSTANCE.getHighCardinalityKeyValues(context);
	}
}
