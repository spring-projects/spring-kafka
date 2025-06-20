/*
 * Copyright 2019-present the original author or authors.
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

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.header.Header;
import org.junit.jupiter.api.Test;

import org.springframework.kafka.support.KafkaHeaders;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * @author Sanghyeok An
 * @since 3.3
 */

class DeliveryAttemptAwareRetryListenerTests {

	@Test
	void should_have_single_header_and_header_value_should_be_1() {
		// Given
		TopicPartition tpForTopicA = new TopicPartition("topicA", 1);
		TopicPartition tpForTopicB = new TopicPartition("topicB", 1);

		ConsumerRecord<String, String> record1 = new ConsumerRecord<>("topicA", 1, 1, "key", "value1");
		ConsumerRecord<String, String> record2 = new ConsumerRecord<>("topicA", 1, 2, "key", "value2");
		ConsumerRecord<String, String> record3 = new ConsumerRecord<>("topicA", 1, 3, "key", "value3");

		ConsumerRecord<String, String> record4 = new ConsumerRecord<>("topicB", 1, 1, "key", "value4");
		ConsumerRecord<String, String> record5 = new ConsumerRecord<>("topicB", 1, 2, "key", "value5");
		ConsumerRecord<String, String> record6 = new ConsumerRecord<>("topicB", 1, 3, "key", "value6");

		Map<TopicPartition, List<ConsumerRecord<String, String>>> map = new HashMap<>();

		List<ConsumerRecord<String, String>> topicARecords = List.of(record1, record2, record3);
		List<ConsumerRecord<String, String>> topicBRecords = List.of(record4, record5, record6);

		map.put(tpForTopicA, topicARecords);
		map.put(tpForTopicB, topicBRecords);

		ConsumerRecords<String, String> consumerRecords = new ConsumerRecords<>(map, Map.of());
		final DeliveryAttemptAwareRetryListener listener = new DeliveryAttemptAwareRetryListener();
		Exception ex = new RuntimeException("Dummy Exception");

		// Given : Expected Value
		int expectedDeliveryAttemptInHeader = 1;

		// When
		listener.failedDelivery(consumerRecords, ex, 1);

		// Then
		for (ConsumerRecord<String, String> consumerRecord : consumerRecords) {
			int deliveryAttemptHeaderCount = 0;
			Iterable<Header> headers = consumerRecord.headers().headers(KafkaHeaders.DELIVERY_ATTEMPT);

			for (Header header : headers) {
				int deliveryAttempt = ByteBuffer.wrap(header.value()).getInt();
				deliveryAttemptHeaderCount++;

				// Assertion
				assertThat(deliveryAttempt).isEqualTo(expectedDeliveryAttemptInHeader);
				assertThat(deliveryAttemptHeaderCount).isEqualTo(1);
			}
		}
	}

	@Test
	void should_have_single_header_and_header_value_should_be_4() {
		// Given
		TopicPartition tpForTopicA = new TopicPartition("topicA", 1);
		TopicPartition tpForTopicB = new TopicPartition("topicB", 1);

		ConsumerRecord<String, String> record1 = new ConsumerRecord<>("topicA", 1, 1, "key", "value1");
		ConsumerRecord<String, String> record2 = new ConsumerRecord<>("topicA", 1, 2, "key", "value2");
		ConsumerRecord<String, String> record3 = new ConsumerRecord<>("topicA", 1, 3, "key", "value3");

		ConsumerRecord<String, String> record4 = new ConsumerRecord<>("topicB", 1, 1, "key", "value4");
		ConsumerRecord<String, String> record5 = new ConsumerRecord<>("topicB", 1, 2, "key", "value5");
		ConsumerRecord<String, String> record6 = new ConsumerRecord<>("topicB", 1, 3, "key", "value6");

		Map<TopicPartition, List<ConsumerRecord<String, String>>> map = new HashMap<>();

		List<ConsumerRecord<String, String>> topicARecords = List.of(record1, record2, record3);
		List<ConsumerRecord<String, String>> topicBRecords = List.of(record4, record5, record6);

		map.put(tpForTopicA, topicARecords);
		map.put(tpForTopicB, topicBRecords);

		ConsumerRecords<String, String> consumerRecords = new ConsumerRecords<>(map, Map.of());
		final DeliveryAttemptAwareRetryListener listener = new DeliveryAttemptAwareRetryListener();
		Exception ex = new RuntimeException("Dummy Exception");

		// Given : Expected Value
		int expectedDeliveryAttemptInHeader = 4;

		// When
		for (int deliveryAttempt = 1; deliveryAttempt < 5; deliveryAttempt++) {
			listener.failedDelivery(consumerRecords, ex, deliveryAttempt);
		}

		// Then
		for (ConsumerRecord<String, String> consumerRecord : consumerRecords) {
			int deliveryAttemptHeaderCount = 0;
			Iterable<Header> headers = consumerRecord.headers().headers(KafkaHeaders.DELIVERY_ATTEMPT);
			for (Header header : headers) {
				int deliveryAttempt = ByteBuffer.wrap(header.value()).getInt();
				deliveryAttemptHeaderCount++;

				// Assertion
				assertThat(deliveryAttempt).isEqualTo(expectedDeliveryAttemptInHeader);
				assertThat(deliveryAttemptHeaderCount).isEqualTo(1);
			}
		}

	}

}
