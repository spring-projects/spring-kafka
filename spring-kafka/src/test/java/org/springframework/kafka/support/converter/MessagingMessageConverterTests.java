/*
 * Copyright 2019-2021 the original author or authors.
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

package org.springframework.kafka.support.converter;

import static org.assertj.core.api.Assertions.assertThat;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.Test;

import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.Message;

/**
 * @author Gary Russell
 * @author Artem Bilan
 *
 * @since 2.1.13
 *
 */
public class MessagingMessageConverterTests {

	@Test
	void missingHeaders() {
		MessagingMessageConverter converter = new MessagingMessageConverter();
		ConsumerRecord<String, String> record = new ConsumerRecord<>("foo", 1, 42, -1L, null, 0L, 0, 0, "bar", "baz");
		Message<?> message = converter.toMessage(record, null, null, null);
		assertThat(message.getPayload()).isEqualTo("baz");
		assertThat(message.getHeaders().get(KafkaHeaders.RECEIVED_TOPIC)).isEqualTo("foo");
		assertThat(message.getHeaders().get(KafkaHeaders.RECEIVED_MESSAGE_KEY)).isEqualTo("bar");
		assertThat(message.getHeaders().get(KafkaHeaders.RAW_DATA)).isNull();
	}

	@Test
	void dontMapNullKey() {
		MessagingMessageConverter converter = new MessagingMessageConverter();
		ConsumerRecord<String, String> record = new ConsumerRecord<>("foo", 1, 42, -1L, null, 0L, 0, 0, null, "baz");
		Message<?> message = converter.toMessage(record, null, null, null);
		assertThat(message.getPayload()).isEqualTo("baz");
		assertThat(message.getHeaders().get(KafkaHeaders.RECEIVED_TOPIC)).isEqualTo("foo");
		assertThat(message.getHeaders().containsKey(KafkaHeaders.RECEIVED_MESSAGE_KEY)).isFalse();
	}

	@Test
	void raw() {
		MessagingMessageConverter converter = new MessagingMessageConverter();
		converter.setRawRecordHeader(true);
		ConsumerRecord<String, String> record = new ConsumerRecord<>("foo", 1, 42, -1L, null, 0L, 0, 0, "bar", "baz");
		Message<?> message = converter.toMessage(record, null, null, null);
		assertThat(message.getPayload()).isEqualTo("baz");
		assertThat(message.getHeaders().get(KafkaHeaders.RECEIVED_TOPIC)).isEqualTo("foo");
		assertThat(message.getHeaders().get(KafkaHeaders.RECEIVED_MESSAGE_KEY)).isEqualTo("bar");
		assertThat(message.getHeaders().get(KafkaHeaders.RAW_DATA)).isSameAs(record);
	}

}
