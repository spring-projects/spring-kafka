/*
 * Copyright 2016-present the original author or authors.
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

import java.lang.reflect.Type;
import java.util.Arrays;
import java.util.List;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.Test;

import org.springframework.kafka.support.converter.BatchMessagingMessageConverter;
import org.springframework.kafka.support.converter.MessagingMessageConverter;
import org.springframework.messaging.Message;
import org.springframework.messaging.MessageHeaders;
import org.springframework.messaging.converter.SmartMessageConverter;
import org.springframework.messaging.support.MessageBuilder;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests for SmartMessageConverter support in batch listeners.
 * Reproduces the issue described in GH-4097.
 *
 * @author Jujuwryy
 * @since 3.3.11
 */
class BatchSmartMessageConverterTests {

	@Test
	void testSmartMessageConverterWorksInBatchConversion() {
		// Given: A BatchMessagingMessageConverter with a record converter and SmartMessageConverter
		MessagingMessageConverter recordConverter = new MessagingMessageConverter();
		BatchMessagingMessageConverter batchConverter = new BatchMessagingMessageConverter(recordConverter);

		// Set up SmartMessageConverter that converts byte[] to String
		TestStringMessageConverter smartConverter = new TestStringMessageConverter();
		batchConverter.setMessagingConverter(smartConverter);

		// Create test records with byte[] values that need conversion to String
		List<ConsumerRecord<?, ?>> records = Arrays.asList(
				new ConsumerRecord<>("topic", 0, 0, "key", "hello".getBytes()),
				new ConsumerRecord<>("topic", 0, 1, "key", "world".getBytes())
		);

		// When: Convert batch with List<String> target type
		Type targetType = new TestParameterizedType(List.class, new Type[]{String.class});
		Message<?> result = batchConverter.toMessage(records, null, null, targetType);

		// Then: Verify the SmartMessageConverter was applied and byte[] was converted to String
		assertThat(result).isNotNull();
		assertThat(result.getPayload()).isInstanceOf(List.class);

		List<?> payloads = (List<?>) result.getPayload();
		assertThat(payloads).hasSize(2);
		assertThat(payloads.get(0)).isEqualTo("hello");
		assertThat(payloads.get(1)).isEqualTo("world");
	}

	@Test
	void testBatchConversionWithoutSmartMessageConverter() {
		// Given: A BatchMessagingMessageConverter without SmartMessageConverter
		MessagingMessageConverter recordConverter = new MessagingMessageConverter();
		BatchMessagingMessageConverter batchConverter = new BatchMessagingMessageConverter(recordConverter);

		// Create test records with byte[] values
		List<ConsumerRecord<?, ?>> records = Arrays.asList(
				new ConsumerRecord<>("topic", 0, 0, "key", "test".getBytes())
		);

		// When: Convert batch
		Type targetType = new TestParameterizedType(List.class, new Type[]{String.class});
		Message<?> result = batchConverter.toMessage(records, null, null, targetType);

		// Then: Should work but payloads remain as byte[]
		assertThat(result).isNotNull();
		List<?> payloads = (List<?>) result.getPayload();
		assertThat(payloads.get(0)).isInstanceOf(byte[].class);
	}

	/**
	 * Test SmartMessageConverter that converts byte[] to String.
	 */
	static class TestStringMessageConverter implements SmartMessageConverter {

		@Override
		public Object fromMessage(Message<?> message, Class<?> targetClass) {
			return convertPayload(message.getPayload());
		}

		@Override
		public Object fromMessage(Message<?> message, Class<?> targetClass, Object conversionHint) {
			return convertPayload(message.getPayload());
		}

		@Override
		public Message<?> toMessage(Object payload, MessageHeaders headers) {
			return MessageBuilder.withPayload(payload).copyHeaders(headers).build();
		}

		@Override
		public Message<?> toMessage(Object payload, MessageHeaders headers, Object conversionHint) {
			return toMessage(payload, headers);
		}

		private Object convertPayload(Object payload) {
			// Convert byte[] to String - this is the core functionality being tested
			if (payload instanceof byte[] bytes) {
				return new String(bytes);
			}
			return payload;
		}
	}

	/**
	 * Helper class for creating parameterized types for testing.
	 */
	static class TestParameterizedType implements java.lang.reflect.ParameterizedType {

		private final Type rawType;

		private final Type[] typeArguments;

		TestParameterizedType(Type rawType, Type[] typeArguments) {
			this.rawType = rawType;
			this.typeArguments = typeArguments;
		}

		public Type[] getActualTypeArguments() {
			return typeArguments;
		}

		public Type getRawType() {
			return rawType;
		}

		public Type getOwnerType() {
			return null;
		}
	}
}
