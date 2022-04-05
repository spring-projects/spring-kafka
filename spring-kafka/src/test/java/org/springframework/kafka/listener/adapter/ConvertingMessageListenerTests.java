/*
 * Copyright 2016-2022 the original author or authors.
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

package org.springframework.kafka.listener.adapter;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.junit.jupiter.api.Test;

import org.springframework.kafka.listener.AcknowledgingConsumerAwareMessageListener;
import org.springframework.kafka.listener.MessageListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.kafka.support.SimpleKafkaHeaderMapper;
import org.springframework.messaging.Message;
import org.springframework.messaging.MessageHeaders;
import org.springframework.messaging.converter.MappingJackson2MessageConverter;
import org.springframework.messaging.converter.MessageConversionException;
import org.springframework.messaging.converter.MessageConverter;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * @author Adrian Chlebosz
 * @since 3.0.0
 *
 */
class ConvertingMessageListenerTests {

	private final ObjectMapper mapper = new ObjectMapper();

	@Test
	public void testMessageListenerIsInvokedWithConvertedSimpleRecord() {
		var consumerRecord = new ConsumerRecord<>("foo", 0, 0, "key", 0);

		var delegateListener = (MessageListener<String, Long>) (data) -> assertThat(data.value()).isNotNull();
		var convertingMessageListener = new ConvertingMessageListener<>(
				delegateListener,
				Long.class
		);

		convertingMessageListener.onMessage(consumerRecord, null, null);
	}

	@Test
	public void testMessageListenerIsInvokedWithRecordConvertedByCustomConverter() throws JsonProcessingException {
		var toBeConverted = new ToBeConverted("foo");
		var toBeConvertedJson = mapper.writeValueAsString(toBeConverted);
		var consumerRecord = new ConsumerRecord<>("foo", 0, 0, "key", toBeConvertedJson);

		var delegateListener = (MessageListener<String, ToBeConverted>) (data) -> {
			assertThat(data.value()).isNotNull();
			assertThat(data.value().getA()).isEqualTo("foo");
		};
		var convertingMessageListener = new ConvertingMessageListener<>(
				delegateListener,
				ToBeConverted.class
		);
		convertingMessageListener.setMessageConverter(new MappingJackson2MessageConverter());

		convertingMessageListener.onMessage(consumerRecord, null, null);
	}

	@SuppressWarnings({ "unchecked", "rawtypes" })
	@Test
	public void testMessageListenerIsInvokedOnlyOnce() {
		var consumerRecord = new ConsumerRecord<>("foo", 0, 0, "key", 0);

		var delegateListener = mock(AcknowledgingConsumerAwareMessageListener.class);
		var convertingMessageListener = new ConvertingMessageListener<>(
			delegateListener,
			Long.class
		);
		convertingMessageListener.setMessageConverter(new MappingJackson2MessageConverter());

		convertingMessageListener.onMessage(consumerRecord, null, null);

		verify(delegateListener, times(0)).onMessage(any());
		verify(delegateListener, times(0)).onMessage(any(), any(Acknowledgment.class));
		verify(delegateListener, times(0)).onMessage(any(), any(Consumer.class));
		verify(delegateListener, times(1)).onMessage(any(), any(), any());
	}

	@Test
	public void testConversionFailsWhileUsingDefaultConverterForComplexObject() throws JsonProcessingException {
		var toBeConverted = new ToBeConverted("foo");
		var toBeConvertedJson = mapper.writeValueAsString(toBeConverted);
		var consumerRecord = new ConsumerRecord<>("foo", 0, 0, "key", toBeConvertedJson);

		var delegateListener = (MessageListener<String, ToBeConverted>) (data) -> {
			assertThat(data.value()).isNotNull();
			assertThat(data.value().getA()).isEqualTo("foo");
		};
		var convertingMessageListener = new ConvertingMessageListener<>(
				delegateListener,
				ToBeConverted.class
		);

		assertThatThrownBy(
			() -> convertingMessageListener.onMessage(consumerRecord, null, null)
		).isInstanceOf(MessageConversionException.class);
	}

	@Test
	public void testHeadersAreAccessibleDuringConversionWhenHeaderMapperIsSpecified() {
		var consumerRecord = new ConsumerRecord<>("foo", 0, 0, "key", 0);
		var header = new RecordHeader("headerKey", "headerValue".getBytes());
		consumerRecord.headers().add(header);

		var delegateListener = (MessageListener<String, Long>) (data) -> { };
		var messageConverter = new MessageConverter() {
			@Override
			public Object fromMessage(Message<?> message, Class<?> targetClass) {
				var headers = message.getHeaders();
				assertThat(headers.containsKey("headerKey")).isTrue();
				assertThat(headers.get("headerKey", byte[].class)).isEqualTo(header.value());
				return 0L;
			}

			@Override
			public Message<?> toMessage(Object payload, MessageHeaders headers) {
				return null;
			}
		};
		var convertingMessageListener = new ConvertingMessageListener<>(
			delegateListener,
			Long.class
		);
		convertingMessageListener.setMessageConverter(messageConverter);
		convertingMessageListener.setKafkaHeaderMapper(new SimpleKafkaHeaderMapper());

		convertingMessageListener.onMessage(consumerRecord, null, null);
	}

	@Test
	public void testHeadersAreInaccessibleDuringConversionWhenHeaderMapperIsNotSpecified() {
		var consumerRecord = new ConsumerRecord<>("foo", 0, 0, "key", 0);
		var header = new RecordHeader("headerKey", "headerValue".getBytes());
		consumerRecord.headers().add(header);

		var delegateListener = (MessageListener<String, Long>) (data) -> { };
		var messageConverter = new MessageConverter() {
			@Override
			public Object fromMessage(Message<?> message, Class<?> targetClass) {
				var headers = message.getHeaders();
				assertThat(headers.containsKey("headerKey")).isFalse();
				return 0L;
			}

			@Override
			public Message<?> toMessage(Object payload, MessageHeaders headers) {
				return null;
			}
		};
		var convertingMessageListener = new ConvertingMessageListener<>(
			delegateListener,
			Long.class
		);
		convertingMessageListener.setMessageConverter(messageConverter);

		convertingMessageListener.onMessage(consumerRecord, null, null);
	}

	private static class ToBeConverted {
		private String a;

		ToBeConverted() {
		}

		ToBeConverted(String a) {
			this.a = a;
		}

		public String getA() {
			return a;
		}

		public void setA(String a) {
			this.a = a;
		}
	}

}
