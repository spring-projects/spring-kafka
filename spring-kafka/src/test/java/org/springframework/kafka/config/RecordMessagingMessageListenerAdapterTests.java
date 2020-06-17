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

package org.springframework.kafka.config;

import static org.assertj.core.api.Assertions.assertThat;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.record.TimestampType;
import org.junit.jupiter.api.Test;

import org.springframework.format.support.DefaultFormattingConversionService;
import org.springframework.kafka.listener.adapter.ConsumerRecordMetadata;
import org.springframework.kafka.listener.adapter.RecordMessagingMessageListenerAdapter;
import org.springframework.messaging.converter.GenericMessageConverter;
import org.springframework.messaging.handler.annotation.support.DefaultMessageHandlerMethodFactory;
import org.springframework.messaging.handler.annotation.support.MessageHandlerMethodFactory;

/**
 * @author Gary Russell
 * @since 2.5
 *
 */
public class RecordMessagingMessageListenerAdapterTests {

	@Test
	void testMetaString() throws Exception {
		Method method = TestClass.class.getDeclaredMethod("listen", String.class, ConsumerRecordMetadata.class);
		TestClass bean = new TestClass();
		MethodKafkaListenerEndpoint<Object, Object> endpoint = new MethodKafkaListenerEndpoint<>();
		endpoint.setBean(bean);
		endpoint.setMethod(method);
		endpoint.setMessageHandlerMethodFactory(messageHandlerFactory());
		RecordMessagingMessageListenerAdapter<Object, Object> adapter =
				(RecordMessagingMessageListenerAdapter<Object, Object>) endpoint.createMessageListener(null, null);
		ConsumerRecord<Object, Object> record = new ConsumerRecord<>("topic", 0, 42L, 43L, TimestampType.CREATE_TIME,
				0L, 44, 45, null, "foo");
		adapter.onMessage(record, null, null);
		assertThat(bean.string).isEqualTo("foo");
		assertThat(bean.metadata.topic()).isEqualTo(record.topic());
		assertThat(bean.metadata.partition()).isEqualTo(record.partition());
		assertThat(bean.metadata.offset()).isEqualTo(record.offset());
		assertThat(bean.metadata.serializedKeySize()).isEqualTo(record.serializedKeySize());
		assertThat(bean.metadata.serializedValueSize()).isEqualTo(record.serializedValueSize());
		assertThat(bean.metadata.timestamp()).isEqualTo(record.timestamp());
		assertThat(bean.metadata.timestampType()).isEqualTo(record.timestampType());
	}

	@Test
	void testMultiMetaString() {
		Method[] declaredMethods = TestMultiClass.class.getDeclaredMethods();
		Method defMethod = null;
		List<Method> methods = new ArrayList<>();
		for (Method method : declaredMethods) {
			if (method.getName().equals("defListen")) {
				defMethod = method;
				methods.add(method);
			}
			else if (method.getName().equals("listen")) {
				methods.add(method);
			}
		}
		TestMultiClass bean = new TestMultiClass();
		MultiMethodKafkaListenerEndpoint<Object, Object> endpoint = new MultiMethodKafkaListenerEndpoint<>(methods,
				defMethod, bean);
		endpoint.setMessageHandlerMethodFactory(messageHandlerFactory());
		RecordMessagingMessageListenerAdapter<Object, Object> adapter =
				(RecordMessagingMessageListenerAdapter<Object, Object>) endpoint.createMessageListener(null, null);
		adapter.onMessage(new ConsumerRecord<>("topic", 0, 0L, null, 42), null, null);
		assertThat(bean.value).isEqualTo(42);
		ConsumerRecord<Object, Object> record = new ConsumerRecord<>("topic", 0, 42L, 43L, TimestampType.CREATE_TIME,
				0L, 44, 45, null, "foo");
		adapter.onMessage(record, null, null);
		assertThat(bean.string).isEqualTo("foo");
		assertThat(bean.metadata.topic()).isEqualTo(record.topic());
		assertThat(bean.metadata.partition()).isEqualTo(record.partition());
		assertThat(bean.metadata.offset()).isEqualTo(record.offset());
		assertThat(bean.metadata.serializedKeySize()).isEqualTo(record.serializedKeySize());
		assertThat(bean.metadata.serializedValueSize()).isEqualTo(record.serializedValueSize());
		assertThat(bean.metadata.timestamp()).isEqualTo(record.timestamp());
		assertThat(bean.metadata.timestampType()).isEqualTo(record.timestampType());
	}

	private MessageHandlerMethodFactory messageHandlerFactory() {
		DefaultMessageHandlerMethodFactory defaultFactory = new DefaultMessageHandlerMethodFactory();
		DefaultFormattingConversionService cs = new DefaultFormattingConversionService();
		defaultFactory.setConversionService(cs);
		GenericMessageConverter messageConverter = new GenericMessageConverter(cs);
		defaultFactory.setMessageConverter(messageConverter);
		defaultFactory.afterPropertiesSet();
		return defaultFactory;
	}

	public static class TestClass {

		String string;

		ConsumerRecordMetadata metadata;

		public void listen(String str, ConsumerRecordMetadata meta) {
			this.string = str;
			this.metadata = meta;
		}

	}

	public static class TestMultiClass {

		int value;

		String string;

		ConsumerRecordMetadata metadata;

		public void listen(Integer value) {
			this.value = value;
		}

		public void defListen(String str, ConsumerRecordMetadata meta) {
			this.string = str;
			this.metadata = meta;
		}

	}

}
