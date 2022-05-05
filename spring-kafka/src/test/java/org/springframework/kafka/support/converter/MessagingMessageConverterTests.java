/*
 * Copyright 2019-2022 the original author or authors.
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

import java.util.Arrays;
import java.util.Collection;
import java.util.Optional;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.junit.jupiter.api.Test;

import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.lang.Nullable;
import org.springframework.messaging.Message;
import org.springframework.messaging.MessageHeaders;
import org.springframework.messaging.converter.AbstractMessageConverter;
import org.springframework.messaging.converter.CompositeMessageConverter;
import org.springframework.messaging.converter.MessageConverter;
import org.springframework.util.MimeType;

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
		ConsumerRecord<String, String> record = new ConsumerRecord<>("foo", 1, 42, -1L, null, 0, 0, "bar", "baz",
				new RecordHeaders(), Optional.empty());
		Message<?> message = converter.toMessage(record, null, null, null);
		assertThat(message.getPayload()).isEqualTo("baz");
		assertThat(message.getHeaders().get(KafkaHeaders.RECEIVED_TOPIC)).isEqualTo("foo");
		assertThat(message.getHeaders().get(KafkaHeaders.RECEIVED_KEY)).isEqualTo("bar");
		assertThat(message.getHeaders().get(KafkaHeaders.RAW_DATA)).isNull();
	}

	@Test
	void dontMapNullKey() {
		MessagingMessageConverter converter = new MessagingMessageConverter();
		ConsumerRecord<String, String> record = new ConsumerRecord<>("foo", 1, 42, -1L, null, 0, 0, null, "baz",
				new RecordHeaders(), Optional.empty());
		Message<?> message = converter.toMessage(record, null, null, null);
		assertThat(message.getPayload()).isEqualTo("baz");
		assertThat(message.getHeaders().get(KafkaHeaders.RECEIVED_TOPIC)).isEqualTo("foo");
		assertThat(message.getHeaders().containsKey(KafkaHeaders.RECEIVED_KEY)).isFalse();
	}

	@Test
	void raw() {
		MessagingMessageConverter converter = new MessagingMessageConverter();
		converter.setRawRecordHeader(true);
		ConsumerRecord<String, String> record = new ConsumerRecord<>("foo", 1, 42, -1L, null, 0, 0, "bar", "baz",
				new RecordHeaders(), Optional.empty());
		Message<?> message = converter.toMessage(record, null, null, null);
		assertThat(message.getPayload()).isEqualTo("baz");
		assertThat(message.getHeaders().get(KafkaHeaders.RECEIVED_TOPIC)).isEqualTo("foo");
		assertThat(message.getHeaders().get(KafkaHeaders.RECEIVED_KEY)).isEqualTo("bar");
		assertThat(message.getHeaders().get(KafkaHeaders.RAW_DATA)).isSameAs(record);
	}

	@Test
	void delegate() {
		MessagingMessageConverter converter = new MessagingMessageConverter();
		converter.setMessagingConverter(new MappingJacksonParameterizedConverter());
		Headers headers = new RecordHeaders();
		headers.add(new RecordHeader(MessageHeaders.CONTENT_TYPE, "application/json".getBytes()));
		ConsumerRecord<String, String> record =
				new ConsumerRecord<>("foo", 1, 42, -1L, null, 0, 0, "bar", "{ \"foo\":\"bar\"}", headers,
						Optional.empty());
		Message<?> message = converter.toMessage(record, null, null, Foo.class);
		assertThat(message.getPayload()).isEqualTo(new Foo("bar"));
	}

	@Test
	void delegateNoContentType() {
		// this works because of the type hint
		MessagingMessageConverter converter = new MessagingMessageConverter();
		converter.setMessagingConverter(new MappingJacksonParameterizedConverter());
		ConsumerRecord<String, String> record =
				new ConsumerRecord<>("foo", 1, 42, -1L, null, 0, 0, "bar", "{ \"foo\":\"bar\"}",
						new RecordHeaders(), Optional.empty());
		Message<?> message = converter.toMessage(record, null, null, Foo.class);
		assertThat(message.getPayload()).isEqualTo(new Foo("bar"));
	}

	@Test
	void contentNegotiation() {
		MessagingMessageConverter converter = new MessagingMessageConverter();
		Collection<MessageConverter> converters = Arrays.asList(new FooConverter(MimeType.valueOf("application/foo")),
				new BarConverter(MimeType.valueOf("application/bar")));
		converter.setMessagingConverter(new CompositeMessageConverter(converters));
		Headers headers = new RecordHeaders();
		headers.add(new RecordHeader(MessageHeaders.CONTENT_TYPE, "application/foo".getBytes()));
		ConsumerRecord<String, String> record =
				new ConsumerRecord<>("foo", 1, 42, -1L, null, 0, 0, "bar", "qux", headers, Optional.empty());
		Message<?> message = converter.toMessage(record, null, null, Foo.class);
		assertThat(message.getPayload()).isEqualTo(new Foo("bar"));
		ProducerRecord<?, ?> pr = converter.fromMessage(message, "test");
		assertThat(pr.topic()).isEqualTo("test");
		assertThat(pr.value()).isEqualTo("foo".getBytes());
		headers.remove(MessageHeaders.CONTENT_TYPE);
		headers.add(new RecordHeader(MessageHeaders.CONTENT_TYPE, "application/bar".getBytes()));
		message = converter.toMessage(record, null, null, Bar.class);
		assertThat(message.getPayload()).isEqualTo(new Bar("bar"));
		pr = converter.fromMessage(message, "test");
		assertThat(pr.topic()).isEqualTo("test");
		assertThat(pr.value()).isEqualTo("bar".getBytes());
		headers.remove(MessageHeaders.CONTENT_TYPE);
		headers.add(new RecordHeader(MessageHeaders.CONTENT_TYPE, "application/baz".getBytes()));
		message = converter.toMessage(record, null, null, Bar.class);
		assertThat(message.getPayload()).isEqualTo("qux"); // no contentType match
		pr = converter.fromMessage(message, "test");
		assertThat(pr.topic()).isEqualTo("test");
		assertThat(pr.value()).isEqualTo("qux");
	}

	@Test
	void contentNegotiationNoHeaderMapper() {
		MessagingMessageConverter converter = new MessagingMessageConverter();
		converter.setHeaderMapper(null);
		Collection<MessageConverter> converters = Arrays.asList(new FooConverter(MimeType.valueOf("application/foo")),
				new BarConverter(MimeType.valueOf("application/bar")));
		converter.setMessagingConverter(new CompositeMessageConverter(converters));
		Headers headers = new RecordHeaders();
		headers.add(new RecordHeader(MessageHeaders.CONTENT_TYPE, "application/foo".getBytes()));
		ConsumerRecord<String, String> record =
				new ConsumerRecord<>("foo", 1, 42, -1L, null, 0, 0, "bar", "qux", headers, Optional.empty());
		Message<?> message = converter.toMessage(record, null, null, Foo.class);
		assertThat(message.getPayload()).isEqualTo(new Foo("bar"));
		headers.remove(MessageHeaders.CONTENT_TYPE);
		message = converter.toMessage(record, null, null, Foo.class);
		assertThat(message.getPayload()).isEqualTo(new Foo("bar")); // no contentType header
	}

	static class FooConverter extends AbstractMessageConverter {

		FooConverter(MimeType supportedMimeType) {
			super(supportedMimeType);
		}

		@Override
		protected boolean supports(Class<?> clazz) {
			return Foo.class.isAssignableFrom(clazz);
		}

		@Override
		@Nullable
		protected Object convertFromInternal(Message<?> message, Class<?> targetClass,
				@Nullable Object conversionHint) {

			return new Foo("bar");
		}

		@Override
		@Nullable
		protected Object convertToInternal(Object payload, @Nullable MessageHeaders headers,
				@Nullable Object conversionHint) {

			return "foo".getBytes();
		}

	}

	static class BarConverter extends FooConverter {

		BarConverter(MimeType supportedMimeType) {
			super(supportedMimeType);
		}

		@Override
		@Nullable
		protected Object convertFromInternal(Message<?> message, Class<?> targetClass,
				@Nullable Object conversionHint) {

			return new Bar("bar");
		}

		@Override
		@Nullable
		protected Object convertToInternal(Object payload, @Nullable MessageHeaders headers,
				@Nullable Object conversionHint) {

			return "bar".getBytes();
		}

	}

	public static class Foo {

		private String foo;

		public Foo() {
		}

		public Foo(String foo) {
			this.foo = foo;
		}

		public String getFoo() {
			return this.foo;
		}

		public void setFoo(String foo) {
			this.foo = foo;
		}

		@Override
		public int hashCode() {
			final int prime = 31;
			int result = 1;
			result = prime * result + ((this.foo == null) ? 0 : this.foo.hashCode());
			return result;
		}

		@Override
		public boolean equals(Object obj) {
			if (this == obj) {
				return true;
			}
			if (obj == null) {
				return false;
			}
			if (getClass() != obj.getClass()) {
				return false;
			}
			Foo other = (Foo) obj;
			if (this.foo == null) {
				if (other.foo != null) {
					return false;
				}
			}
			else if (!this.foo.equals(other.foo)) {
				return false;
			}
			return true;
		}

	}

	public static class Bar extends Foo {

		public Bar() {
		}

		public Bar(String foo) {
			super(foo);
		}

	}

}
