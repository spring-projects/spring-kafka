/*
 * Copyright 2017-2018 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.springframework.kafka.support;

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.charset.Charset;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import org.apache.kafka.common.header.internals.RecordHeaders;
import org.junit.Test;

import org.springframework.kafka.support.DefaultKafkaHeaderMapper.NonTrustedHeaderType;
import org.springframework.kafka.test.utils.KafkaTestUtils;
import org.springframework.messaging.Message;
import org.springframework.messaging.MessageHeaders;
import org.springframework.messaging.support.ExecutorSubscribableChannel;
import org.springframework.messaging.support.MessageBuilder;
import org.springframework.util.LinkedMultiValueMap;
import org.springframework.util.MimeType;
import org.springframework.util.MimeTypeUtils;

/**
 * @author Gary Russell
 * @since 1.3
 *
 */
public class DefaultKafkaHeaderMapperTests {

	@Test
	public void testTrustedAndNot() {
		DefaultKafkaHeaderMapper mapper = new DefaultKafkaHeaderMapper();
		mapper.addToStringClasses(Bar.class.getName());
		MimeType utf8Text = new MimeType(MimeTypeUtils.TEXT_PLAIN, Charset.forName("UTF-8"));
		Message<String> message = MessageBuilder.withPayload("foo")
				.setHeader("foo", "bar".getBytes())
				.setHeader("baz", "qux")
				.setHeader("fix", new Foo())
				.setHeader("linkedMVMap", new LinkedMultiValueMap<>())
				.setHeader(MessageHeaders.REPLY_CHANNEL, new ExecutorSubscribableChannel())
				.setHeader(MessageHeaders.ERROR_CHANNEL, "errors")
				.setHeader(MessageHeaders.CONTENT_TYPE, utf8Text)
				.setHeader("simpleContentType", MimeTypeUtils.TEXT_PLAIN)
				.setHeader("customToString", new Bar("fiz"))
				.build();
		RecordHeaders recordHeaders = new RecordHeaders();
		mapper.fromHeaders(message.getHeaders(), recordHeaders);
		assertThat(recordHeaders.toArray().length).isEqualTo(9); // 8 + json_types
		Map<String, Object> headers = new HashMap<>();
		mapper.toHeaders(recordHeaders, headers);
		assertThat(headers.get("foo")).isInstanceOf(byte[].class);
		assertThat(new String((byte[]) headers.get("foo"))).isEqualTo("bar");
		assertThat(headers.get("baz")).isEqualTo("qux");
		assertThat(headers.get("fix")).isInstanceOf(NonTrustedHeaderType.class);
		assertThat(headers.get("linkedMVMap")).isInstanceOf(LinkedMultiValueMap.class);
		assertThat(headers.get(MessageHeaders.CONTENT_TYPE)).isEqualTo(utf8Text.toString());
		assertThat(headers.get("simpleContentType")).isEqualTo(MimeTypeUtils.TEXT_PLAIN.toString());
		assertThat(headers.get(MessageHeaders.REPLY_CHANNEL)).isNull();
		assertThat(headers.get(MessageHeaders.ERROR_CHANNEL)).isEqualTo("errors");
		assertThat(headers.get("customToString")).isEqualTo("Bar [field=fiz]");
		NonTrustedHeaderType ntht = (NonTrustedHeaderType) headers.get("fix");
		assertThat(ntht.getHeaderValue()).isNotNull();
		assertThat(ntht.getUntrustedType()).isEqualTo(Foo.class.getName());
		assertThat(headers).hasSize(8);

		mapper.addTrustedPackages(getClass().getPackage().getName());
		headers = new HashMap<>();
		mapper.toHeaders(recordHeaders, headers);
		assertThat(headers.get("foo")).isInstanceOf(byte[].class);
		assertThat(new String((byte[]) headers.get("foo"))).isEqualTo("bar");
		assertThat(headers.get("baz")).isEqualTo("qux");
		assertThat(headers.get("fix")).isEqualTo(new Foo());
		assertThat(headers).hasSize(8);
	}

	@Test
	public void testReserializedNonTrusted() {
		DefaultKafkaHeaderMapper mapper = new DefaultKafkaHeaderMapper();
		Message<String> message = MessageBuilder.withPayload("foo")
				.setHeader("fix", new Foo())
				.build();
		RecordHeaders recordHeaders = new RecordHeaders();
		mapper.fromHeaders(message.getHeaders(), recordHeaders);
		assertThat(recordHeaders.toArray().length).isEqualTo(2); // 1 + json_types
		Map<String, Object> headers = new HashMap<>();
		mapper.toHeaders(recordHeaders, headers);
		assertThat(headers.get("fix")).isInstanceOf(NonTrustedHeaderType.class);
		NonTrustedHeaderType ntht = (NonTrustedHeaderType) headers.get("fix");
		assertThat(ntht.getHeaderValue()).isNotNull();
		assertThat(ntht.getUntrustedType()).isEqualTo(Foo.class.getName());
		assertThat(headers).hasSize(1);

		recordHeaders = new RecordHeaders();
		mapper.fromHeaders(new MessageHeaders(headers), recordHeaders);
		headers = new HashMap<>();
		mapper.toHeaders(recordHeaders, headers);
		assertThat(headers.get("fix")).isInstanceOf(NonTrustedHeaderType.class);
		ntht = (NonTrustedHeaderType) headers.get("fix");
		assertThat(ntht.getHeaderValue()).isNotNull();
		assertThat(ntht.getUntrustedType()).isEqualTo(Foo.class.getName());

		mapper.addTrustedPackages(getClass().getPackage().getName());
		headers = new HashMap<>();
		mapper.toHeaders(recordHeaders, headers);
		assertThat(headers.get("fix")).isInstanceOf(Foo.class);
	}

	@Test
	public void testMimeBackwardsCompat() {
		DefaultKafkaHeaderMapper mapper = new DefaultKafkaHeaderMapper();
		MessageHeaders headers = new MessageHeaders(
				Collections.singletonMap("foo", MimeType.valueOf("application/json")));

		RecordHeaders recordHeaders = new RecordHeaders();
		mapper.fromHeaders(headers, recordHeaders);
		Map<String, Object> receivedHeaders = new HashMap<>();
		mapper.toHeaders(recordHeaders, receivedHeaders);
		Object fooHeader = receivedHeaders.get("foo");
		assertThat(fooHeader).isInstanceOf(String.class);
		assertThat(fooHeader).isEqualTo("application/json");

		KafkaTestUtils.getPropertyValue(mapper, "toStringClasses", Set.class).clear();
		recordHeaders = new RecordHeaders();
		mapper.fromHeaders(headers, recordHeaders);
		receivedHeaders = new HashMap<>();
		mapper.toHeaders(recordHeaders, receivedHeaders);
		fooHeader = receivedHeaders.get("foo");
		assertThat(fooHeader).isInstanceOf(MimeType.class);
		assertThat(fooHeader).isEqualTo(MimeType.valueOf("application/json"));
	}

	public static final class Foo {

		private String bar = "bar";

		public String getBar() {
			return this.bar;
		}

		public void setBar(String bar) {
			this.bar = bar;
		}

		@Override
		public int hashCode() {
			final int prime = 31;
			int result = 1;
			result = prime * result + ((this.bar == null) ? 0 : this.bar.hashCode());
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
			if (this.bar == null) {
				if (other.bar != null) {
					return false;
				}
			}
			else if (!this.bar.equals(other.bar)) {
				return false;
			}
			return true;
		}

	}

	public static class Bar {

		private String field;

		public Bar() {
			super();
		}

		public Bar(String field) {
			this.field = field;
		}

		public String getField() {
			return this.field;
		}

		public void setField(String field) {
			this.field = field;
		}

		@Override
		public String toString() {
			return "Bar [field=" + this.field + "]";
		}

	}

}
