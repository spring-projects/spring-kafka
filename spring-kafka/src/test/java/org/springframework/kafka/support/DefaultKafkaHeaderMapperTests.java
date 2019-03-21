/*
 * Copyright 2017-2018 the original author or authors.
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

package org.springframework.kafka.support;

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.charset.Charset;
import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.common.header.internals.RecordHeaders;
import org.junit.Test;

import org.springframework.kafka.support.DefaultKafkaHeaderMapper.NonTrustedHeaderType;
import org.springframework.messaging.Message;
import org.springframework.messaging.MessageHeaders;
import org.springframework.messaging.support.ExecutorSubscribableChannel;
import org.springframework.messaging.support.MessageBuilder;
import org.springframework.util.MimeType;
import org.springframework.util.MimeTypeUtils;

/**
 * @author Gary Russell
 * @since 1.3
 *
 */
public class DefaultKafkaHeaderMapperTests {

	@Test
	public void test() {
		DefaultKafkaHeaderMapper mapper = new DefaultKafkaHeaderMapper();
		MimeType utf8Text = new MimeType(MimeTypeUtils.TEXT_PLAIN, Charset.forName("UTF-8"));
		Message<String> message = MessageBuilder.withPayload("foo")
				.setHeader("foo", "bar".getBytes())
				.setHeader("baz", "qux")
				.setHeader("fix", new Foo())
				.setHeader(MessageHeaders.REPLY_CHANNEL, new ExecutorSubscribableChannel())
				.setHeader(MessageHeaders.ERROR_CHANNEL, "errors")
				.setHeader(MessageHeaders.CONTENT_TYPE, utf8Text)
				.setHeader("simpleContentType", MimeTypeUtils.TEXT_PLAIN)
				.build();
		RecordHeaders recordHeaders = new RecordHeaders();
		mapper.fromHeaders(message.getHeaders(), recordHeaders);
		assertThat(recordHeaders.toArray().length).isEqualTo(7); // 6 + json_types
		Map<String, Object> headers = new HashMap<>();
		mapper.toHeaders(recordHeaders, headers);
		assertThat(headers.get("foo")).isInstanceOf(byte[].class);
		assertThat(new String((byte[]) headers.get("foo"))).isEqualTo("bar");
		assertThat(headers.get("baz")).isEqualTo("qux");
		assertThat(headers.get("fix")).isInstanceOf(NonTrustedHeaderType.class);
		assertThat(headers.get(MessageHeaders.CONTENT_TYPE)).isEqualTo(utf8Text);
		assertThat(headers.get("simpleContentType")).isEqualTo(MimeTypeUtils.TEXT_PLAIN);
		assertThat(headers.get(MessageHeaders.REPLY_CHANNEL)).isNull();
		assertThat(headers.get(MessageHeaders.ERROR_CHANNEL)).isEqualTo("errors");
		NonTrustedHeaderType ntht = (NonTrustedHeaderType) headers.get("fix");
		assertThat(ntht.getHeaderValue()).isNotNull();
		assertThat(ntht.getUntrustedType()).isEqualTo(Foo.class.getName());
		assertThat(headers).hasSize(6);
		mapper.addTrustedPackages(getClass().getPackage().getName());
		headers = new HashMap<>();
		mapper.toHeaders(recordHeaders, headers);
		assertThat(headers.get("foo")).isInstanceOf(byte[].class);
		assertThat(new String((byte[]) headers.get("foo"))).isEqualTo("bar");
		assertThat(headers.get("baz")).isEqualTo("qux");
		assertThat(headers.get("fix")).isEqualTo(new Foo());
		assertThat(headers).hasSize(6);
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

}
