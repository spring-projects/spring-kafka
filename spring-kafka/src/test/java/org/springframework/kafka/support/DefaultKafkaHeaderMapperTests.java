/*
 * Copyright 2017-2022 the original author or authors.
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
import static org.assertj.core.api.Assertions.entry;

import java.net.URI;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.junit.jupiter.api.Test;

import org.springframework.kafka.support.DefaultKafkaHeaderMapper.NonTrustedHeaderType;
import org.springframework.messaging.Message;
import org.springframework.messaging.MessageHeaders;
import org.springframework.messaging.support.ExecutorSubscribableChannel;
import org.springframework.messaging.support.MessageBuilder;
import org.springframework.util.LinkedMultiValueMap;
import org.springframework.util.MimeType;
import org.springframework.util.MimeTypeUtils;

/**
 * @author Gary Russell
 * @author Artem Bilan
 *
 * @since 1.3
 *
 */
public class DefaultKafkaHeaderMapperTests {

	@Test
	void testTrustedAndNot() {
		DefaultKafkaHeaderMapper mapper = new DefaultKafkaHeaderMapper();
		mapper.addToStringClasses(Bar.class.getName());
		MimeType utf8Text = new MimeType(MimeTypeUtils.TEXT_PLAIN, StandardCharsets.UTF_8);
		Message<String> message = MessageBuilder.withPayload("foo")
				.setHeader("foo", "bar".getBytes())
				.setHeader("baz", "qux")
				.setHeader("fix", new Foo())
				.setHeader("linkedMVMap", new LinkedMultiValueMap<>())
				.setHeader(MessageHeaders.REPLY_CHANNEL, new ExecutorSubscribableChannel())
				.setHeader(MessageHeaders.ERROR_CHANNEL, "errors")
				.setHeader(MessageHeaders.CONTENT_TYPE, utf8Text)
				.setHeader("simpleContentType", MimeTypeUtils.TEXT_PLAIN_VALUE)
				.setHeader("customToString", new Bar("fiz"))
				.setHeader("uri", URI.create("https://foo.bar"))
				.setHeader("intA", new int[] { 42 })
				.setHeader("longA", new long[] { 42L })
				.setHeader("floatA", new float[] { 1.0f })
				.setHeader("doubleA", new double[] { 1.0 })
				.setHeader("charA", new char[] { 'c' })
				.setHeader("boolA", new boolean[] { true })
				.setHeader("IntA", new Integer[] { 42 })
				.setHeader("LongA", new Long[] { 42L })
				.setHeader("FloatA", new Float[] { 1.0f })
				.setHeader("DoubleA", new Double[] { 1.0 })
				.setHeader("CharA", new Character[] { 'c' })
				.setHeader("BoolA", new Boolean[] { true })
				.setHeader("stringA", new String[] { "array" })
				.build();
		RecordHeaders recordHeaders = new RecordHeaders();
		mapper.fromHeaders(message.getHeaders(), recordHeaders);
		int expectedSize = message.getHeaders().size() - 3; // ID, Timestamp, reply channel
		assertThat(recordHeaders.toArray().length).isEqualTo(expectedSize + 1); // json_types header
		Map<String, Object> headers = new HashMap<>();
		mapper.toHeaders(recordHeaders, headers);
		assertThat(headers.get("foo")).isInstanceOf(byte[].class);
		assertThat(new String((byte[]) headers.get("foo"))).isEqualTo("bar");
		assertThat(headers.get("baz")).isEqualTo("qux");
		assertThat(headers.get("fix")).isInstanceOf(NonTrustedHeaderType.class);
		assertThat(headers.get("linkedMVMap")).isInstanceOf(LinkedMultiValueMap.class);
		assertThat(MimeType.valueOf(headers.get(MessageHeaders.CONTENT_TYPE).toString())).isEqualTo(utf8Text);
		assertThat(headers.get("simpleContentType")).isEqualTo(MimeTypeUtils.TEXT_PLAIN_VALUE);
		assertThat(headers.get(MessageHeaders.REPLY_CHANNEL)).isNull();
		assertThat(headers.get(MessageHeaders.ERROR_CHANNEL)).isEqualTo("errors");
		assertThat(headers.get("customToString")).isEqualTo("Bar [field=fiz]");
		assertThat(headers.get("uri")).isEqualTo(URI.create("https://foo.bar"));
		assertThat(headers.get("intA")).isEqualTo(new int[] { 42 });
		assertThat(headers.get("longA")).isEqualTo(new long[] { 42L });
		assertThat(headers.get("floatA")).isEqualTo(new float[] { 1.0f });
		assertThat(headers.get("doubleA")).isEqualTo(new double[] { 1.0 });
		assertThat(headers.get("charA")).isEqualTo(new char[] { 'c' });
		assertThat(headers.get("IntA")).isEqualTo(new Integer[] { 42 });
		assertThat(headers.get("LongA")).isEqualTo(new Long[] { 42L });
		assertThat(headers.get("FloatA")).isEqualTo(new Float[] { 1.0f });
		assertThat(headers.get("DoubleA")).isEqualTo(new Double[] { 1.0 });
		assertThat(headers.get("CharA")).isEqualTo(new Character[] { 'c' });
		assertThat(headers.get("stringA")).isEqualTo(new String[] { "array" });
		NonTrustedHeaderType ntht = (NonTrustedHeaderType) headers.get("fix");
		assertThat(ntht.getHeaderValue()).isNotNull();
		assertThat(ntht.getUntrustedType()).isEqualTo(Foo.class.getName());
		assertThat(headers).hasSize(expectedSize);

		mapper.addTrustedPackages(getClass().getPackage().getName());
		headers = new HashMap<>();
		mapper.toHeaders(recordHeaders, headers);
		assertThat(headers.get("foo")).isInstanceOf(byte[].class);
		assertThat(new String((byte[]) headers.get("foo"))).isEqualTo("bar");
		assertThat(headers.get("baz")).isEqualTo("qux");
		assertThat(headers.get("fix")).isEqualTo(new Foo());
		assertThat(headers).hasSize(expectedSize);
	}

	@Test
	void testDeserializedNonTrusted() {
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
	void testMimeTypeInHeaders() {
		DefaultKafkaHeaderMapper mapper = new DefaultKafkaHeaderMapper();
		MessageHeaders headers = new MessageHeaders(
				Collections.singletonMap("foo",
						Arrays.asList(MimeType.valueOf("application/json"), MimeType.valueOf("text/plain"))));

		RecordHeaders recordHeaders = new RecordHeaders();
		mapper.fromHeaders(headers, recordHeaders);
		Map<String, Object> receivedHeaders = new HashMap<>();
		mapper.toHeaders(recordHeaders, receivedHeaders);
		Object fooHeader = receivedHeaders.get("foo");
		assertThat(fooHeader).isInstanceOf(List.class);
		assertThat(fooHeader).asList().containsExactly("application/json", "text/plain");
	}

	@Test
	void testSpecificStringConvert() {
		DefaultKafkaHeaderMapper mapper = new DefaultKafkaHeaderMapper();
		Map<String, Boolean> rawMappedHeaders = new HashMap<>();
		rawMappedHeaders.put("thisOnesAString", true);
		rawMappedHeaders.put("thisOnesBytes", false);
		mapper.setRawMappedHeaders(rawMappedHeaders);
		Map<String, Object> headersMap = new HashMap<>();
		headersMap.put("thisOnesAString", "foo");
		headersMap.put("thisOnesBytes", "bar");
		headersMap.put("alwaysRaw", "baz".getBytes());
		MessageHeaders headers = new MessageHeaders(headersMap);
		Headers target = new RecordHeaders();
		mapper.fromHeaders(headers, target);
		assertThat(target).containsExactlyInAnyOrder(
				new RecordHeader("thisOnesAString", "foo".getBytes()),
				new RecordHeader("thisOnesBytes", "bar".getBytes()),
				new RecordHeader("alwaysRaw", "baz".getBytes()));
		headersMap.clear();
		mapper.toHeaders(target, headersMap);
		assertThat(headersMap).contains(
				entry("thisOnesAString", "foo"),
				entry("thisOnesBytes", "bar".getBytes()),
				entry("alwaysRaw", "baz".getBytes()));
	}

	@Test
	void testJsonStringConvert() {
		DefaultKafkaHeaderMapper mapper = new DefaultKafkaHeaderMapper();
		Map<String, Boolean> rawMappedHeaders = new HashMap<>();
		rawMappedHeaders.put("thisOnesBytes", false);
		mapper.setRawMappedHeaders(rawMappedHeaders);
		Map<String, Object> headersMap = new HashMap<>();
		headersMap.put("thisOnesAString", "foo");
		headersMap.put("thisOnesBytes", "bar");
		headersMap.put("thisOnesEmpty", "");
		headersMap.put("alwaysRaw", "baz".getBytes());
		MessageHeaders headers = new MessageHeaders(headersMap);
		Headers target = new RecordHeaders();
		mapper.fromHeaders(headers, target);
		assertThat(target).containsExactlyInAnyOrder(
				new RecordHeader(DefaultKafkaHeaderMapper.JSON_TYPES,
						("{\"thisOnesEmpty\":\"java.lang.String\","
								+ "\"thisOnesAString\":\"java.lang.String\"}").getBytes()),
				new RecordHeader("thisOnesAString", "foo".getBytes()),
				new RecordHeader("alwaysRaw", "baz".getBytes()),
				new RecordHeader("thisOnesEmpty", "".getBytes()),
				new RecordHeader("thisOnesBytes", "bar".getBytes()));
		headersMap.clear();
		target.add(new RecordHeader(DefaultKafkaHeaderMapper.JSON_TYPES,
				("{\"thisOnesEmpty\":\"java.lang.String\","
						+ "\"thisOnesAString\":\"java.lang.String\","
						+ "\"backwardCompatible\":\"java.lang.String\"}").getBytes()));
		target.add(new RecordHeader("backwardCompatible", "\"qux\"".getBytes()));
		mapper.toHeaders(target, headersMap);
		assertThat(headersMap).contains(
				entry("thisOnesAString", "foo"),
				entry("thisOnesEmpty", ""),
				entry("thisOnesBytes", "bar".getBytes()),
				entry("alwaysRaw", "baz".getBytes()),
				entry("backwardCompatible", "qux"));
		// Now with String encoding
		mapper.setEncodeStrings(true);
		target = new RecordHeaders();
		mapper.fromHeaders(headers, target);
		assertThat(target).containsExactlyInAnyOrder(
				new RecordHeader(DefaultKafkaHeaderMapper.JSON_TYPES,
						("{\"thisOnesEmpty\":\"java.lang.String\","
								+ "\"thisOnesAString\":\"java.lang.String\"}").getBytes()),
				new RecordHeader("thisOnesAString", "\"foo\"".getBytes()),
				new RecordHeader("thisOnesEmpty", "\"\"".getBytes()),
				new RecordHeader("alwaysRaw", "baz".getBytes()),
				new RecordHeader("thisOnesBytes", "bar".getBytes()));
	}

	@Test
	void testAlwaysStringConvert() {
		DefaultKafkaHeaderMapper mapper = new DefaultKafkaHeaderMapper();
		mapper.setMapAllStringsOut(true);
		Map<String, Boolean> rawMappedHeaders = new HashMap<>();
		rawMappedHeaders.put("thisOnesBytes", false);
		mapper.setRawMappedHeaders(rawMappedHeaders);
		Map<String, Object> headersMap = new HashMap<>();
		headersMap.put("thisOnesAString", "foo");
		headersMap.put("thisOnesBytes", "bar");
		headersMap.put("alwaysRaw", "baz".getBytes());
		MessageHeaders headers = new MessageHeaders(headersMap);
		Headers target = new RecordHeaders();
		mapper.fromHeaders(headers, target);
		assertThat(target).containsExactlyInAnyOrder(
				new RecordHeader("thisOnesAString", "foo".getBytes()),
				new RecordHeader("thisOnesBytes", "bar".getBytes()),
				new RecordHeader("alwaysRaw", "baz".getBytes()));
		headersMap.clear();
		mapper.toHeaders(target, headersMap);
		assertThat(headersMap).contains(
				entry("thisOnesAString", "foo".getBytes()),
				entry("thisOnesBytes", "bar".getBytes()),
				entry("alwaysRaw", "baz".getBytes()));
	}

	@Test
	void deliveryAttempt() {
		DefaultKafkaHeaderMapper mapper = new DefaultKafkaHeaderMapper();
		byte[] delivery = new byte[4];
		ByteBuffer.wrap(delivery).putInt(42);
		Headers headers = new RecordHeaders(new Header[] { new RecordHeader(KafkaHeaders.DELIVERY_ATTEMPT, delivery) });
		Map<String, Object> springHeaders = new HashMap<>();
		mapper.toHeaders(headers, springHeaders);
		assertThat(springHeaders.get(KafkaHeaders.DELIVERY_ATTEMPT)).isEqualTo(42);
		headers = new RecordHeaders();
		mapper.fromHeaders(new MessageHeaders(springHeaders), headers);
		assertThat(headers.lastHeader(KafkaHeaders.DELIVERY_ATTEMPT)).isNull();
	}

	@Test
	void listenerInfo() {
		DefaultKafkaHeaderMapper mapper = new DefaultKafkaHeaderMapper();
		Headers headers = new RecordHeaders(
				new Header[] { new RecordHeader(KafkaHeaders.LISTENER_INFO, "info".getBytes()) });
		Map<String, Object> springHeaders = new HashMap<>();
		mapper.toHeaders(headers, springHeaders);
		assertThat(springHeaders.get(KafkaHeaders.LISTENER_INFO)).isEqualTo("info");
		headers = new RecordHeaders();
		mapper.fromHeaders(new MessageHeaders(springHeaders), headers);
		assertThat(headers.lastHeader(KafkaHeaders.LISTENER_INFO)).isNull();
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
				return other.bar == null;
			}
			else {
				return this.bar.equals(other.bar);
			}
		}

	}

	public static class Bar {

		private String field;

		public Bar() {
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
