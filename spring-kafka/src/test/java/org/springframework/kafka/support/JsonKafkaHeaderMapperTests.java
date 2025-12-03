/*
 * Copyright 2017-present the original author or authors.
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
import org.assertj.core.api.InstanceOfAssertFactories;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import org.springframework.core.log.LogAccessor;
import org.springframework.kafka.retrytopic.RetryTopicHeaders;
import org.springframework.kafka.support.JsonKafkaHeaderMapper.NonTrustedHeaderType;
import org.springframework.kafka.support.serializer.DeserializationException;
import org.springframework.kafka.support.serializer.SerializationTestUtils;
import org.springframework.kafka.support.serializer.SerializationUtils;
import org.springframework.messaging.Message;
import org.springframework.messaging.MessageHeaders;
import org.springframework.messaging.support.ExecutorSubscribableChannel;
import org.springframework.messaging.support.MessageBuilder;
import org.springframework.util.LinkedMultiValueMap;
import org.springframework.util.MimeType;
import org.springframework.util.MimeTypeUtils;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.entry;
import static org.mockito.BDDMockito.given;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

/**
 * @author Gary Russell
 * @author Artem Bilan
 * @author Soby Chacko
 * @author Sanghyeok An
 *
 * @since 1.3
 *
 */
public class JsonKafkaHeaderMapperTests {

	@Test
	void testTrustedAndNot() {
		JsonKafkaHeaderMapper mapper = new JsonKafkaHeaderMapper();
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
		JsonKafkaHeaderMapper mapper = new JsonKafkaHeaderMapper();
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
	void testTrustedPackages() {
		JsonKafkaHeaderMapper mapper = new JsonKafkaHeaderMapper();
		MessageHeaders headers = new MessageHeaders(
				Collections.singletonMap("foo",
						Arrays.asList(MimeType.valueOf("application/json"), MimeType.valueOf("text/plain"))));

		RecordHeaders recordHeaders = new RecordHeaders();
		mapper.fromHeaders(headers, recordHeaders);
		Map<String, Object> receivedHeaders = new HashMap<>();
		mapper.toHeaders(recordHeaders, receivedHeaders);
		Object fooHeader = receivedHeaders.get("foo");
		assertThat(fooHeader).isInstanceOf(List.class);
		assertThat(fooHeader).asInstanceOf(InstanceOfAssertFactories.LIST)
				.containsExactly("application/json", "text/plain");
	}

	@Test
	void testSpecificStringConvert() {
		JsonKafkaHeaderMapper mapper = new JsonKafkaHeaderMapper();
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
		JsonKafkaHeaderMapper mapper = new JsonKafkaHeaderMapper();
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
				new RecordHeader(JsonKafkaHeaderMapper.JSON_TYPES,
						("{\"thisOnesEmpty\":\"java.lang.String\","
								+ "\"thisOnesAString\":\"java.lang.String\"}").getBytes()),
				new RecordHeader("thisOnesAString", "foo".getBytes()),
				new RecordHeader("alwaysRaw", "baz".getBytes()),
				new RecordHeader("thisOnesEmpty", "".getBytes()),
				new RecordHeader("thisOnesBytes", "bar".getBytes()));
		headersMap.clear();
		target.add(new RecordHeader(JsonKafkaHeaderMapper.JSON_TYPES,
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
				new RecordHeader(JsonKafkaHeaderMapper.JSON_TYPES,
						("{\"thisOnesEmpty\":\"java.lang.String\","
								+ "\"thisOnesAString\":\"java.lang.String\"}").getBytes()),
				new RecordHeader("thisOnesAString", "\"foo\"".getBytes()),
				new RecordHeader("thisOnesEmpty", "\"\"".getBytes()),
				new RecordHeader("alwaysRaw", "baz".getBytes()),
				new RecordHeader("thisOnesBytes", "bar".getBytes()));
	}

	@Test
	void testAlwaysStringConvert() {
		JsonKafkaHeaderMapper mapper = new JsonKafkaHeaderMapper();
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
		JsonKafkaHeaderMapper mapper = new JsonKafkaHeaderMapper();
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
		JsonKafkaHeaderMapper mapper = new JsonKafkaHeaderMapper();
		Headers headers = new RecordHeaders(
				new Header[] { new RecordHeader(KafkaHeaders.LISTENER_INFO, "info".getBytes()) });
		Map<String, Object> springHeaders = new HashMap<>();
		mapper.toHeaders(headers, springHeaders);
		assertThat(springHeaders.get(KafkaHeaders.LISTENER_INFO)).isEqualTo("info");
		headers = new RecordHeaders();
		mapper.fromHeaders(new MessageHeaders(springHeaders), headers);
		assertThat(headers.lastHeader(KafkaHeaders.LISTENER_INFO)).isNull();
	}

	@Test
	void inboundJson() {
		JsonKafkaHeaderMapper outboundMapper = new JsonKafkaHeaderMapper();
		JsonKafkaHeaderMapper inboundMapper = JsonKafkaHeaderMapper.forInboundOnlyWithMatchers("!fo*", "*");
		HashMap<String, Object> map = new HashMap<>();
		map.put("foo", "bar");
		map.put("foa", "bar");
		map.put("baz", "qux");
		MessageHeaders msgHeaders = new MessageHeaders(map);
		Headers headers = new RecordHeaders();
		outboundMapper.fromHeaders(msgHeaders, headers);
		headers.add(KafkaHeaders.DELIVERY_ATTEMPT, new byte[] { 0, 0, 0, 1 });
		map.clear();
		inboundMapper.toHeaders(headers, map);
		assertThat(map).doesNotContainKey("foo")
				.doesNotContainKey("foa")
				.containsKey(KafkaHeaders.DELIVERY_ATTEMPT)
				.containsKey("baz");
	}

	@Test
	void multiValueHeaderToTest() {
		// GIVEN
		String multiValueHeader1 = "test-multi-value1";
		byte[] multiValueHeader1Value1 = { 0, 0, 0, 0 };
		byte[] multiValueHeader1Value2 = { 0, 0, 0, 1 };
		byte[] multiValueHeader1Value3 = { 0, 0, 0, 2 };
		byte[] multiValueHeader1Value4 = { 0, 0, 0, 3 };

		String multiValueHeader2 = "test-multi-value2";
		byte[] multiValueHeader2Value1 = { 0, 0, 0, 4 };
		byte[] multiValueHeader2Value2 = { 0, 0, 0, 5 };

		String multiValueWildCardHeader1 = "test-wildcard-value1";
		byte[] multiValueWildCardHeader1Value1 = { 0, 0, 0, 6 };
		byte[] multiValueWildCardHeader1Value2 = { 0, 0, 0, 7 };

		String multiValueWildCardHeader2 = "test-wildcard-value2";
		byte[] multiValueWildCardHeader2Value1 = { 0, 0, 0, 8 };
		byte[] multiValueWildCardHeader2Value2 = { 0, 0, 0, 9 };

		String singleValueHeader = "test-single-value1";
		byte[] singleValueHeaderValue = { 0, 0, 0, 6 };

		JsonKafkaHeaderMapper mapper = new JsonKafkaHeaderMapper();
		mapper.setMultiValueHeaderPatterns(multiValueHeader1, multiValueHeader2, "test-wildcard-*");

		Headers rawHeaders = new RecordHeaders();

		byte[] deliveryAttemptHeaderValue = { 0, 0, 0, 1 };
		byte[] originalOffsetHeaderValue = { 0, 0, 0, 1 };
		byte[] defaultHeaderAttemptsValues = { 0, 0, 0, 5 };

		rawHeaders.add(KafkaHeaders.DELIVERY_ATTEMPT, deliveryAttemptHeaderValue);
		rawHeaders.add(KafkaHeaders.ORIGINAL_OFFSET, originalOffsetHeaderValue);
		rawHeaders.add(RetryTopicHeaders.DEFAULT_HEADER_ATTEMPTS, defaultHeaderAttemptsValues);
		rawHeaders.add(singleValueHeader, singleValueHeaderValue);

		rawHeaders.add(multiValueHeader1, multiValueHeader1Value1);
		rawHeaders.add(multiValueHeader1, multiValueHeader1Value2);
		rawHeaders.add(multiValueHeader1, multiValueHeader1Value3);
		rawHeaders.add(multiValueHeader1, multiValueHeader1Value4);

		rawHeaders.add(multiValueHeader2, multiValueHeader2Value1);
		rawHeaders.add(multiValueHeader2, multiValueHeader2Value2);

		rawHeaders.add(multiValueWildCardHeader1, multiValueWildCardHeader1Value1);
		rawHeaders.add(multiValueWildCardHeader1, multiValueWildCardHeader1Value2);
		rawHeaders.add(multiValueWildCardHeader2, multiValueWildCardHeader2Value1);
		rawHeaders.add(multiValueWildCardHeader2, multiValueWildCardHeader2Value2);

		// WHEN
		Map<String, Object> mappedHeaders = new HashMap<>();
		mapper.toHeaders(rawHeaders, mappedHeaders);

		// THEN
		assertThat(mappedHeaders.get(KafkaHeaders.DELIVERY_ATTEMPT)).isEqualTo(1);
		assertThat(mappedHeaders.get(KafkaHeaders.ORIGINAL_OFFSET)).isEqualTo(originalOffsetHeaderValue);
		assertThat(mappedHeaders.get(RetryTopicHeaders.DEFAULT_HEADER_ATTEMPTS)).isEqualTo(defaultHeaderAttemptsValues);
		assertThat(mappedHeaders.get(singleValueHeader)).isEqualTo(singleValueHeaderValue);

		assertThat(mappedHeaders)
				.extractingByKey(multiValueHeader1, InstanceOfAssertFactories.list(byte[].class))
				.containsExactly(multiValueHeader1Value1, multiValueHeader1Value2,
								multiValueHeader1Value3, multiValueHeader1Value4);

		assertThat(mappedHeaders)
				.extractingByKey(multiValueHeader2, InstanceOfAssertFactories.list(byte[].class))
				.containsExactly(multiValueHeader2Value1, multiValueHeader2Value2);

		assertThat(mappedHeaders)
				.extractingByKey(multiValueWildCardHeader1, InstanceOfAssertFactories.list(byte[].class))
				.containsExactly(multiValueWildCardHeader1Value1, multiValueWildCardHeader1Value2);

		assertThat(mappedHeaders)
				.extractingByKey(multiValueWildCardHeader2, InstanceOfAssertFactories.list(byte[].class))
				.containsExactly(multiValueWildCardHeader2Value1, multiValueWildCardHeader2Value2);
	}

	@ParameterizedTest
	@ValueSource(ints = {500, 1000, 2000})
	void hugeNumberOfSingleValueHeaderToTest(int numberOfSingleValueHeaderCount) {
		// GIVEN
		Headers rawHeaders = new RecordHeaders();

		String multiValueHeader1 = "test-multi-value1";
		byte[] multiValueHeader1Value1 = { 0, 0, 0, 0 };
		byte[] multiValueHeader1Value2 = { 0, 0, 0, 1 };

		rawHeaders.add(multiValueHeader1, multiValueHeader1Value1);
		rawHeaders.add(multiValueHeader1, multiValueHeader1Value2);

		byte[] deliveryAttemptHeaderValue = { 0, 0, 0, 1 };
		byte[] originalOffsetHeaderValue = { 0, 0, 0, 2 };
		byte[] defaultHeaderAttemptsValues = { 0, 0, 0, 5 };

		rawHeaders.add(KafkaHeaders.DELIVERY_ATTEMPT, deliveryAttemptHeaderValue);
		rawHeaders.add(KafkaHeaders.ORIGINAL_OFFSET, originalOffsetHeaderValue);
		rawHeaders.add(RetryTopicHeaders.DEFAULT_HEADER_ATTEMPTS, defaultHeaderAttemptsValues);

		byte[] singleValueHeaderValue = { 0, 0, 0, 6 };
		for (int i = 0; i < numberOfSingleValueHeaderCount; i++) {
			String singleValueHeader = "test-single-value" + i;
			rawHeaders.add(singleValueHeader, singleValueHeaderValue);
		}

		JsonKafkaHeaderMapper mapper = new JsonKafkaHeaderMapper();
		mapper.setMultiValueHeaderPatterns(multiValueHeader1);

		// WHEN
		Map<String, Object> mappedHeaders = new HashMap<>();
		mapper.toHeaders(rawHeaders, mappedHeaders);

		// THEN
		assertThat(mappedHeaders.get(KafkaHeaders.DELIVERY_ATTEMPT)).isEqualTo(1);
		assertThat(mappedHeaders.get(KafkaHeaders.ORIGINAL_OFFSET)).isEqualTo(originalOffsetHeaderValue);
		assertThat(mappedHeaders.get(RetryTopicHeaders.DEFAULT_HEADER_ATTEMPTS)).isEqualTo(defaultHeaderAttemptsValues);

		for (int i = 0; i < numberOfSingleValueHeaderCount; i++) {
			String singleValueHeader = "test-single-value" + i;
			assertThat(mappedHeaders.get(singleValueHeader)).isEqualTo(singleValueHeaderValue);
		}

		assertThat(mappedHeaders)
				.extractingByKey(multiValueHeader1, InstanceOfAssertFactories.list(byte[].class))
				.containsExactly(multiValueHeader1Value1, multiValueHeader1Value2);
	}

	@ParameterizedTest
	@ValueSource(ints = {500, 1000, 2000})
	void hugeNumberOfMultiValueHeaderToTest(int numberOfMultiValueHeaderCount) {
		// GIVEN
		JsonKafkaHeaderMapper mapper = new JsonKafkaHeaderMapper();
		Headers rawHeaders = new RecordHeaders();

		byte[] multiValueHeader1Value1 = { 0, 0, 0, 0 };
		byte[] multiValueHeader1Value2 = { 0, 0, 0, 1 };

		for (int i = 0; i < numberOfMultiValueHeaderCount; i++) {
			String multiValueHeader = "test-multi-value" + i;
			mapper.setMultiValueHeaderPatterns(multiValueHeader);
			rawHeaders.add(multiValueHeader, multiValueHeader1Value1);
			rawHeaders.add(multiValueHeader, multiValueHeader1Value2);
		}

		byte[] deliveryAttemptHeaderValue = { 0, 0, 0, 1 };
		byte[] originalOffsetHeaderValue = { 0, 0, 0, 2 };
		byte[] defaultHeaderAttemptsValues = { 0, 0, 0, 5 };

		rawHeaders.add(KafkaHeaders.DELIVERY_ATTEMPT, deliveryAttemptHeaderValue);
		rawHeaders.add(KafkaHeaders.ORIGINAL_OFFSET, originalOffsetHeaderValue);
		rawHeaders.add(RetryTopicHeaders.DEFAULT_HEADER_ATTEMPTS, defaultHeaderAttemptsValues);

		String singleValueHeader = "test-single-value";
		byte[] singleValueHeaderValue = { 0, 0, 0, 6 };
		rawHeaders.add(singleValueHeader, singleValueHeaderValue);

		// WHEN
		Map<String, Object> mappedHeaders = new HashMap<>();
		mapper.toHeaders(rawHeaders, mappedHeaders);

		// THEN
		assertThat(mappedHeaders.get(KafkaHeaders.DELIVERY_ATTEMPT)).isEqualTo(1);
		assertThat(mappedHeaders.get(KafkaHeaders.ORIGINAL_OFFSET)).isEqualTo(originalOffsetHeaderValue);
		assertThat(mappedHeaders.get(RetryTopicHeaders.DEFAULT_HEADER_ATTEMPTS)).isEqualTo(defaultHeaderAttemptsValues);
		assertThat(mappedHeaders.get(singleValueHeader)).isEqualTo(singleValueHeaderValue);

		for (int i = 0; i < numberOfMultiValueHeaderCount; i++) {
			String multiValueHeader = "test-multi-value" + i;
			assertThat(mappedHeaders)
					.extractingByKey(multiValueHeader, InstanceOfAssertFactories.list(byte[].class))
					.containsExactly(multiValueHeader1Value1, multiValueHeader1Value2);
		}
	}

	@Test
	void multiValueHeaderFromTest() {
		// GIVEN
		String multiValueHeader1 = "test-multi-value1";
		byte[] multiValueHeader1Value1 = { 0, 0, 0, 1 };
		byte[] multiValueHeader1Value2 = { 0, 0, 0, 2 };

		String multiValueHeader2 = "test-multi-value2";
		byte[] multiValueHeader2Value1 = { 0, 0, 0, 3 };
		byte[] multiValueHeader2Value2 = { 0, 0, 0, 4 };

		String multiValueHeader3 = "test-other-multi-value1";
		byte[] multiValueHeader3Value1 = { 0, 0, 0, 9 };
		byte[] multiValueHeader3Value2 = { 0, 0, 0, 10 };

		String multiValueHeader4 = "test-prefix-match-multi";
		byte[] multiValueHeader4Value1 = { 0, 0, 0, 11 };
		byte[] multiValueHeader4Value2 = { 0, 0, 0, 12 };

		String singleValueHeader = "test-single-value1";
		byte[] singleValueHeaderValue1 = { 0, 0, 0, 5 };

		Message<String> message = MessageBuilder
				.withPayload("test-multi-value-header")
				.setHeader(multiValueHeader1, List.of(multiValueHeader1Value1,
													multiValueHeader1Value2))
				.setHeader(multiValueHeader2, List.of(multiValueHeader2Value1,
													multiValueHeader2Value2))
				.setHeader(multiValueHeader3, List.of(multiValueHeader3Value1,
													multiValueHeader3Value2))
				.setHeader(multiValueHeader4, List.of(multiValueHeader4Value1,
													multiValueHeader4Value2))
				.setHeader(singleValueHeader, singleValueHeaderValue1)
				.build();

		JsonKafkaHeaderMapper mapper = new JsonKafkaHeaderMapper();
		mapper.setMultiValueHeaderPatterns("test-multi-*",
											multiValueHeader3,
											"*-prefix-match-multi*");

		// WHEN
		Headers results = new RecordHeaders();
		mapper.fromHeaders(message.getHeaders(), results);

		// THEN
		assertThat(results.headers(multiValueHeader1))
				.extracting(Header::value)
				.containsExactly(multiValueHeader1Value1, multiValueHeader1Value2);

		assertThat(results.headers(multiValueHeader2))
				.extracting(Header::value)
				.containsExactly(multiValueHeader2Value1, multiValueHeader2Value2);

		assertThat(results.headers(multiValueHeader3))
				.extracting(Header::value)
				.containsExactly(multiValueHeader3Value1, multiValueHeader3Value2);

		assertThat(results.headers(multiValueHeader4))
				.extracting(Header::value)
				.containsExactly(multiValueHeader4Value1, multiValueHeader4Value2);

		assertThat(results.headers(singleValueHeader))
				.extracting(Header::value)
				.containsExactly(singleValueHeaderValue1);
	}

	@Test
	void deserializationExceptionHeadersAreMappedAsNonByteArray() {
		JsonKafkaHeaderMapper mapper = new JsonKafkaHeaderMapper();

		byte[] keyDeserExceptionBytes = SerializationTestUtils.header(true);
		Header keyHeader = SerializationTestUtils.deserializationHeader(KafkaUtils.KEY_DESERIALIZER_EXCEPTION_HEADER,
				keyDeserExceptionBytes);
		byte[] valueDeserExceptionBytes = SerializationTestUtils.header(false);
		Header valueHeader = SerializationTestUtils.deserializationHeader(KafkaUtils.VALUE_DESERIALIZER_EXCEPTION_HEADER,
				valueDeserExceptionBytes);
		Headers headers = new RecordHeaders(
				new Header[] { keyHeader, valueHeader });
		Map<String, Object> springHeaders = new HashMap<>();
		mapper.toHeaders(headers, springHeaders);
		assertThat(springHeaders.get(KafkaUtils.KEY_DESERIALIZER_EXCEPTION_HEADER)).isEqualTo(keyHeader);
		assertThat(springHeaders.get(KafkaUtils.VALUE_DESERIALIZER_EXCEPTION_HEADER)).isEqualTo(valueHeader);

		LogAccessor logger = new LogAccessor(this.getClass());

		DeserializationException keyDeserializationException = SerializationUtils.byteArrayToDeserializationException(logger, keyHeader);
		assertThat(keyDeserExceptionBytes).containsExactly(SerializationTestUtils.header(keyDeserializationException));

		DeserializationException valueDeserializationException =
				SerializationUtils.byteArrayToDeserializationException(logger, valueHeader);
		assertThat(valueDeserExceptionBytes).containsExactly(SerializationTestUtils.header(valueDeserializationException));

		headers = new RecordHeaders();
		mapper.fromHeaders(new MessageHeaders(springHeaders), headers);
		assertThat(headers.lastHeader(KafkaUtils.KEY_DESERIALIZER_EXCEPTION_HEADER)).isNull();
		assertThat(headers.lastHeader(KafkaUtils.VALUE_DESERIALIZER_EXCEPTION_HEADER)).isNull();
	}

	@Test
	void ensureNullHeaderValueHandledGraciously() {
		JsonKafkaHeaderMapper mapper = new JsonKafkaHeaderMapper();

		Header mockHeader = mock(Header.class);
		given(mockHeader.value()).willReturn(null);

		Object result = mapper.headerValueToAddIn(mockHeader);

		assertThat(result).isNull();
		verify(mockHeader).value();
		verify(mockHeader, never()).key();
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
