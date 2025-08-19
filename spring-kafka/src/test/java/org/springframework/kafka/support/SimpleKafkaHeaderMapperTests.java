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

package org.springframework.kafka.support;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.assertj.core.api.InstanceOfAssertFactories;
import org.junit.jupiter.api.Test;

import org.springframework.kafka.retrytopic.RetryTopicHeaders;
import org.springframework.messaging.Message;
import org.springframework.messaging.MessageHeaders;
import org.springframework.messaging.support.MessageBuilder;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatIllegalStateException;
import static org.assertj.core.api.Assertions.entry;

/**
 * @author Gary Russell
 * @author Sanghyeok An
 * @since 2.2.5
 *
 */
public class SimpleKafkaHeaderMapperTests {

	@Test
	public void testSpecificStringConvert() {
		SimpleKafkaHeaderMapper mapper = new SimpleKafkaHeaderMapper();
		Map<String, Boolean> rawMappedHeaders = new HashMap<>();
		rawMappedHeaders.put("thisOnesAString", true);
		rawMappedHeaders.put("thisOnesBytes", false);
		mapper.setRawMappedHeaders(rawMappedHeaders);
		Map<String, Object> headersMap = new HashMap<>();
		headersMap.put("thisOnesAString", "foo");
		headersMap.put("thisOnesBytes", "bar");
		headersMap.put("neverConverted", "baz".getBytes());
		MessageHeaders headers = new MessageHeaders(headersMap);
		Headers target = new RecordHeaders();
		mapper.fromHeaders(headers, target);
		assertThat(target).containsExactlyInAnyOrder(
				new RecordHeader("thisOnesAString", "foo".getBytes()),
				new RecordHeader("thisOnesBytes", "bar".getBytes()),
				new RecordHeader("neverConverted", "baz".getBytes()));
		headersMap.clear();
		mapper.toHeaders(target, headersMap);
		assertThat(headersMap).contains(
				entry("thisOnesAString", "foo"),
				entry("thisOnesBytes", "bar".getBytes()),
				entry("neverConverted", "baz".getBytes()));
	}

	@Test
	public void testNotStringConvert() {
		SimpleKafkaHeaderMapper mapper = new SimpleKafkaHeaderMapper();
		Map<String, Boolean> rawMappedHeaders = new HashMap<>();
		rawMappedHeaders.put("thisOnesBytes", false);
		mapper.setRawMappedHeaders(rawMappedHeaders);
		Map<String, Object> headersMap = new HashMap<>();
		headersMap.put("thisOnesAString", "foo");
		headersMap.put("thisOnesBytes", "bar");
		headersMap.put("neverConverted", "baz".getBytes());
		MessageHeaders headers = new MessageHeaders(headersMap);
		Headers target = new RecordHeaders();
		mapper.fromHeaders(headers, target);
		assertThat(target).containsExactlyInAnyOrder(
				new RecordHeader("neverConverted", "baz".getBytes()),
				new RecordHeader("thisOnesBytes", "bar".getBytes()));
		headersMap.clear();
		mapper.toHeaders(target, headersMap);
		assertThat(headersMap).contains(
				entry("thisOnesBytes", "bar".getBytes()),
				entry("neverConverted", "baz".getBytes()));
	}

	@Test
	public void testAlwaysStringConvert() {
		SimpleKafkaHeaderMapper mapper = new SimpleKafkaHeaderMapper();
		mapper.setMapAllStringsOut(true);
		Map<String, Boolean> rawMappedHeaders = new HashMap<>();
		rawMappedHeaders.put("thisOnesBytes", false);
		mapper.setRawMappedHeaders(rawMappedHeaders);
		Map<String, Object> headersMap = new HashMap<>();
		headersMap.put("thisOnesAString", "foo");
		headersMap.put("thisOnesBytes", "bar");
		headersMap.put("neverConverted", "baz".getBytes());
		MessageHeaders headers = new MessageHeaders(headersMap);
		Headers target = new RecordHeaders();
		mapper.fromHeaders(headers, target);
		assertThat(target).containsExactlyInAnyOrder(
				new RecordHeader("thisOnesAString", "foo".getBytes()),
				new RecordHeader("thisOnesBytes", "bar".getBytes()),
				new RecordHeader("neverConverted", "baz".getBytes()));
		headersMap.clear();
		mapper.toHeaders(target, headersMap);
		assertThat(headersMap).contains(
				entry("thisOnesAString", "foo".getBytes()),
				entry("thisOnesBytes", "bar".getBytes()),
				entry("neverConverted", "baz".getBytes()));
	}

	@Test
	public void testDefaultHeaderPatterns() {
		SimpleKafkaHeaderMapper mapper = new SimpleKafkaHeaderMapper();
		mapper.setMapAllStringsOut(true);
		Map<String, Object> headersMap = new HashMap<>();
		headersMap.put(MessageHeaders.ID, "foo".getBytes());
		headersMap.put(MessageHeaders.TIMESTAMP, "bar");
		headersMap.put("thisOnePresent", "baz");
		MessageHeaders headers = new MessageHeaders(headersMap);
		Headers target = new RecordHeaders();
		mapper.fromHeaders(headers, target);
		assertThat(target).contains(
				new RecordHeader("thisOnePresent", "baz".getBytes()));
		headersMap.clear();
		mapper.toHeaders(target, headersMap);
		assertThat(headersMap).contains(
				entry("thisOnePresent", "baz".getBytes()));
	}

	@Test
	void deliveryAttempt() {
		SimpleKafkaHeaderMapper mapper = new SimpleKafkaHeaderMapper();
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
		SimpleKafkaHeaderMapper mapper = new SimpleKafkaHeaderMapper();
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
	void inboundMappingNoPatterns() {
		SimpleKafkaHeaderMapper inboundMapper = SimpleKafkaHeaderMapper.forInboundOnlyWithMatchers();
		Headers headers = new RecordHeaders();
		headers.add("foo", "bar".getBytes());
		headers.add(KafkaHeaders.DELIVERY_ATTEMPT, new byte[] { 0, 0, 0, 1 });
		Map<String, Object> mapped = new HashMap<>();
		inboundMapper.toHeaders(headers, mapped);
		assertThat(mapped).containsKey("foo")
				.containsKey(KafkaHeaders.DELIVERY_ATTEMPT);
		assertThatIllegalStateException()
				.isThrownBy(() -> inboundMapper.fromHeaders(new MessageHeaders(mapped), headers));
	}

	@Test
	void inboundMappingWithPatterns() {
		SimpleKafkaHeaderMapper inboundMapper = SimpleKafkaHeaderMapper.forInboundOnlyWithMatchers("!foo", "*");
		Headers headers = new RecordHeaders();
		headers.add("foo", "bar".getBytes());
		headers.add(KafkaHeaders.DELIVERY_ATTEMPT, new byte[] { 0, 0, 0, 1 });
		Map<String, Object> mapped = new HashMap<>();
		inboundMapper.toHeaders(headers, mapped);
		assertThat(mapped).doesNotContainKey("foo")
				.containsKey(KafkaHeaders.DELIVERY_ATTEMPT);
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

		String singleValueHeader = "test-single-value1";
		byte[] singleValueHeaderValue = { 0, 0, 0, 6 };

		byte[] deliveryAttemptValue = { 0, 0, 0, 1 };
		byte[] originalOffset = { 0, 0, 0, 1 };
		byte[] defaultHeaderAttempts = { 0, 0, 0, 5 };

		JsonKafkaHeaderMapper mapper = new JsonKafkaHeaderMapper();
		mapper.setMultiValueHeaderPatterns(multiValueHeader1, multiValueHeader2);

		Headers rawHeaders = new RecordHeaders();
		rawHeaders.add(KafkaHeaders.DELIVERY_ATTEMPT, deliveryAttemptValue);
		rawHeaders.add(KafkaHeaders.ORIGINAL_OFFSET, originalOffset);
		rawHeaders.add(RetryTopicHeaders.DEFAULT_HEADER_ATTEMPTS, defaultHeaderAttempts);
		rawHeaders.add(singleValueHeader, singleValueHeaderValue);

		rawHeaders.add(multiValueHeader1, multiValueHeader1Value1);
		rawHeaders.add(multiValueHeader1, multiValueHeader1Value2);
		rawHeaders.add(multiValueHeader1, multiValueHeader1Value3);
		rawHeaders.add(multiValueHeader1, multiValueHeader1Value4);

		rawHeaders.add(multiValueHeader2, multiValueHeader2Value1);
		rawHeaders.add(multiValueHeader2, multiValueHeader2Value2);

		// WHEN
		Map<String, Object> mappedHeaders = new HashMap<>();
		mapper.toHeaders(rawHeaders, mappedHeaders);

		// THEN
		assertThat(mappedHeaders.get(KafkaHeaders.DELIVERY_ATTEMPT)).isEqualTo(1);
		assertThat(mappedHeaders.get(KafkaHeaders.ORIGINAL_OFFSET)).isEqualTo(originalOffset);
		assertThat(mappedHeaders.get(RetryTopicHeaders.DEFAULT_HEADER_ATTEMPTS)).isEqualTo(defaultHeaderAttempts);
		assertThat(mappedHeaders.get(singleValueHeader)).isEqualTo(singleValueHeaderValue);

		assertThat(mappedHeaders)
				.extractingByKey(multiValueHeader1, InstanceOfAssertFactories.list(byte[].class))
				.containsExactly(multiValueHeader1Value1, multiValueHeader1Value2,
								multiValueHeader1Value3, multiValueHeader1Value4);

		assertThat(mappedHeaders)
				.extractingByKey(multiValueHeader2, InstanceOfAssertFactories.list(byte[].class))
				.containsExactly(multiValueHeader2Value1, multiValueHeader2Value2);
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

		String singleValueHeader = "test-single-value1";
		byte[] singleValueHeaderValue = { 0, 0, 0, 5 };

		Message<String> message = MessageBuilder
				.withPayload("test-multi-value-header")
				.setHeader(multiValueHeader1, List.of(multiValueHeader1Value1, multiValueHeader1Value2))
				.setHeader(multiValueHeader2, List.of(multiValueHeader2Value1, multiValueHeader2Value2))
				.setHeader(singleValueHeader, singleValueHeaderValue)
				.build();

		SimpleKafkaHeaderMapper mapper = new SimpleKafkaHeaderMapper();
		mapper.setMultiValueHeaderPatterns(multiValueHeader1, multiValueHeader2);

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

		assertThat(results.headers(singleValueHeader))
				.extracting(Header::value)
				.containsExactly(singleValueHeaderValue);
	}

}
