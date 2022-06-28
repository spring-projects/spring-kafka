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

package org.springframework.kafka.support;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatIllegalStateException;
import static org.assertj.core.api.Assertions.entry;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.junit.jupiter.api.Test;

import org.springframework.messaging.MessageHeaders;

/**
 * @author Gary Russell
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

}
