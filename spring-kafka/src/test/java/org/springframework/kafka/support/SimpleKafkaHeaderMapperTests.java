/*
 * Copyright 2019 the original author or authors.
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

import java.util.HashMap;
import java.util.Map;

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
}
