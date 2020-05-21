/*
 * Copyright 2019-2020 the original author or authors.
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

package org.springframework.kafka.support.serializer;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.common.serialization.BytesDeserializer;
import org.apache.kafka.common.serialization.BytesSerializer;
import org.apache.kafka.common.serialization.IntegerDeserializer;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.common.utils.Bytes;
import org.junit.jupiter.api.Test;

import org.springframework.kafka.support.DefaultKafkaHeaderMapper;
import org.springframework.messaging.MessageHeaders;

/**
 * @author Gary Russell
 * @since 2.3
 *
 */
public class DelegatingSerializationTests {

	@Test
	void testWithMapConfig() {
		DelegatingSerializer serializer = new DelegatingSerializer();
		Map<String, Object> configs = new HashMap<>();
		Map<String, Object> serializers = new HashMap<>();
		serializers.put("bytes", new BytesSerializer());
		serializers.put("int", IntegerSerializer.class);
		serializers.put("string", StringSerializer.class.getName());
		configs.put(DelegatingSerializer.SERIALIZATION_SELECTOR_CONFIG, serializers);
		serializer.configure(configs, false);
		DelegatingDeserializer deserializer = new DelegatingDeserializer();
		Map<String, Object> deserializers = new HashMap<>();
		deserializers.put("bytes", new BytesDeserializer());
		deserializers.put("int", IntegerDeserializer.class);
		deserializers.put("string", StringDeserializer.class.getName());
		configs.put(DelegatingSerializer.SERIALIZATION_SELECTOR_CONFIG, deserializers);
		deserializer.configure(configs, false);
		doTest(serializer, deserializer);
	}

	@Test
	void testWithPropertyConfig() {
		DelegatingSerializer serializer = new DelegatingSerializer();
		Map<String, Object> configs = new HashMap<>();
		configs.put(DelegatingSerializer.SERIALIZATION_SELECTOR_CONFIG, "bytes:" + BytesSerializer.class.getName()
				+ ", int:" + IntegerSerializer.class.getName() + ", string: " + StringSerializer.class.getName());
		serializer.configure(configs, false);
		DelegatingDeserializer deserializer = new DelegatingDeserializer();
		configs.put(DelegatingSerializer.SERIALIZATION_SELECTOR_CONFIG, "bytes:" + BytesDeserializer.class.getName()
				+ ", int:" + IntegerDeserializer.class.getName() + ", string: " + StringDeserializer.class.getName());
		deserializer.configure(configs, false);
		doTest(serializer, deserializer);
	}

	private void doTest(DelegatingSerializer serializer, DelegatingDeserializer deserializer) {
		Headers headers = new RecordHeaders();
		headers.add(new RecordHeader(DelegatingSerializer.SERIALIZATION_SELECTOR, "bytes".getBytes()));
		byte[] bytes = new byte[] { 1, 2, 3, 4 };
		byte[] serialized = serializer.serialize("foo", headers, new Bytes(bytes));
		assertThat(serialized).isSameAs(bytes);
		headers.add(new RecordHeader(DelegatingSerializer.SERIALIZATION_SELECTOR, "int".getBytes()));
		serialized = serializer.serialize("foo", headers, 42);
		assertThat(serialized).isEqualTo(new byte[] { 0, 0, 0, 42 });
		assertThat(deserializer.deserialize("foo", headers, serialized)).isEqualTo(42);
		headers.add(new RecordHeader(DelegatingSerializer.SERIALIZATION_SELECTOR, "string".getBytes()));
		serialized = serializer.serialize("foo", headers, "bar");
		assertThat(serialized).isEqualTo(new byte[] { 'b', 'a', 'r' });
		assertThat(deserializer.deserialize("foo", headers, serialized)).isEqualTo("bar");

		// implicit Serdes
		headers.remove(DelegatingSerializer.SERIALIZATION_SELECTOR);
		DelegatingSerializer spySe = spy(serializer);
		serialized = spySe.serialize("foo", headers, 42L);
		serialized = spySe.serialize("foo", headers, 42L);
		verify(spySe, times(1)).trySerdes(42L);
		assertThat(headers.lastHeader(DelegatingSerializer.SERIALIZATION_SELECTOR).value())
				.isEqualTo(Long.class.getName().getBytes());
		DelegatingDeserializer spyDe = spy(deserializer);
		assertThat(spyDe.deserialize("foo", headers, serialized)).isEqualTo(42L);
		spyDe.deserialize("foo", headers, serialized);
		verify(spyDe, times(1)).trySerdes(Long.class.getName());

		// The DKHM will jsonize the value; test that we ignore the quotes
		MessageHeaders messageHeaders = new MessageHeaders(
				Collections.singletonMap(DelegatingSerializer.SERIALIZATION_SELECTOR, "string"));
		new DefaultKafkaHeaderMapper().fromHeaders(messageHeaders, headers);
		assertThat(headers.lastHeader(DelegatingSerializer.SERIALIZATION_SELECTOR).value())
				.isEqualTo(new byte[] { 's', 't', 'r', 'i', 'n', 'g' });
		serialized = serializer.serialize("foo", headers, "bar");
		assertThat(serialized).isEqualTo(new byte[] { 'b', 'a', 'r' });
		assertThat(deserializer.deserialize("foo", headers, serialized)).isEqualTo("bar");

	}

	@Test
	void testBadIncomingOnlyOnce() {
		DelegatingDeserializer spy = spy(new DelegatingDeserializer());
		Headers headers = new RecordHeaders();
		headers.add(new RecordHeader(DelegatingSerializer.SERIALIZATION_SELECTOR, "junk".getBytes()));
		byte[] data = "foo".getBytes();
		assertThat(spy.deserialize("foo", headers, data)).isSameAs(data);
		spy.deserialize("foo", headers, data);
		verify(spy, times(1)).trySerdes("junk");
	}

}
