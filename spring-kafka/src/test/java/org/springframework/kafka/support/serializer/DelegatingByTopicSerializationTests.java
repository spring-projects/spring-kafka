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

package org.springframework.kafka.support.serializer;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.BytesDeserializer;
import org.apache.kafka.common.serialization.BytesSerializer;
import org.apache.kafka.common.serialization.IntegerDeserializer;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * @author Gary Russell
 * @author Wang Zhiyang
 *
 * @since 2.8
 *
 */
public class DelegatingByTopicSerializationTests {

	@Test
	void testWithMapConfig() {
		DelegatingByTopicSerializer serializer = new DelegatingByTopicSerializer();
		Map<String, Object> configs = new HashMap<>();
		Map<String, Object> serializers = new HashMap<>();
		serializers.put("fo.*", new BytesSerializer());
		serializers.put("bar", IntegerSerializer.class);
		serializers.put("baz", StringSerializer.class);
		configs.put(DelegatingByTopicSerializer.VALUE_SERIALIZATION_TOPIC_CONFIG, serializers);
		configs.put(DelegatingByTopicSerializer.CASE_SENSITIVE, "false");
		configs.put(DelegatingByTopicSerializer.VALUE_SERIALIZATION_TOPIC_DEFAULT, ByteArraySerializer.class);
		serializer.configure(configs, false);
		assertThatSerializer(serializer);
		assertThat(serializer.findDelegate("Foo")).isInstanceOf(BytesSerializer.class);
		DelegatingByTopicDeserializer deserializer = new DelegatingByTopicDeserializer();
		Map<String, Object> deserializers = new HashMap<>();
		deserializers.put("fo.*", new BytesDeserializer());
		deserializers.put("bar", IntegerDeserializer.class);
		deserializers.put("baz", StringDeserializer.class);
		configs.put(DelegatingByTopicDeserializer.VALUE_SERIALIZATION_TOPIC_CONFIG, deserializers);
		configs.put(DelegatingByTopicDeserializer.CASE_SENSITIVE, false);
		configs.put(DelegatingByTopicDeserializer.VALUE_SERIALIZATION_TOPIC_DEFAULT, ByteArrayDeserializer.class);
		deserializer.configure(configs, false);
		assertThatDeserializer(deserializer);
		assertThat(deserializer.findDelegate("Foo")).isInstanceOf(BytesDeserializer.class);
		byte[] serialized = serializer.serialize("baz", null, "qux");
		assertThat(deserializer.deserialize("baz", null, serialized)).isEqualTo("qux");
		assertThat(deserializer.deserialize("baz", null, ByteBuffer.wrap(serialized))).isEqualTo("qux");
	}

	@Test
	void testWithPropertyConfig() {
		DelegatingByTopicSerializer serializer = new DelegatingByTopicSerializer();
		Map<String, Object> configs = new HashMap<>();
		configs.put(DelegatingByTopicSerializer.VALUE_SERIALIZATION_TOPIC_CONFIG, "fo*:" + BytesSerializer.class.getName()
				+ ", bar:" + IntegerSerializer.class.getName() + ", baz: " + StringSerializer.class.getName());
		configs.put(DelegatingByTopicSerializer.VALUE_SERIALIZATION_TOPIC_DEFAULT, ByteArraySerializer.class);
		serializer.configure(configs, false);
		assertThatSerializer(serializer);
		DelegatingByTopicDeserializer deserializer = new DelegatingByTopicDeserializer();
		configs.put(DelegatingByTopicDeserializer.VALUE_SERIALIZATION_TOPIC_CONFIG, "fo*:" + BytesDeserializer.class.getName()
				+ ", bar:" + IntegerDeserializer.class.getName() + ", baz: " + StringDeserializer.class.getName());
		configs.put(DelegatingByTopicSerializer.VALUE_SERIALIZATION_TOPIC_DEFAULT, ByteArrayDeserializer.class);
		deserializer.configure(configs, false);
		assertThatDeserializer(deserializer);
	}

	@Test
	void testWithMapConfigKeys() {
		DelegatingByTopicSerializer serializer = new DelegatingByTopicSerializer();
		Map<String, Object> configs = new HashMap<>();
		Map<String, Object> serializers = new HashMap<>();
		serializers.put("fo.*", new BytesSerializer());
		serializers.put("bar", IntegerSerializer.class);
		serializers.put("baz", StringSerializer.class);
		configs.put(DelegatingByTopicSerializer.KEY_SERIALIZATION_TOPIC_CONFIG, serializers);
		configs.put(DelegatingByTopicSerializer.KEY_SERIALIZATION_TOPIC_DEFAULT, ByteArraySerializer.class);
		serializer.configure(configs, true);
		assertThatSerializer(serializer);
		DelegatingByTopicDeserializer deserializer = new DelegatingByTopicDeserializer();
		Map<String, Object> deserializers = new HashMap<>();
		deserializers.put("fo.*", new BytesDeserializer());
		deserializers.put("bar", IntegerDeserializer.class);
		deserializers.put("baz", StringDeserializer.class);
		configs.put(DelegatingByTopicSerializer.KEY_SERIALIZATION_TOPIC_CONFIG, deserializers);
		configs.put(DelegatingByTopicSerializer.KEY_SERIALIZATION_TOPIC_DEFAULT, ByteArrayDeserializer.class);
		deserializer.configure(configs, true);
		assertThatDeserializer(deserializer);
		byte[] serialized = serializer.serialize("baz", null, "qux");
		assertThat(deserializer.deserialize("baz", null, serialized)).isEqualTo("qux");
		assertThat(deserializer.deserialize("baz", null, ByteBuffer.wrap(serialized))).isEqualTo("qux");
	}

	@Test
	void testWithPropertyConfigKeys() {
		DelegatingByTopicSerializer serializer = new DelegatingByTopicSerializer();
		Map<String, Object> configs = new HashMap<>();
		configs.put(DelegatingByTopicSerializer.KEY_SERIALIZATION_TOPIC_CONFIG, "fo*:" + BytesSerializer.class.getName()
				+ ", bar:" + IntegerSerializer.class.getName() + ", baz: " + StringSerializer.class.getName());
		configs.put(DelegatingByTopicSerializer.KEY_SERIALIZATION_TOPIC_DEFAULT, ByteArraySerializer.class);
		serializer.configure(configs, true);
		assertThatSerializer(serializer);
		DelegatingByTopicDeserializer deserializer = new DelegatingByTopicDeserializer();
		configs.put(DelegatingByTopicSerializer.KEY_SERIALIZATION_TOPIC_CONFIG, "fo*:" + BytesDeserializer.class.getName()
				+ ", bar:" + IntegerDeserializer.class.getName() + ", baz: " + StringDeserializer.class.getName());
		configs.put(DelegatingByTopicSerializer.KEY_SERIALIZATION_TOPIC_DEFAULT, ByteArrayDeserializer.class);
		deserializer.configure(configs, true);
		assertThatDeserializer(deserializer);
	}

	private void assertThatSerializer(DelegatingByTopicSerializer serializer) {
		assertThat(serializer.findDelegate("foo")).isInstanceOf(BytesSerializer.class);
		assertThat(serializer.findDelegate("bar")).isInstanceOf(IntegerSerializer.class);
		assertThat(serializer.findDelegate("baz")).isInstanceOf(StringSerializer.class);
		assertThat(serializer.findDelegate("defaultTopic")).isInstanceOf(ByteArraySerializer.class);
	}

	private void assertThatDeserializer(DelegatingByTopicDeserializer deserializer) {
		assertThat(deserializer.findDelegate("foo")).isInstanceOf(BytesDeserializer.class);
		assertThat(deserializer.findDelegate("bar")).isInstanceOf(IntegerDeserializer.class);
		assertThat(deserializer.findDelegate("baz")).isInstanceOf(StringDeserializer.class);
		assertThat(deserializer.findDelegate("defaultTopic")).isInstanceOf(ByteArrayDeserializer.class);
	}

}
