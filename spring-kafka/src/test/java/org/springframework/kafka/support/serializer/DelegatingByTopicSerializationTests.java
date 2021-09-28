/*
 * Copyright 2019-2021 the original author or authors.
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

import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.common.serialization.BytesDeserializer;
import org.apache.kafka.common.serialization.BytesSerializer;
import org.apache.kafka.common.serialization.IntegerDeserializer;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.Test;

/**
 * @author Gary Russell
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
		serializer.configure(configs, false);
		assertThat(serializer.findSerializer("foo")).isInstanceOf(BytesSerializer.class);
		assertThat(serializer.findSerializer("bar")).isInstanceOf(IntegerSerializer.class);
		assertThat(serializer.findSerializer("baz")).isInstanceOf(StringSerializer.class);
		assertThat(serializer.findSerializer("Foo")).isInstanceOf(BytesSerializer.class);
		DelegatingByTopicDeserializer deserializer = new DelegatingByTopicDeserializer();
		Map<String, Object> deserializers = new HashMap<>();
		deserializers.put("fo.*", new BytesDeserializer());
		deserializers.put("bar", IntegerDeserializer.class);
		deserializers.put("baz", StringDeserializer.class);
		configs.put(DelegatingByTopicSerializer.VALUE_SERIALIZATION_TOPIC_CONFIG, deserializers);
		configs.put(DelegatingByTopicSerializer.CASE_SENSITIVE, "false");
		deserializer.configure(configs, false);
		assertThat(deserializer.findDeserializer("foo")).isInstanceOf(BytesDeserializer.class);
		assertThat(deserializer.findDeserializer("bar")).isInstanceOf(IntegerDeserializer.class);
		assertThat(deserializer.findDeserializer("baz")).isInstanceOf(StringDeserializer.class);
		assertThat(deserializer.deserialize("baz", null, serializer.serialize("baz", null, "qux"))).isEqualTo("qux");
		assertThat(deserializer.findDeserializer("Foo")).isInstanceOf(BytesDeserializer.class);
	}

	@Test
	void testWithPropertyConfig() {
		DelegatingByTopicSerializer serializer = new DelegatingByTopicSerializer();
		Map<String, Object> configs = new HashMap<>();
		configs.put(DelegatingByTopicSerializer.VALUE_SERIALIZATION_TOPIC_CONFIG, "fo*:" + BytesSerializer.class.getName()
				+ ", bar:" + IntegerSerializer.class.getName() + ", baz: " + StringSerializer.class.getName());
		serializer.configure(configs, false);
		assertThat(serializer.findSerializer("foo")).isInstanceOf(BytesSerializer.class);
		DelegatingByTopicDeserializer deserializer = new DelegatingByTopicDeserializer();
		configs.put(DelegatingByTopicSerializer.VALUE_SERIALIZATION_TOPIC_CONFIG, "fo*:" + BytesDeserializer.class.getName()
				+ ", bar:" + IntegerDeserializer.class.getName() + ", baz: " + StringDeserializer.class.getName());
		deserializer.configure(configs, false);
		assertThat(deserializer.findDeserializer("foo")).isInstanceOf(BytesDeserializer.class);
		assertThat(deserializer.findDeserializer("bar")).isInstanceOf(IntegerDeserializer.class);
		assertThat(deserializer.findDeserializer("baz")).isInstanceOf(StringDeserializer.class);
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
		serializer.configure(configs, true);
		assertThat(serializer.findSerializer("foo")).isInstanceOf(BytesSerializer.class);
		assertThat(serializer.findSerializer("bar")).isInstanceOf(IntegerSerializer.class);
		assertThat(serializer.findSerializer("baz")).isInstanceOf(StringSerializer.class);
		DelegatingByTopicDeserializer deserializer = new DelegatingByTopicDeserializer();
		Map<String, Object> deserializers = new HashMap<>();
		deserializers.put("fo.*", new BytesDeserializer());
		deserializers.put("bar", IntegerDeserializer.class);
		deserializers.put("baz", StringDeserializer.class);
		configs.put(DelegatingByTopicSerializer.KEY_SERIALIZATION_TOPIC_CONFIG, deserializers);
		deserializer.configure(configs, true);
		assertThat(deserializer.findDeserializer("foo")).isInstanceOf(BytesDeserializer.class);
		assertThat(deserializer.findDeserializer("bar")).isInstanceOf(IntegerDeserializer.class);
		assertThat(deserializer.findDeserializer("baz")).isInstanceOf(StringDeserializer.class);
		assertThat(deserializer.deserialize("baz", null, serializer.serialize("baz", null, "qux"))).isEqualTo("qux");
	}

	@Test
	void testWithPropertyConfigKeys() {
		DelegatingByTopicSerializer serializer = new DelegatingByTopicSerializer();
		Map<String, Object> configs = new HashMap<>();
		configs.put(DelegatingByTopicSerializer.KEY_SERIALIZATION_TOPIC_CONFIG, "fo*:" + BytesSerializer.class.getName()
				+ ", bar:" + IntegerSerializer.class.getName() + ", baz: " + StringSerializer.class.getName());
		serializer.configure(configs, true);
		assertThat(serializer.findSerializer("foo")).isInstanceOf(BytesSerializer.class);
		DelegatingByTopicDeserializer deserializer = new DelegatingByTopicDeserializer();
		configs.put(DelegatingByTopicSerializer.KEY_SERIALIZATION_TOPIC_CONFIG, "fo*:" + BytesDeserializer.class.getName()
				+ ", bar:" + IntegerDeserializer.class.getName() + ", baz: " + StringDeserializer.class.getName());
		deserializer.configure(configs, true);
		assertThat(deserializer.findDeserializer("foo")).isInstanceOf(BytesDeserializer.class);
		assertThat(deserializer.findDeserializer("bar")).isInstanceOf(IntegerDeserializer.class);
		assertThat(deserializer.findDeserializer("baz")).isInstanceOf(StringDeserializer.class);
	}

}
