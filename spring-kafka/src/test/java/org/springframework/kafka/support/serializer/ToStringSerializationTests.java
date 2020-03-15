/*
 * Copyright 2016-2020 the original author or authors.
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

import java.nio.charset.StandardCharsets;
import java.util.Collections;

import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import org.springframework.kafka.support.serializer.testentities.DummyEntity;

public class ToStringSerializationTests {

	@Test
	@DisplayName("Test serialization of key with headers")
	public void testToStringSerializer_keySerializationWithHeaders() {

		/* Given */
		ToStringSerializer<TestEntity> serializer = new ToStringSerializer<>();
		serializer.configure(Collections.emptyMap(), true);
		serializer.setAddTypeInfo(true);

		Headers headers = new RecordHeaders();

		TestEntity entity = new TestEntity("toto", 123, true);

		/* When */
		byte[] serialized = serializer.serialize("my-topic", headers, entity);

		/* Then */
		assertThat(serialized)
				.isNotNull()
				.isNotEmpty();
		assertThat(new String(serialized))
				.isEqualTo("toto:123:true");
		assertThat(headers)
				.isNotEmpty()
				.hasSize(1);
		assertThat(headers.lastHeader(ToStringSerializer.KEY_TYPE))
				.hasFieldOrPropertyWithValue("key", ToStringSerializer.KEY_TYPE)
				.hasFieldOrPropertyWithValue("value", TestEntity.class.getName().getBytes());
	}

	@Test
	@DisplayName("Test serialization of key without headers")
	public void testToStringSerializer_keySerializationWithoutHeaders() {

		/* Given */
		ToStringSerializer<TestEntity> serializer = new ToStringSerializer<>();
		serializer.configure(Collections.emptyMap(), true);
		serializer.setAddTypeInfo(false);

		Headers headers = new RecordHeaders();

		TestEntity entity = new TestEntity("toto", 123, true);

		/* When */
		byte[] serialized = serializer.serialize("my-topic", headers, entity);

		/* Then */
		assertThat(serialized)
				.isNotNull()
				.isNotEmpty();
		assertThat(new String(serialized))
				.isEqualTo("toto:123:true");
		assertThat(headers)
				.isEmpty();
	}

	@Test
	@DisplayName("Test serialization of value with headers")
	public void testToStringSerializer_valueSerializationWithHeaders() {

		/* Given */
		ToStringSerializer<TestEntity> serializer = new ToStringSerializer<>();
		serializer.configure(Collections.emptyMap(), false);
		serializer.setAddTypeInfo(true);

		Headers headers = new RecordHeaders();

		TestEntity entity = new TestEntity("toto", 123, true);

		/* When */
		byte[] serialized = serializer.serialize("my-topic", headers, entity);

		/* Then */
		assertThat(serialized)
				.isNotNull()
				.isNotEmpty();
		assertThat(new String(serialized))
				.isEqualTo("toto:123:true");
		assertThat(headers)
				.isNotEmpty()
				.hasSize(1);
		assertThat(headers.lastHeader(ToStringSerializer.VALUE_TYPE))
				.hasFieldOrPropertyWithValue("key", ToStringSerializer.VALUE_TYPE)
				.hasFieldOrPropertyWithValue("value", TestEntity.class.getName().getBytes());
	}

	@Test
	@DisplayName("Test serialization of value without headers")
	public void testToStringSerializer_valueSerializationWithoutHeaders() {

		/* Given */
		ToStringSerializer<TestEntity> serializer = new ToStringSerializer<>();
		serializer.configure(Collections.emptyMap(), false);
		serializer.setAddTypeInfo(false);

		Headers headers = new RecordHeaders();

		TestEntity entity = new TestEntity("toto", 123, true);

		/* When */
		byte[] serialized = serializer.serialize("my-topic", headers, entity);

		/* Then */
		assertThat(serialized)
				.isNotNull()
				.isNotEmpty();
		assertThat(new String(serialized))
				.isEqualTo("toto:123:true");
		assertThat(headers)
				.isEmpty();
	}

	@Test
	@DisplayName("Test simple deserialization without headers")
	public void testSimpleDeserialization() {

		/* Given */
		ParseStringDeserializer<TestEntity> deserializer = new ParseStringDeserializer<>(TestEntity::parse);
		byte[] data = "toto:123:true".getBytes();

		/* When */
		TestEntity entity = deserializer.deserialize("my-topic", data);

		/* Then */
		assertThat(entity)
				.hasFieldOrPropertyWithValue("first", "toto")
				.hasFieldOrPropertyWithValue("second", 123)
				.hasFieldOrPropertyWithValue("third", true);
	}

	@Test
	@DisplayName("Test deserialization using headers")
	public void testSerialization_usingHeaders() {

		/* Given */
		ParseStringDeserializer<Object> deserializer = new ParseStringDeserializer<>((str, headers) -> {
			byte[] header = headers.lastHeader(ToStringSerializer.VALUE_TYPE).value();
			String entityType = new String(header);

			if (entityType.contains("TestEntity")) {
				return TestEntity.parse(str);
			}
			else if (entityType.contains("DummyEntity")) {
				DummyEntity dummyEntity = new DummyEntity();
				String[] tokens = str.split(":");
				dummyEntity.stringValue = tokens[0];
				dummyEntity.intValue = Integer.parseInt(tokens[1]);
				return dummyEntity;
			}

			return null;
		});

		byte[] data = "toto:123:true".getBytes();
		Headers headers = new RecordHeaders();
		headers.add(ToStringSerializer.VALUE_TYPE, DummyEntity.class.getName().getBytes());

		/* When */
		Object entity = deserializer.deserialize("my-topic", headers, data);

		/* Then */
		assertThat(entity)
				.isNotNull()
				.isInstanceOf(DummyEntity.class)
				.hasFieldOrPropertyWithValue("stringValue", "toto")
				.hasFieldOrPropertyWithValue("intValue", 123);
	}

	@Test
	@DisplayName("Test serialization/deserialization with provided charset")
	public void testSerializationDeserializationWithCharset() {

		/* Given */
		ToStringSerializer<TestEntity> serializer = new ToStringSerializer<>();
		serializer.setCharset(StandardCharsets.ISO_8859_1);

		TestEntity entity = new TestEntity("tôtô", 123, true);

		ParseStringDeserializer<TestEntity> deserializerLatin1 = new ParseStringDeserializer<>(TestEntity::parse);
		deserializerLatin1.setCharset(StandardCharsets.ISO_8859_1);

		ParseStringDeserializer<TestEntity> deserializerUtf8 = new ParseStringDeserializer<>(TestEntity::parse);

		/* When */
		byte[] serialized = serializer.serialize("my-topic", entity);

		TestEntity deserializedWithLatin1 = deserializerLatin1.deserialize("my-topic", serialized);
		TestEntity deserializedWithUtf8 = deserializerUtf8.deserialize("my-topic", serialized);

		/* Then */
		assertThat(deserializedWithLatin1)
				.hasFieldOrPropertyWithValue("first", "tôtô")
				.hasFieldOrPropertyWithValue("second", 123)
				.hasFieldOrPropertyWithValue("third", true);

		assertThat(deserializedWithUtf8)
				.hasFieldOrPropertyWithValue("second", 123)
				.hasFieldOrPropertyWithValue("third", true);
		assertThat(deserializedWithUtf8.first)
				.isNotEqualTo("tôtô");
	}

	private static final class TestEntity {
		String first;
		int second;
		boolean third;

		TestEntity(String first, int second, boolean third) {
			this.first = first;
			this.second = second;
			this.third = third;
		}

		@Override
		public String toString() {
			return String.join(":", first, Integer.toString(second), Boolean.toString(third));
		}

		public static TestEntity parse(String str) {
			String[] tokens = str.split(":");
			String first = tokens[0];
			int second = Integer.parseInt(tokens[1]);
			boolean third = Boolean.parseBoolean(tokens[2]);
			return new TestEntity(first, second, third);
		}
	}
}
