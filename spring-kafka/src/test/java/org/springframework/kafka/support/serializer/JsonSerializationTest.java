/*
 * Copyright 2015-2016 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.springframework.kafka.support.serializer;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;

import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.Before;
import org.junit.Test;

import org.springframework.kafka.support.serializer.testentities.DummyEntity;

/**
 * @author Igor Stepanov
 */
public class JsonSerializationTest {

	private StringSerializer stringWriter;
	private StringDeserializer stringReader;
	private DummyEntitySerializer jsonWriter;
	private DummyEntityDeserializer jsonReader;
	private DummyEntity entity;
	private String topic;

	@Before
	public void init() {
		entity = new DummyEntity();
		entity.intValue = 19;
		entity.longValue = 7L;
		entity.stringValue = "dummy";
		List<String> list = Arrays.asList(new String[] { "dummy1", "dummy2" });
		entity.complexStruct = new HashMap<>();
		entity.complexStruct.put((short) 4, list);

		topic = "topic-name";

		jsonReader = new DummyEntityDeserializer();
		jsonReader.configure(new HashMap<String, Object>(), false);
		jsonReader.close(); // does nothing, so may be called any time, or not called at all
		jsonWriter = new DummyEntitySerializer();
		jsonWriter.configure(new HashMap<String, Object>(), false);
		jsonWriter.close(); // does nothing, so may be called any time, or not called at all
		stringReader = new StringDeserializer();
		stringReader.configure(new HashMap<String, Object>(), false);
		stringWriter = new StringSerializer();
		stringWriter.configure(new HashMap<String, Object>(), false);
	}

	/*
	 * 1. Serialize test entity to byte array.
	 * 2. Deserialize it back from the created byte array.
	 * 3. Check the result with the source entity.
	 */
	@Test
	public void deserialize_serialized_entity_equals() {
		assertThat(jsonReader.deserialize(topic, jsonWriter.serialize(topic, entity))).isEqualTo(entity);
	}

	/*
	 * 1. Serialize "dummy" String to byte array.
	 * 2. Deserialize it back from the created byte array.
	 *    - this operation should fail as the source is not json
	 *    - but the exception is logged, not rethrown - this unblocks the consuming of another messages
	 * 3. In case of exception result is "null".
	 */
	@Test
	public void deserialize_serialized_dummy_equals_null() {
		assertThat(jsonReader.deserialize(topic, stringWriter.serialize(topic, "dummy"))).isEqualTo(null);
	}

	@Test
	public void serialized_string_null_equals_null() {
		assertThat(stringWriter.serialize(topic, null)).isEqualTo(null);
	}

	@Test
	public void serialized_json_null_equals_null() {
		assertThat(jsonWriter.serialize(topic, null)).isEqualTo(null);
	}

	@Test
	public void deserialized_string_null_equals_null() {
		assertThat(stringReader.deserialize(topic, null)).isEqualTo(null);
	}

	@Test
	public void deserialized_json_null_equals_null() {
		assertThat(jsonReader.deserialize(topic, null)).isEqualTo(null);
	}

	private class DummyEntitySerializer extends JsonSerializer<DummyEntity> {
	}

	private class DummyEntityDeserializer extends JsonDeserializer<DummyEntity> {
	}
}
