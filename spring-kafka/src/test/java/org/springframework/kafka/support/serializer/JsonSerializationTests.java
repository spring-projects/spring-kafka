/*
 * Copyright 2016-2019 the original author or authors.
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
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;
import static org.assertj.core.api.Assertions.assertThatIllegalArgumentException;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;

import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import org.springframework.kafka.support.converter.AbstractJavaTypeMapper;
import org.springframework.kafka.support.converter.DefaultJackson2JavaTypeMapper;
import org.springframework.kafka.support.converter.Jackson2JavaTypeMapper.TypePrecedence;
import org.springframework.kafka.support.serializer.testentities.DummyEntity;
import org.springframework.kafka.test.utils.KafkaTestUtils;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.core.type.TypeReference;

/**
 * @author Igor Stepanov
 * @author Artem Bilan
 * @author Yanming Zhou
 * @author Torsten Schleede
 * @author Gary Russell
 * @author Ivan Ponomarev
 */
public class JsonSerializationTests {

	private StringSerializer stringWriter;

	private StringDeserializer stringReader;

	private JsonSerializer<Object> jsonWriter;

	private JsonDeserializer<DummyEntity> jsonReader;

	private JsonDeserializer<DummyEntity[]> jsonArrayReader;

	private JsonDeserializer<DummyEntity> dummyEntityJsonDeserializer;

	private JsonDeserializer<DummyEntity[]> dummyEntityArrayJsonDeserializer;

	private DummyEntity entity;

	private DummyEntity[] entityArray;

	private String topic;

	@BeforeEach
	public void init() {
		entity = new DummyEntity();
		entity.intValue = 19;
		entity.longValue = 7L;
		entity.stringValue = "dummy";
		List<String> list = Arrays.asList("dummy1", "dummy2");
		entity.complexStruct = new HashMap<>();
		entity.complexStruct.put((short) 4, list);
		entityArray = new DummyEntity[] { entity };

		topic = "topic-name";

		jsonReader = new JsonDeserializer<DummyEntity>() { };
		jsonReader.configure(new HashMap<>(), false);
		jsonReader.close(); // does nothing, so may be called any time, or not called at all
		jsonArrayReader = new JsonDeserializer<DummyEntity[]>() { };
		jsonArrayReader.configure(new HashMap<>(), false);
		jsonArrayReader.close(); // does nothing, so may be called any time, or not called at all
		jsonWriter = new JsonSerializer<>();
		jsonWriter.configure(new HashMap<>(), false);
		jsonWriter.close(); // does nothing, so may be called any time, or not called at all
		stringReader = new StringDeserializer();
		stringReader.configure(new HashMap<>(), false);
		stringWriter = new StringSerializer();
		stringWriter.configure(new HashMap<>(), false);
		dummyEntityJsonDeserializer = new DummyEntityJsonDeserializer();
		dummyEntityArrayJsonDeserializer = new DummyEntityArrayJsonDeserializer();
	}

	/*
	 * 1. Serialize test entity to byte array.
	 * 2. Deserialize it back from the created byte array.
	 * 3. Check the result with the source entity.
	 */
	@Test
	public void testDeserializeSerializedEntityEquals() {
		assertThat(jsonReader.deserialize(topic, jsonWriter.serialize(topic, entity))).isEqualTo(entity);
		Headers headers = new RecordHeaders();
		headers.add(AbstractJavaTypeMapper.DEFAULT_CLASSID_FIELD_NAME, DummyEntity.class.getName().getBytes());
		assertThat(dummyEntityJsonDeserializer.deserialize(topic, headers, jsonWriter.serialize(topic, entity))).isEqualTo(entity);
	}

	/*
	 * 1. Serialize test entity array to byte array.
	 * 2. Deserialize it back from the created byte array.
	 * 3. Check the result with the source entity array.
	 */
	@Test
	public void testDeserializeSerializedEntityArrayEquals() {
		assertThat(jsonArrayReader.deserialize(topic, jsonWriter.serialize(topic, entityArray))).isEqualTo(entityArray);
		Headers headers = new RecordHeaders();
		headers.add(AbstractJavaTypeMapper.DEFAULT_CLASSID_FIELD_NAME, DummyEntity[].class.getName().getBytes());
		assertThat(dummyEntityArrayJsonDeserializer.deserialize(topic, headers, jsonWriter.serialize(topic, entityArray))).isEqualTo(entityArray);
	}

	/*
	 * 1. Serialize "dummy" String to byte array.
	 * 2. Deserialize it back from the created byte array.
	 * 3. Fails with SerializationException.
	 */
	@Test
	public void testDeserializeSerializedDummyException() {
		assertThatExceptionOfType(SerializationException.class)
				.isThrownBy(() -> jsonReader.deserialize(topic, stringWriter.serialize(topic, "dummy")))
				.withMessageStartingWith("Can't deserialize data [")
				.withCauseInstanceOf(JsonParseException.class);

		Headers headers = new RecordHeaders();
		headers.add(AbstractJavaTypeMapper.DEFAULT_CLASSID_FIELD_NAME, "com.malware.DummyEntity".getBytes());
		assertThatIllegalArgumentException()
				.isThrownBy(() -> dummyEntityJsonDeserializer
						.deserialize(topic, headers, jsonWriter.serialize(topic, entity)))
				.withMessageContaining("not in the trusted packages");
	}

	@Test
	public void testSerializedStringNullEqualsNull() {
		assertThat(stringWriter.serialize(topic, null)).isEqualTo(null);
	}

	@Test
	public void testSerializedJsonNullEqualsNull() {
		assertThat(jsonWriter.serialize(topic, null)).isEqualTo(null);
	}

	@Test
	public void testDeserializedStringNullEqualsNull() {
		assertThat(stringReader.deserialize(topic, null)).isEqualTo(null);
	}

	@Test
	public void testDeserializedJsonNullEqualsNull() {
		assertThat(jsonReader.deserialize(topic, null)).isEqualTo(null);
	}

	@Test
	public void testExtraFieldIgnored() {
		JsonDeserializer<DummyEntity> deser = new JsonDeserializer<>(DummyEntity.class);
		assertThat(deser.deserialize(topic, "{\"intValue\":1,\"extra\":2}".getBytes()))
				.isInstanceOf(DummyEntity.class);
		deser.close();
	}

	@Test
	public void testDeserTypeHeadersConfig() {
		this.jsonReader.configure(Collections.singletonMap(JsonDeserializer.USE_TYPE_INFO_HEADERS, false), false);
		assertThat(KafkaTestUtils.getPropertyValue(this.jsonReader, "typeMapper.typePrecedence"))
			.isEqualTo(TypePrecedence.INFERRED);
		this.jsonReader.configure(Collections.singletonMap(JsonDeserializer.USE_TYPE_INFO_HEADERS, true), false);
		assertThat(KafkaTestUtils.getPropertyValue(this.jsonReader, "typeMapper.typePrecedence"))
			.isEqualTo(TypePrecedence.TYPE_ID);
		this.jsonReader.configure(Collections.singletonMap(JsonDeserializer.USE_TYPE_INFO_HEADERS, false), false);
		assertThat(KafkaTestUtils.getPropertyValue(this.jsonReader, "typeMapper.typePrecedence"))
			.isEqualTo(TypePrecedence.INFERRED);
		this.jsonReader.setUseTypeHeaders(true);
		this.jsonReader.configure(Collections.emptyMap(), false);
		assertThat(KafkaTestUtils.getPropertyValue(this.jsonReader, "typeMapper.typePrecedence"))
			.isEqualTo(TypePrecedence.TYPE_ID);
		this.jsonReader.setTypeMapper(new DefaultJackson2JavaTypeMapper());
		this.jsonReader.configure(Collections.singletonMap(JsonDeserializer.USE_TYPE_INFO_HEADERS, true), false);
		assertThat(KafkaTestUtils.getPropertyValue(this.jsonReader, "typeMapper.typePrecedence"))
			.isEqualTo(TypePrecedence.INFERRED);
	}

	@Test
	public void testDeserializerTypeInference() {
		JsonSerializer<List<String>> ser = new JsonSerializer<>();
		JsonDeserializer<List<String>> de = new JsonDeserializer<>(List.class);
		List<String> dummy = Arrays.asList("foo", "bar", "baz");
		assertThat(de.deserialize(topic, ser.serialize(topic, dummy))).isEqualTo(dummy);
		ser.close();
		de.close();
	}

	@Test
	public void testDeserializerTypeReference() {
		JsonSerializer<List<DummyEntity>> ser = new JsonSerializer<>();
		JsonDeserializer<List<DummyEntity>> de = new JsonDeserializer<>(new TypeReference<List<DummyEntity>>() { });
		List<DummyEntity> dummy = Arrays.asList(this.entityArray);
		assertThat(de.deserialize(this.topic, ser.serialize(this.topic, dummy))).isEqualTo(dummy);
		ser.close();
		de.close();
	}

	@Test
	public void testPreExistingHeaders() {
		JsonSerializer<? super Foo> ser = new JsonSerializer<>();
		Headers headers = new RecordHeaders();
		ser.serialize("", headers, new Foo());
		byte[] data = ser.serialize("", headers, new Bar());
		JsonDeserializer<? super Foo> deser = new JsonDeserializer<>();
		deser.setRemoveTypeHeaders(false);
		deser.addTrustedPackages(this.getClass().getPackage().getName());
		assertThat(deser.deserialize("", headers, data)).isInstanceOf(Bar.class);
		assertThat(headers.headers(AbstractJavaTypeMapper.DEFAULT_CLASSID_FIELD_NAME)).hasSize(1);
		ser.close();
		deser.close();
	}

	@Test
	public void testDontUseTypeHeaders() {
		JsonSerializer<? super Foo> ser = new JsonSerializer<>();
		Headers headers = new RecordHeaders();
		byte[] data = ser.serialize("", headers, new Bar());
		JsonDeserializer<? super Foo> deser = new JsonDeserializer<>(Foo.class);
		deser.setRemoveTypeHeaders(false);
		deser.setUseTypeHeaders(false);
		deser.addTrustedPackages(this.getClass().getPackage().getName());
		assertThat(deser.deserialize("", headers, data)).isExactlyInstanceOf(Foo.class);
		assertThat(headers.headers(AbstractJavaTypeMapper.DEFAULT_CLASSID_FIELD_NAME)).hasSize(1);
		ser.close();
		deser.close();
	}

	static class DummyEntityJsonDeserializer extends JsonDeserializer<DummyEntity> {

	}

	static class DummyEntityArrayJsonDeserializer extends JsonDeserializer<DummyEntity[]> {

	}

	public static class Foo {

		public String foo = "foo";

	}

	public static class Bar extends Foo {

		public String bar = "bar";

	}

}
