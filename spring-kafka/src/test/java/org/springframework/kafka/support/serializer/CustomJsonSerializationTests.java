/*
 * Copyright 2016-2021 the original author or authors.
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

import java.io.IOException;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

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
import org.springframework.kafka.support.serializer.testentities.DummyEntityWithTimestamp;
import org.springframework.kafka.test.utils.KafkaTestUtils;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JavaType;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.type.TypeFactory;

/**
 * @author Igor Stepanov
 * @author Artem Bilan
 * @author Yanming Zhou
 * @author Torsten Schleede
 * @author Gary Russell
 * @author Ivan Ponomarev
 * @author Carl Nygard
 */
public class CustomJsonSerializationTests {

	private StringSerializer stringWriter;

	private StringDeserializer stringReader;

	private CustomJsonSerializer<Object> jsonWriter;

	private CustomJsonDeserializer<DummyEntityWithTimestamp> jsonReader;

	private CustomJsonDeserializer<DummyEntityWithTimestamp[]> jsonArrayReader;

	private CustomJsonDeserializer<DummyEntityWithTimestamp> dummyEntityJsonDeserializer;

	private CustomJsonDeserializer<DummyEntityWithTimestamp[]> dummyEntityArrayJsonDeserializer;

	private DummyEntityWithTimestamp entity;

	private DummyEntityWithTimestamp[] entityArray;

	private String topic;

	private String timestamp;


	@BeforeEach
	void init() {
		timestamp = "2021-04-10T19:00:30.123456-04:00";
		entity = new DummyEntityWithTimestamp();
		entity.intValue = 19;
		entity.longValue = 7L;
		entity.stringValue = "dummy";
		entity.timestampValue = ZonedDateTime.parse(timestamp, DateTimeFormatter.ISO_OFFSET_DATE_TIME);
		List<String> list = Arrays.asList("dummy1", "dummy2");
		entity.complexStruct = new HashMap<>();
		entity.complexStruct.put((short) 4, list);
		entityArray = new DummyEntityWithTimestamp[] { entity };

		topic = "topic-name";

		jsonReader = new CustomJsonDeserializer<DummyEntityWithTimestamp>() { };
		jsonReader.configure(new HashMap<>(), false);
		jsonReader.close(); // does nothing, so may be called any time, or not called at all
		jsonArrayReader = new CustomJsonDeserializer<DummyEntityWithTimestamp[]>() { };
		jsonArrayReader.configure(new HashMap<>(), false);
		jsonArrayReader.close(); // does nothing, so may be called any time, or not called at all
		jsonWriter = new CustomJsonSerializer<>();
		jsonWriter.configure(new HashMap<>(), false);
		jsonWriter.close(); // does nothing, so may be called any time, or not called at all
		stringReader = new StringDeserializer();
		stringReader.configure(new HashMap<>(), false);
		stringWriter = new StringSerializer();
		stringWriter.configure(new HashMap<>(), false);
		dummyEntityJsonDeserializer = new DummyEntityWithTimestampJsonSerializer();
		dummyEntityArrayJsonDeserializer = new DummyEntityWithTimestampArrayJsonDeserializer();
	}

	/*
	 * 1. Serialize test entity to byte array.
	 * 2. Deserialize it back from the created byte array.
	 * 3. Check the result with the source entity.
	 */
	@Test
	void testDeserializeSerializedEntityEquals() {
		assertThat(jsonReader.deserialize(topic, jsonWriter.serialize(topic, entity))).isEqualTo(entity);
		Headers headers = new RecordHeaders();
		headers.add(AbstractJavaTypeMapper.DEFAULT_CLASSID_FIELD_NAME, DummyEntityWithTimestamp.class.getName().getBytes());
		assertThat(dummyEntityJsonDeserializer.deserialize(topic, headers, jsonWriter.serialize(topic, entity))).isEqualTo(entity);
	}

	/*
	 * 1. Serialize test entity array to byte array.
	 * 2. Deserialize it back from the created byte array.
	 * 3. Check the result with the source entity array.
	 */
	@Test
	void testDeserializeSerializedEntityArrayEquals() {
		assertThat(jsonArrayReader.deserialize(topic, jsonWriter.serialize(topic, entityArray))).isEqualTo(entityArray);
		Headers headers = new RecordHeaders();
		headers.add(AbstractJavaTypeMapper.DEFAULT_CLASSID_FIELD_NAME, DummyEntityWithTimestamp[].class.getName().getBytes());
		assertThat(dummyEntityArrayJsonDeserializer.deserialize(topic, headers, jsonWriter.serialize(topic, entityArray))).isEqualTo(entityArray);
	}

	/*
	 * 1. Serialize "dummy" String to byte array.
	 * 2. Deserialize it back from the created byte array.
	 * 3. Fails with SerializationException.
	 */
	@Test
	void testDeserializeSerializedDummyException() {
		assertThatExceptionOfType(SerializationException.class)
				.isThrownBy(() -> jsonReader.deserialize(topic, stringWriter.serialize(topic, "dummy")))
				.withMessageStartingWith("Can't deserialize data [")
				.withCauseInstanceOf(JsonParseException.class);

		Headers headers = new RecordHeaders();
		headers.add(AbstractJavaTypeMapper.DEFAULT_CLASSID_FIELD_NAME, "com.malware.DummyEntityWithTimestamp".getBytes());
		assertThatIllegalArgumentException()
				.isThrownBy(() -> dummyEntityJsonDeserializer
						.deserialize(topic, headers, jsonWriter.serialize(topic, entity)))
				.withMessageContaining("not in the trusted packages");
	}

	@Test
	void testSerializedStringNullEqualsNull() {
		assertThat(stringWriter.serialize(topic, null)).isEqualTo(null);
	}

	@Test
	void testSerializedJsonNullEqualsNull() {
		assertThat(jsonWriter.serialize(topic, null)).isEqualTo(null);
	}

	@Test
	void testDeserializedStringNullEqualsNull() {
		assertThat(stringReader.deserialize(topic, null)).isEqualTo(null);
	}

	@Test
	void testDeserializedJsonNullEqualsNull() {
		assertThat(jsonReader.deserialize(topic, null)).isEqualTo(null);
	}

	@Test
	void testExtraFieldIgnored() {
		JsonDeserializer<DummyEntityWithTimestamp> deser = new CustomJsonDeserializer<>(DummyEntityWithTimestamp.class);
		assertThat(deser.deserialize(topic, "{\"intValue\":1,\"extra\":2}".getBytes()))
				.isInstanceOf(DummyEntityWithTimestamp.class);
		deser.close();
	}

	@Test
	void testDeserTypeHeadersConfig() {
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
	void testDeserializerTypeInference() {
		JsonSerializer<List<String>> ser = new CustomJsonSerializer<>();
		JsonDeserializer<List<String>> de = new CustomJsonDeserializer<>(List.class);
		List<String> dummy = Arrays.asList("foo", "bar", "baz");
		assertThat(de.deserialize(topic, ser.serialize(topic, dummy))).isEqualTo(dummy);
		ser.close();
		de.close();
	}

	@Test
	void testDeserializerTypeReference() {
		JsonSerializer<List<DummyEntityWithTimestamp>> ser = new CustomJsonSerializer<>();
		JsonDeserializer<List<DummyEntityWithTimestamp>> de = new CustomJsonDeserializer<>(new TypeReference<List<DummyEntityWithTimestamp>>() { });
		List<DummyEntityWithTimestamp> dummy = Arrays.asList(this.entityArray);
		assertThat(de.deserialize(this.topic, ser.serialize(this.topic, dummy))).isEqualTo(dummy);
		ser.close();
		de.close();
	}

	@Test
	void testDeserializerTypeForcedType() {
		JsonSerializer<List<Parent>> ser = new CustomJsonSerializer<>(new TypeReference<List<Parent>>() { });
		JsonDeserializer<List<Parent>> de = new CustomJsonDeserializer<>(new TypeReference<List<Parent>>() { });
		List<Parent> dummy = Arrays.asList(new Child(1), new Parent(2));
		assertThat(de.deserialize(this.topic, ser.serialize(this.topic, dummy))).isEqualTo(dummy);
		ser.close();
		de.close();
	}

	@Test
	void jsonNode() throws IOException {
		JsonSerializer<Object> ser = new CustomJsonSerializer<>();
		JsonDeserializer<JsonNode> de = new CustomJsonDeserializer<>();
		de.configure(Collections.singletonMap(JsonDeserializer.VALUE_DEFAULT_TYPE, JsonNode.class), false);
		DummyEntityWithTimestamp dummy = new DummyEntityWithTimestamp();
		byte[] serialized = ser.serialize("foo", dummy);
		JsonNode node = new ObjectMapper().reader().readTree(serialized);
		Headers headers = new RecordHeaders();
		serialized = ser.serialize("foo", headers, node);
		de.deserialize("foo", headers, serialized);
	}

	@Test
	void testPreExistingHeaders() {
		JsonSerializer<? super Foo> ser = new CustomJsonSerializer<>();
		Headers headers = new RecordHeaders();
		ser.serialize("", headers, new Foo());
		byte[] data = ser.serialize("", headers, new Bar());
		JsonDeserializer<? super Foo> deser = new CustomJsonDeserializer<>();
		deser.setRemoveTypeHeaders(false);
		deser.addTrustedPackages(this.getClass().getPackage().getName());
		assertThat(deser.deserialize("", headers, data)).isInstanceOf(Bar.class);
		assertThat(headers.headers(AbstractJavaTypeMapper.DEFAULT_CLASSID_FIELD_NAME)).hasSize(1);
		ser.close();
		deser.close();
	}

	@Test
	void testDontUseTypeHeaders() {
		JsonSerializer<? super Foo> ser = new CustomJsonSerializer<>();
		Headers headers = new RecordHeaders();
		byte[] data = ser.serialize("", headers, new Bar());
		JsonDeserializer<? super Foo> deser = new CustomJsonDeserializer<>(Foo.class);
		deser.setRemoveTypeHeaders(false);
		deser.setUseTypeHeaders(false);
		deser.addTrustedPackages(this.getClass().getPackage().getName());
		assertThat(deser.deserialize("", headers, data)).isExactlyInstanceOf(Foo.class);
		assertThat(headers.headers(AbstractJavaTypeMapper.DEFAULT_CLASSID_FIELD_NAME)).hasSize(1);
		ser.close();
		deser.close();
	}

	@SuppressWarnings("unchecked")
	@Test
	void testParseTrustedPackages() {
		JsonDeserializer<Object> deser = new CustomJsonDeserializer<>();
		Map<String, Object> props = Collections.singletonMap(JsonDeserializer.TRUSTED_PACKAGES, "foo, bar, \tbaz");
		deser.configure(props, false);
		assertThat(KafkaTestUtils.getPropertyValue(deser, "typeMapper.trustedPackages", Set.class))
				.contains("foo", "bar", "baz");
	}

	@SuppressWarnings("unchecked")
	@Test
	void testTrustMappingPackages() {
		JsonDeserializer<Object> deser = new CustomJsonDeserializer<>();
		Map<String, Object> props = Collections.singletonMap(JsonDeserializer.TYPE_MAPPINGS,
				"foo:" + Foo.class.getName());
		deser.configure(props, false);
		assertThat(KafkaTestUtils.getPropertyValue(deser, "typeMapper.trustedPackages", Set.class))
				.contains(Foo.class.getPackageName());
		assertThat(KafkaTestUtils.getPropertyValue(deser, "typeMapper.trustedPackages", Set.class))
			.contains(Foo.class.getPackageName() + ".*");
	}

	@SuppressWarnings("unchecked")
	@Test
	void testTrustMappingPackagesMapper() {
		JsonDeserializer<Object> deser = new CustomJsonDeserializer<>();
		DefaultJackson2JavaTypeMapper mapper = new DefaultJackson2JavaTypeMapper();
		mapper.setIdClassMapping(Collections.singletonMap("foo", Foo.class));
		deser.setTypeMapper(mapper);
		assertThat(KafkaTestUtils.getPropertyValue(deser, "typeMapper.trustedPackages", Set.class))
				.contains(Foo.class.getPackageName());
		assertThat(KafkaTestUtils.getPropertyValue(deser, "typeMapper.trustedPackages", Set.class))
			.contains(Foo.class.getPackageName() + ".*");
	}

	@Test
	void testTypeFunctionViaProperties() {
		JsonDeserializer<Object> deser = new CustomJsonDeserializer<>();
		Map<String, Object> props = new HashMap<>();
		props.put(JsonDeserializer.KEY_TYPE_METHOD, getClass().getName() + ".stringType");
		props.put(JsonDeserializer.VALUE_TYPE_METHOD, getClass().getName() + ".fooBarJavaType");
		props.put(JsonDeserializer.TRUSTED_PACKAGES, "*");
		deser.configure(props, false);
		assertThat(deser.deserialize("", "{\"foo\":\"bar\"}".getBytes())).isInstanceOf(Foo.class);
		assertThat(deser.deserialize("", new RecordHeaders(), "{\"bar\":\"baz\"}".getBytes()))
				.isInstanceOf(Bar.class);

		deser.configure(props, true);
		assertThat(deser.deserialize("", new RecordHeaders(), "\"foo\"".getBytes()))
				.isEqualTo("foo");
		deser.close();
	}

	@Test
	void testTypeResolverViaProperties() {
		JsonDeserializer<Object> deser = new CustomJsonDeserializer<>();
		Map<String, Object> props = new HashMap<>();
		props.put(JsonDeserializer.KEY_TYPE_METHOD, getClass().getName() + ".stringTypeForTopic");
		props.put(JsonDeserializer.VALUE_TYPE_METHOD, getClass().getName() + ".fooBarJavaTypeForTopic");
		props.put(JsonDeserializer.TRUSTED_PACKAGES, "*");
		deser.configure(props, false);
		assertThat(deser.deserialize("", "{\"foo\":\"bar\"}".getBytes())).isInstanceOf(Foo.class);
		assertThat(deser.deserialize("", new RecordHeaders(), "{\"bar\":\"baz\"}".getBytes()))
				.isInstanceOf(Bar.class);

		deser.configure(props, true);
		assertThat(deser.deserialize("", new RecordHeaders(), "\"foo\"".getBytes()))
				.isEqualTo("foo");
		deser.close();
	}

	@Test
	void testTypeFunctionDirect() {
		JsonDeserializer<Object> deser = new CustomJsonDeserializer<>()
				.trustedPackages("*")
				.typeFunction(CustomJsonSerializationTests::fooBarJavaType);
		assertThat(deser.deserialize("", "{\"foo\":\"bar\"}".getBytes())).isInstanceOf(Foo.class);
		assertThat(deser.deserialize("", new RecordHeaders(), "{\"bar\":\"baz\"}".getBytes()))
				.isInstanceOf(Bar.class);
		deser.close();
	}

	@Test
	void testTypeResolverDirect() {
		JsonDeserializer<Object> deser = new CustomJsonDeserializer<>()
				.trustedPackages("*")
				.typeResolver(CustomJsonSerializationTests::fooBarJavaTypeForTopic);
		assertThat(deser.deserialize("", "{\"foo\":\"bar\"}".getBytes())).isInstanceOf(Foo.class);
		assertThat(deser.deserialize("", new RecordHeaders(), "{\"bar\":\"baz\"}".getBytes()))
				.isInstanceOf(Bar.class);
		deser.close();
	}

	@Test
	void testCopyWithType() {
		JsonDeserializer<Object> deser = new CustomJsonDeserializer<>();
		JsonSerializer<Object> ser = new CustomJsonSerializer<>();
		JsonDeserializer<Parent> typedDeser = deser.copyWithType(Parent.class);
		JsonSerializer<Parent> typedSer = ser.copyWithType(Parent.class);
		Child serializedValue = new Child(1);
		assertThat(typedDeser.deserialize("", typedSer.serialize("", serializedValue))).isEqualTo(serializedValue);
		deser.close();
		ser.close();
		typedDeser.close();
		typedSer.close();
	}

	public static JavaType fooBarJavaType(byte[] data, Headers headers) {
		if (data[0] == '{' && data[1] == 'f') {
			return TypeFactory.defaultInstance().constructType(Foo.class);
		}
		else {
			return TypeFactory.defaultInstance().constructType(Bar.class);
		}
	}

	public static JavaType fooBarJavaTypeForTopic(String topic, byte[] data, Headers headers) {
		if (data[0] == '{' && data[1] == 'f') {
			return TypeFactory.defaultInstance().constructType(Foo.class);
		}
		else {
			return TypeFactory.defaultInstance().constructType(Bar.class);
		}
	}

	public static JavaType stringType(byte[] data, Headers headers) {
		return TypeFactory.defaultInstance().constructType(String.class);
	}

	public static JavaType stringTypeForTopic(String topic, byte[] data, Headers headers) {
		return TypeFactory.defaultInstance().constructType(String.class);
	}

	static class DummyEntityWithTimestampJsonSerializer extends CustomJsonDeserializer<DummyEntityWithTimestamp> {

	}

	static class DummyEntityWithTimestampArrayJsonDeserializer extends CustomJsonDeserializer<DummyEntityWithTimestamp[]> {

	}

	public static class Foo {

		public String foo = "foo";

	}

	public static class Bar extends Foo {

		public String bar = "bar";

	}

	@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, include = JsonTypeInfo.As.PROPERTY, property = "type")
	@JsonSubTypes({
		@JsonSubTypes.Type(value = Parent.class, name = "parent"),
		@JsonSubTypes.Type(value = Child.class, name = "child")
	})
	public static class Parent {
		@JsonProperty
		private int number;

		Parent() { }

		Parent(int number) {
			this.number = number;
		}

		@Override
		public boolean equals(Object o) {
			if (o == null || !getClass().equals(o.getClass())) {
				return false;
			}
			Parent parent = (Parent) o;
			return number == parent.number;
		}

		@Override
		public int hashCode() {
			return Objects.hash(number);
		}
	}

	public static class Child extends Parent {
		Child() { }

		Child(int number) {
			super(number);
		}
	}


}
