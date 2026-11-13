/*
 * Copyright 2019-present the original author or authors.
 */

package org.springframework.kafka.support.serializer;

import java.nio.charset.StandardCharsets;
import java.util.Map;

import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.junit.jupiter.api.Test;
import tools.jackson.databind.DeserializationFeature;
import tools.jackson.databind.SerializationFeature;
import tools.jackson.databind.json.JsonMapper;

import org.springframework.kafka.support.mapping.DefaultJacksonJavaTypeMapper;
import org.springframework.kafka.test.utils.KafkaTestUtils;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * @author Gary Russell
 * @since 2.3
 *
 */
public class JsonSerdeTests {

	@Test
	void serializationFeaturesAndCopy() {
		JsonMapper mapper = JsonMapper.builder().build();
		JacksonJsonSerializer<PrimitiveValue> serializer = new JacksonJsonSerializer<>(mapper);
		serializer.configure(Map.of(JacksonJsonSerializer.SERIALIZATION_FEATURES_ENABLE, " INDENT_OUTPUT "), false);
		assertThat(new String(serializer.serialize("topic", new PrimitiveValue(42)), StandardCharsets.UTF_8))
				.contains("\n");
		assertThat(new String(serializer.<PrimitiveValue>copyWithType(PrimitiveValue.class)
				.serialize("topic", new PrimitiveValue(42)), StandardCharsets.UTF_8)).contains("\n");
		assertThat(mapper.isEnabled(SerializationFeature.INDENT_OUTPUT)).isFalse();
		JacksonJsonSerializer<PrimitiveValue> compact = new JacksonJsonSerializer<>(
				JsonMapper.builder().enable(SerializationFeature.INDENT_OUTPUT).build());
		compact.configure(Map.of(JacksonJsonSerializer.SERIALIZATION_FEATURES_DISABLE, "INDENT_OUTPUT"), true);
		assertThat(new String(compact.serialize("topic", new PrimitiveValue(42)), StandardCharsets.UTF_8))
				.doesNotContain("\n");
	}

	@Test
	void deserializationFeaturesForDefaultHeadersResolverAndCopy() {
		JsonMapper mapper = JsonMapper.builder().enable(DeserializationFeature.FAIL_ON_NULL_FOR_PRIMITIVES).build();
		JacksonJsonDeserializer<PrimitiveValue> deserializer = new JacksonJsonDeserializer<>(PrimitiveValue.class, mapper);
		deserializer.configure(Map.of(JacksonJsonDeserializer.DESERIALIZATION_FEATURES_DISABLE,
				" FAIL_ON_NULL_FOR_PRIMITIVES, FAIL_ON_UNKNOWN_PROPERTIES "), false);
		byte[] bytes = "{\"value\":null,\"unknown\":1}".getBytes(StandardCharsets.UTF_8);
		assertThat(deserializer.deserialize("topic", bytes)).isEqualTo(new PrimitiveValue(0));
		JacksonJsonDeserializer<PrimitiveValue> copy = deserializer.copyWithType(PrimitiveValue.class);
		copy.configure(Map.of(), false);
		assertThat(copy.deserialize("topic", bytes))
				.isEqualTo(new PrimitiveValue(0));
		RecordHeaders headers = new RecordHeaders();
		new JacksonJsonSerializer<PrimitiveValue>().serialize("topic", headers, new PrimitiveValue(1));
		assertThat(deserializer.deserialize("topic", headers, bytes)).isEqualTo(new PrimitiveValue(0));
		deserializer.typeResolver((topic, data, recordHeaders) -> mapper.constructType(PrimitiveValue.class));
		assertThat(deserializer.deserialize("topic", bytes)).isEqualTo(new PrimitiveValue(0));
		assertThat(deserializer.deserialize("topic", new RecordHeaders(), bytes)).isEqualTo(new PrimitiveValue(0));
		assertThat(mapper.isEnabled(DeserializationFeature.FAIL_ON_NULL_FOR_PRIMITIVES)).isTrue();
	}

	@Test
	void enableDeserializationFeatureAndConfigureOnce() {
		JacksonJsonDeserializer<PrimitiveValue> deserializer = new JacksonJsonDeserializer<>(PrimitiveValue.class);
		deserializer.configure(Map.of(JacksonJsonDeserializer.DESERIALIZATION_FEATURES_ENABLE,
				"FAIL_ON_UNKNOWN_PROPERTIES"), true);
		deserializer.configure(Map.of(JacksonJsonDeserializer.DESERIALIZATION_FEATURES_DISABLE,
				"FAIL_ON_UNKNOWN_PROPERTIES"), true);
		assertThatThrownBy(() -> deserializer.deserialize("topic",
				"{\"value\":1,\"unknown\":1}".getBytes(StandardCharsets.UTF_8))).isInstanceOf(SerializationException.class);
	}

	@Test
	void featureConfigurationWithPropertySelectedTargetType() {
		for (boolean isKey : new boolean[] {false, true}) {
			JacksonJsonDeserializer<PrimitiveValue> deserializer = new JacksonJsonDeserializer<>();
			deserializer.configure(Map.of(
					isKey ? JacksonJsonDeserializer.KEY_DEFAULT_TYPE : JacksonJsonDeserializer.VALUE_DEFAULT_TYPE,
					PrimitiveValue.class,
					JacksonJsonDeserializer.DESERIALIZATION_FEATURES_DISABLE, "FAIL_ON_NULL_FOR_PRIMITIVES"), isKey);
			assertThat(deserializer.deserialize("topic", "{\"value\":null}".getBytes(StandardCharsets.UTF_8)))
					.isEqualTo(new PrimitiveValue(0));
		}
	}

	@Test
	void invalidFeatureConfiguration() {
		for (Object invalid : new Object[] {"UNKNOWN_FEATURE", "INDENT_OUTPUT,", true}) {
			assertThatThrownBy(() -> new JacksonJsonSerializer<>().configure(
					Map.of(JacksonJsonSerializer.SERIALIZATION_FEATURES_ENABLE, invalid), false))
					.isInstanceOf(IllegalArgumentException.class)
					.hasMessageContaining(JacksonJsonSerializer.SERIALIZATION_FEATURES_ENABLE);
		}
		assertThatThrownBy(() -> new JacksonJsonDeserializer<>().configure(
				Map.of(JacksonJsonDeserializer.DESERIALIZATION_FEATURES_ENABLE, "UNKNOWN_FEATURE"), false))
				.isInstanceOf(IllegalArgumentException.class);
		assertThatThrownBy(() -> new JacksonJsonDeserializer<>().configure(Map.of(
				JacksonJsonDeserializer.DESERIALIZATION_FEATURES_ENABLE, "FAIL_ON_UNKNOWN_PROPERTIES",
				JacksonJsonDeserializer.DESERIALIZATION_FEATURES_DISABLE, "FAIL_ON_UNKNOWN_PROPERTIES"), false))
				.isInstanceOf(IllegalArgumentException.class);
		assertThatThrownBy(() -> new JacksonJsonSerializer<>().noTypeInfo().configure(
				Map.of(JacksonJsonSerializer.SERIALIZATION_FEATURES_ENABLE, "INDENT_OUTPUT"), false))
				.isInstanceOf(IllegalStateException.class);
		assertThatThrownBy(() -> new JacksonJsonDeserializer<>().ignoreTypeHeaders().configure(
				Map.of(JacksonJsonDeserializer.DESERIALIZATION_FEATURES_DISABLE, "FAIL_ON_UNKNOWN_PROPERTIES"), false))
				.isInstanceOf(IllegalStateException.class);
	}

	@Test
	void noTypeInfo() {
		JacksonJsonSerde<String> serde = new JacksonJsonSerde<>(String.class)
				.forKeys()
				.noTypeInfo()
				.ignoreTypeHeaders()
				.dontRemoveTypeHeaders();
		assertThat(KafkaTestUtils.getPropertyValue(serde, "jsonSerializer.typeMapper.classIdFieldName"))
				.isEqualTo(DefaultJacksonJavaTypeMapper.KEY_DEFAULT_CLASSID_FIELD_NAME);
		assertThat(KafkaTestUtils.getPropertyValue(serde, "jsonDeserializer.typeMapper.classIdFieldName"))
				.isEqualTo(DefaultJacksonJavaTypeMapper.KEY_DEFAULT_CLASSID_FIELD_NAME);
		assertThat(KafkaTestUtils.getPropertyValue(serde, "jsonSerializer.addTypeInfo", Boolean.class)).isFalse();
		assertThat(KafkaTestUtils.getPropertyValue(serde, "jsonDeserializer.useTypeHeaders", Boolean.class)).isFalse();
		assertThat(KafkaTestUtils.getPropertyValue(serde, "jsonDeserializer.removeTypeHeaders", Boolean.class))
				.isFalse();
		serde.close();
	}

	record PrimitiveValue(int value) {
	}

}
