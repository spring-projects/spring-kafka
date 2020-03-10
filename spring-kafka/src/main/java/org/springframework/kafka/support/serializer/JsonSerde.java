/*
 * Copyright 2017-2020 the original author or authors.
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

import java.util.Map;

import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;

import org.springframework.core.ResolvableType;
import org.springframework.kafka.support.JacksonUtils;
import org.springframework.kafka.support.converter.Jackson2JavaTypeMapper;
import org.springframework.lang.Nullable;
import org.springframework.util.Assert;

import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * A {@link org.apache.kafka.common.serialization.Serde} that provides serialization and
 * deserialization in JSON format.
 * <p>
 * The implementation delegates to underlying {@link JsonSerializer} and
 * {@link JsonDeserializer} implementations.
 *
 * @param <T> target class for serialization/deserialization
 *
 * @author Marius Bogoevici
 * @author Elliot Kennedy
 * @author Gary Russell
 * @author Ivan Ponomarev
 *
 * @since 1.1.5
 */
public class JsonSerde<T> implements Serde<T> {

	private final JsonSerializer<T> jsonSerializer;

	private final JsonDeserializer<T> jsonDeserializer;

	public JsonSerde() {
		this((ObjectMapper) null);
	}

	public JsonSerde(Class<? super T> targetType) {
		this(targetType, null);
	}

	public JsonSerde(ObjectMapper objectMapper) {
		this(null, objectMapper);
	}

	@SuppressWarnings("unchecked")
	public JsonSerde(@Nullable Class<? super T> targetTypeArg, @Nullable ObjectMapper objectMapperArg) {
		ObjectMapper objectMapper = objectMapperArg;
		Class<T> targetType = (Class<T>) targetTypeArg;
		if (objectMapper == null) {
			objectMapper = JacksonUtils.enhancedObjectMapper();
		}
		this.jsonSerializer = new JsonSerializer<>(objectMapper);
		if (targetType == null) {
			targetType = (Class<T>) ResolvableType.forClass(getClass()).getSuperType().resolveGeneric(0);
		}
		this.jsonDeserializer = new JsonDeserializer<>(targetType, objectMapper);
	}

	public JsonSerde(JsonSerializer<T> jsonSerializer, JsonDeserializer<T> jsonDeserializer) {
		Assert.notNull(jsonSerializer, "'jsonSerializer' must not be null.");
		Assert.notNull(jsonDeserializer, "'jsonDeserializer' must not be null.");
		this.jsonSerializer = jsonSerializer;
		this.jsonDeserializer = jsonDeserializer;
	}

	@Override
	public void configure(Map<String, ?> configs, boolean isKey) {
		this.jsonSerializer.configure(configs, isKey);
		this.jsonDeserializer.configure(configs, isKey);
	}

	@Override
	public void close() {
		this.jsonSerializer.close();
		this.jsonDeserializer.close();
	}

	@Override
	public Serializer<T> serializer() {
		return this.jsonSerializer;
	}

	@Override
	public Deserializer<T> deserializer() {
		return this.jsonDeserializer;
	}

	// Fluent API

	/**
	 * Designate this Serde for serializing/deserializing keys (default is values).
	 * @return the serde.
	 * @since 2.3
	 */
	public JsonSerde<T> forKeys() {
		this.jsonSerializer.forKeys();
		this.jsonDeserializer.forKeys();
		return this;
	}

	/**
	 * Configure the serializer to not add type information.
	 * @return the serde.
	 * @since 2.3
	 */
	public JsonSerde<T> noTypeInfo() {
		this.jsonSerializer.noTypeInfo();
		return this;
	}

	/**
	 * Don't remove type information headers after deserialization.
	 * @return the serde.
	 * @since 2.3
	 */
	public JsonSerde<T> dontRemoveTypeHeaders() {
		this.jsonDeserializer.dontRemoveTypeHeaders();
		return this;
	}

	/**
	 * Ignore type information headers and use the configured target class.
	 * @return the serde.
	 * @since 2.3
	 */
	public JsonSerde<T> ignoreTypeHeaders() {
		this.jsonDeserializer.ignoreTypeHeaders();
		return this;
	}

	/**
	 * Use the supplied {@link Jackson2JavaTypeMapper}.
	 * @param mapper the mapper.
	 * @return the serde.
	 * @since 2.3
	 */
	public JsonSerde<T> typeMapper(Jackson2JavaTypeMapper mapper) {
		this.jsonSerializer.setTypeMapper(mapper);
		this.jsonDeserializer.setTypeMapper(mapper);
		return this;
	}

}
