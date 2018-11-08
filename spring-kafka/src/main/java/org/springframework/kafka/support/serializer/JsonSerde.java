/*
 * Copyright 2017-2018 the original author or authors.
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

import java.util.Map;

import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;

import org.springframework.core.ResolvableType;
import org.springframework.lang.Nullable;
import org.springframework.util.Assert;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.MapperFeature;
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
 *
 * @since 1.1.5
 */
public class JsonSerde<T> implements Serde<T> {

	private final JsonSerializer<T> jsonSerializer;

	private final JsonDeserializer<T> jsonDeserializer;

	public JsonSerde() {
		this((ObjectMapper) null);
	}

	public JsonSerde(Class<T> targetType) {
		this(targetType, null);
	}

	public JsonSerde(ObjectMapper objectMapper) {
		this(null, objectMapper);
	}

	@SuppressWarnings("unchecked")
	public JsonSerde(@Nullable Class<T> targetTypeArg, @Nullable ObjectMapper objectMapperArg) {
		ObjectMapper objectMapper = objectMapperArg;
		Class<T> targetType = targetTypeArg;
		if (objectMapper == null) {
			objectMapper = new ObjectMapper();
			objectMapper.configure(MapperFeature.DEFAULT_VIEW_INCLUSION, false);
			objectMapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
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

	/**
	 * Configure the TypeMapper to use key types if the JsonSerde is used to serialize keys.
	 * @param isKey Use key type headers if true
	 * @return the JsonSerde
	 * @since 2.1.3
	 */
	public JsonSerde<T> setUseTypeMapperForKey(boolean isKey) {
		this.jsonSerializer.setUseTypeMapperForKey(isKey);
		this.jsonDeserializer.setUseTypeMapperForKey(isKey);
		return this;
	}

}
