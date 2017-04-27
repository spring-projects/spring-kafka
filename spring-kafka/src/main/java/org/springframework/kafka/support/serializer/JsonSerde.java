/*
 * Copyright 2017 the original author or authors.
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
 * @since 2.0
 */
public class JsonSerde<T> implements Serde<T> {

	private final JsonSerializer<T> jsonSerializer;

	private final JsonDeserializer<T> jsonDeserializer;

	public JsonSerde() {
		this(null, null);
	}

	public JsonSerde(Class<T> targetType) {
		this(targetType, null);
	}

	public JsonSerde(ObjectMapper objectMapper) {
		this(null, objectMapper);
	}

	public JsonSerde(Class<T> targetType, ObjectMapper objectMapper) {
		this.jsonSerializer = objectMapper == null ? new JsonSerializer<>() : new JsonSerializer<>(objectMapper);
		if (objectMapper == null) {
			this.jsonDeserializer = targetType == null ? new JsonDeserializer<>() : new JsonDeserializer<>(targetType);
		}
		else {
			this.jsonDeserializer = targetType == null ? new JsonDeserializer<>(objectMapper)
					: new JsonDeserializer<>(targetType, objectMapper);
		}
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
}
