/*
 * Copyright 2015-2016 the original author or authors.
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

import java.io.IOException;
import java.util.Arrays;
import java.util.Map;

import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Deserializer;

import org.springframework.core.ResolvableType;
import org.springframework.util.Assert;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.MapperFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectReader;

/**
 * Generic {@link Deserializer} for receiving JSON from Kafka and return Java objects.
 *
 * @param <T> class of the entity, representing messages
 *
 * @author Igor Stepanov
 * @author Artem Bilan
 */
public class JsonDeserializer<T> implements Deserializer<T> {

	protected final ObjectMapper objectMapper;

	protected final Class<T> targetType;

	private volatile ObjectReader reader;

	protected JsonDeserializer() {
		this((Class<T>) null);
	}

	protected JsonDeserializer(ObjectMapper objectMapper) {
		this(null, objectMapper);
	}

	public JsonDeserializer(Class<T> targetType) {
		this(targetType, new ObjectMapper());
		this.objectMapper.configure(MapperFeature.DEFAULT_VIEW_INCLUSION, false);
		this.objectMapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
	}

	@SuppressWarnings("unchecked")
	public JsonDeserializer(Class<T> targetType, ObjectMapper objectMapper) {
		Assert.notNull(objectMapper, "'objectMapper' must not be null.");
		this.objectMapper = objectMapper;
		if (targetType == null) {
			targetType = (Class<T>) ResolvableType.forClass(getClass()).getSuperType().resolveGeneric(0);
		}
		Assert.notNull(targetType, "'targetType' cannot be resolved.");
		this.targetType = targetType;
	}

	public void configure(Map<String, ?> configs, boolean isKey) {
		// No-op
	}

	public T deserialize(String topic, byte[] data) {
		if (this.reader == null) {
			this.reader = this.objectMapper.readerFor(this.targetType);
		}
		try {
			T result = null;
			if (data != null) {
				result = this.reader.readValue(data);
			}
			return result;
		}
		catch (IOException e) {
			throw new SerializationException("Can't deserialize data [" + Arrays.toString(data) +
					"] from topic [" + topic + "]", e);
		}
	}

	public void close() {
		// No-op
	}

}
