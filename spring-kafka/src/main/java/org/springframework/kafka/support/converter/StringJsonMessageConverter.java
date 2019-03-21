/*
 * Copyright 2016 the original author or authors.
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

package org.springframework.kafka.support.converter;

import java.io.IOException;
import java.lang.reflect.Type;

import org.apache.kafka.clients.consumer.ConsumerRecord;

import org.springframework.kafka.support.KafkaNull;
import org.springframework.messaging.Message;
import org.springframework.util.Assert;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JavaType;
import com.fasterxml.jackson.databind.MapperFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.type.TypeFactory;

/**
 * JSON Message converter - String on output, String or byte[] on input.
 *
 * @author Gary Russell
 * @author Artem Bilan
 * @author Dariusz Szablinski
 */
public class StringJsonMessageConverter extends MessagingMessageConverter {

	private final ObjectMapper objectMapper;

	public StringJsonMessageConverter() {
		this(new ObjectMapper());
		this.objectMapper.configure(MapperFeature.DEFAULT_VIEW_INCLUSION, false);
		this.objectMapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
	}

	public StringJsonMessageConverter(ObjectMapper objectMapper) {
		Assert.notNull(objectMapper, "'objectMapper' must not be null.");
		this.objectMapper = objectMapper;
	}

	@Override
	protected Object convertPayload(Message<?> message) {
		try {
			return this.objectMapper.writeValueAsString(message.getPayload());
		}
		catch (JsonProcessingException e) {
			throw new ConversionException("Failed to convert to JSON", e);
		}
	}


	@Override
	protected Object extractAndConvertValue(ConsumerRecord<?, ?> record, Type type) {
		Object value = record.value();
		if (record.value() == null) {
			return KafkaNull.INSTANCE;
		}

		JavaType javaType = TypeFactory.defaultInstance().constructType(type);
		if (value instanceof String) {
			try {
				return this.objectMapper.readValue((String) value, javaType);
			}
			catch (IOException e) {
				throw new ConversionException("Failed to convert from JSON", e);
			}
		}
		else if (value instanceof byte[]) {
			try {
				return this.objectMapper.readValue((byte[]) value, javaType);
			}
			catch (IOException e) {
				throw new ConversionException("Failed to convert from JSON", e);
			}
		}
		else {
			throw new IllegalStateException("Only String or byte[] supported");
		}
	}

}
