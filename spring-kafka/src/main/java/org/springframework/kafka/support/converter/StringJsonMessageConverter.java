/*
 * Copyright 2016 the original author or authors.
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

package org.springframework.kafka.support.converter;

import java.io.IOException;
import java.lang.reflect.Type;

import org.apache.kafka.clients.consumer.ConsumerRecord;

import org.springframework.messaging.Message;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JavaType;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.type.TypeFactory;

/**
 * JSON Message converter - String on output, String or byte[] on input.
 *
 * @author Gary Russell
 *
 */
public class StringJsonMessageConverter extends MessagingMessageConverter {

	private final ObjectMapper objectMapper = new ObjectMapper();


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
		JavaType javaType = TypeFactory.defaultInstance().constructType(type);
		Object value = record.value();
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
