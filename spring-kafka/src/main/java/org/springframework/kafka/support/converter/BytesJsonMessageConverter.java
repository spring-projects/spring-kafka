/*
 * Copyright 2018 the original author or authors.
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

import org.apache.kafka.common.utils.Bytes;

import org.springframework.messaging.Message;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * JSON Message converter - {@code Bytes} on output, String, Bytes, or byte[] on input.
 * Used in conjunction with Kafka {@code BytesSerializer/BytesDeserializer}. More
 * efficient than {@link StringJsonMessageConverter} because the {@code String/byte[]} is
 * avoided.
 *
 * @author Gary Russell
 * @since 2.1.7
 *
 */
public class BytesJsonMessageConverter extends StringJsonMessageConverter {

	public BytesJsonMessageConverter() {
		super();
	}

	public BytesJsonMessageConverter(ObjectMapper objectMapper) {
		super(objectMapper);
	}

	@Override
	protected Object convertPayload(Message<?> message) {
		try {
			return Bytes.wrap(getObjectMapper().writeValueAsBytes(message.getPayload()));
		}
		catch (JsonProcessingException e) {
			throw new ConversionException("Failed to convert to JSON", e);
		}
	}

}
