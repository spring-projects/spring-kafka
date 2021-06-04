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

package org.springframework.kafka.support.converter;

import org.springframework.messaging.Message;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * JSON Message converter - String on output, String, Bytes, or byte[] on input. Used in
 * conjunction with Kafka
 * {@code StringSerializer/(StringDeserializer, BytesDeserializer, or ByteArrayDeserializer)}.
 * Consider using the ByteArrayJsonMessageConverter instead to avoid unnecessary
 * {@code String->byte[]} conversion.
 *
 * @author Gary Russell
 * @author Artem Bilan
 * @author Dariusz Szablinski
 */
public class StringJsonMessageConverter extends JsonMessageConverter {

	public StringJsonMessageConverter() {
	}

	public StringJsonMessageConverter(ObjectMapper objectMapper) {
		super(objectMapper);
	}

	@Override
	protected Object convertPayload(Message<?> message) {
		try {
			return getObjectMapper()
					.writeValueAsString(message.getPayload());
		}
		catch (JsonProcessingException e) {
			throw new ConversionException("Failed to convert to JSON", message, e);
		}
	}

}
