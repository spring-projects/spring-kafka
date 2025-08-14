/*
 * Copyright 2016-present the original author or authors.
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

import org.jspecify.annotations.Nullable;
import tools.jackson.databind.json.JsonMapper;

import org.springframework.kafka.support.KafkaNull;
import org.springframework.messaging.Message;

/**
 * JSON Message converter based on Jackson 3 - String on output, String, Bytes, or byte[] on input. Used in
 * conjunction with Kafka
 * {@code StringSerializer/(StringDeserializer, BytesDeserializer, or ByteArrayDeserializer)}.
 * Consider using the ByteArrayJsonMessageConverter instead to avoid unnecessary
 * {@code String->byte[]} conversion.
 *
 * @author Soby Chacko
 * @since 4.0
 */
public class StringJacksonJsonMessageConverter extends JacksonJsonMessageConverter {

	public StringJacksonJsonMessageConverter() {
	}

	public StringJacksonJsonMessageConverter(JsonMapper objectMapper) {
		super(objectMapper);
	}

	@Override
	protected @Nullable Object convertPayload(Message<?> message) {
		try {
			return message.getPayload() instanceof KafkaNull
					? null
					: getJsonMapper().writeValueAsString(message.getPayload());
		}
		catch (Exception e) {
			throw new ConversionException("Failed to convert to JSON", message, e);
		}
	}
}
