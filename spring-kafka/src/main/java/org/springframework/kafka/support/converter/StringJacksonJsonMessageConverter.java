/*
 * Copyright 2016-present the original author or authors.
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
