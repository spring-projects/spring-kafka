/*
 * Copyright 2018-present the original author or authors.
 */

package org.springframework.kafka.support.converter;

import org.apache.kafka.common.utils.Bytes;
import org.jspecify.annotations.Nullable;
import tools.jackson.databind.json.JsonMapper;

import org.springframework.kafka.support.KafkaNull;
import org.springframework.messaging.Message;

/**
 * JSON Message converter that uses  - {@code Bytes} on output, String, Bytes, or byte[] on input.
 * Used in conjunction with Kafka
 * {@code BytesSerializer/(BytesDeserializer, ByteArrayDeserializer, or StringDeserializer)}.
 * More efficient than {@link StringJacksonJsonMessageConverter} because the
 * {@code String<->byte[]} conversion is avoided.
 *
 * @author Soby Chacko
 * @since 4.0
 */
public class BytesJacksonJsonMessageConverter extends JacksonJsonMessageConverter {

	public BytesJacksonJsonMessageConverter() {
	}

	public BytesJacksonJsonMessageConverter(JsonMapper jsonMapper) {
		super(jsonMapper);
	}

	@Override
	protected @Nullable Object convertPayload(Message<?> message) {
		try {
			return message.getPayload() instanceof KafkaNull
					? null
					: Bytes.wrap(getJsonMapper().writeValueAsBytes(message.getPayload()));
		}
		catch (Exception e) {
			throw new ConversionException("Failed to convert to JSON", message, e);
		}
	}

}
