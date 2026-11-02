/*
 * Copyright 2019-present the original author or authors.
 */

package org.springframework.kafka.support.converter;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.jspecify.annotations.Nullable;

import org.springframework.kafka.support.KafkaNull;
import org.springframework.messaging.Message;

/**
 * JSON Message converter - {@code byte[]} on output, String, Bytes, or byte[] on input.
 * Used in conjunction with Kafka
 * {@code ByteArraySerializer/(ByteArrayDeserializer, BytesDeserializer, or StringDeserializer)}.
 * More efficient than {@link StringJsonMessageConverter} because the
 * {@code String<->byte[]} conversion is avoided.
 *
 * @author Gary Russell
 * @author Vladimir Loginov
 * @author Ngoc Nhan
 * @since 2.3
 *
 * @deprecated since 4.0 in favor of {@link ByteArrayJacksonJsonMessageConverter} for Jackson 3.
 */
@Deprecated(forRemoval = true, since = "4.0")
@SuppressWarnings("removal")
public class ByteArrayJsonMessageConverter extends JsonMessageConverter {

	public ByteArrayJsonMessageConverter() {
	}

	public ByteArrayJsonMessageConverter(ObjectMapper objectMapper) {
		super(objectMapper);
	}

	@Override
	protected @Nullable Object convertPayload(Message<?> message) {
		try {
			return message.getPayload() instanceof KafkaNull
					? null
					: getObjectMapper().writeValueAsBytes(message.getPayload());
		}
		catch (JsonProcessingException e) {
			throw new ConversionException("Failed to convert to JSON", message, e);
		}
	}

}
