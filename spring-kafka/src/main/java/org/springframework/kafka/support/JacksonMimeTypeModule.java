/*
 * Copyright 2019-present the original author or authors.
 */

package org.springframework.kafka.support;

import java.io.IOException;
import java.io.Serial;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.module.SimpleModule;

import org.springframework.util.MimeType;

/**
 * A {@link SimpleModule} extension for {@link MimeType} serialization.
 *
 * @author Artem Bilan
 *
 * @since 2.3
 *
 * @deprecated since 4.0 in favor of {@link MimeTypeJacksonModule}.
 */
@Deprecated(forRemoval = true, since = "4.0")
public final class JacksonMimeTypeModule extends SimpleModule {

	@Serial
	private static final long serialVersionUID = 1L;

	public JacksonMimeTypeModule() {
		addSerializer(MimeType.class, new MimeTypeSerializer());
	}

	/**
	 * Simple {@link JsonSerializer} extension to represent a {@link MimeType} object in the
	 * target JSON as a plain string.
	 */
	private static final class MimeTypeSerializer extends JsonSerializer<MimeType> {

		MimeTypeSerializer() {
		}

		@Override
		public void serialize(MimeType value, JsonGenerator generator, SerializerProvider serializers)
				throws IOException {

			generator.writeString(value.toString());
		}

	}

}
