/*
 * Copyright 2025-present the original author or authors.
 */

package org.springframework.kafka.support;

import java.io.Serial;

import tools.jackson.core.JsonGenerator;
import tools.jackson.databind.SerializationContext;
import tools.jackson.databind.ValueSerializer;
import tools.jackson.databind.module.SimpleModule;

import org.springframework.util.MimeType;

/**
 * A {@link SimpleModule} extension for {@link MimeType} serialization.
 *
 * @author Artem Bilan
 * @author Soby Chacko
 *
 * @since 4.0
 */
public final class MimeTypeJacksonModule extends SimpleModule {

	@Serial
	private static final long serialVersionUID = 1L;

	public MimeTypeJacksonModule() {
		addSerializer(MimeType.class, new MimeTypeSerializer());
	}

	/**
	 * Simple {@link ValueSerializer} extension to represent a {@link MimeType} object in the
	 * target JSON as a plain string.
	 */
	private static final class MimeTypeSerializer extends ValueSerializer<MimeType> {

		MimeTypeSerializer() {
		}

		@Override
		public void serialize(MimeType value, JsonGenerator generator, SerializationContext serializers) {
			generator.writeString(value.toString());
		}

	}

}
