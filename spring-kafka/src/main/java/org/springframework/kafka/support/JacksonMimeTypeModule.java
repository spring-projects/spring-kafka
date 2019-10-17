/*
 * Copyright 2019 the original author or authors.
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

package org.springframework.kafka.support;

import java.io.IOException;

import org.springframework.util.MimeType;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.module.SimpleModule;

/**
 * A {@link SimpleModule} extension for {@link MimeType} serialization.
 *
 * @author Artem Bilan
 *
 * @since 2.3
 */
public final class JacksonMimeTypeModule extends SimpleModule {

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
			super();
		}

		@Override
		public void serialize(MimeType value, JsonGenerator generator, SerializerProvider serializers)
				throws IOException {

			generator.writeString(value.toString());
		}

	}

}
