/*
 * Copyright 2025-present the original author or authors.
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
