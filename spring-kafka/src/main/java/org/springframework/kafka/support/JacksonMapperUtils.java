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

import tools.jackson.databind.DeserializationFeature;
import tools.jackson.databind.MapperFeature;
import tools.jackson.databind.ObjectMapper;
import tools.jackson.databind.SerializationFeature;
import tools.jackson.databind.json.JsonMapper;

/**
 * The utilities for Jackson {@link ObjectMapper} instances.
 *
 * @author Artem Bilan
 * @author Soby Chacko
 *
 * @since 4.0
 */
public final class JacksonMapperUtils {

	/**
	 * Factory for {@link ObjectMapper} instances with registered well-known modules
	 * and disabled {@link MapperFeature#DEFAULT_VIEW_INCLUSION} and
	 * {@link DeserializationFeature#FAIL_ON_UNKNOWN_PROPERTIES} features.
	 * @return the {@link JsonMapper} instance.
	 */
	public static JsonMapper enhancedJsonMapper() {
		return JsonMapper.builder()
				.findAndAddModules(JsonKafkaHeaderMapper.class.getClassLoader())
				.disable(tools.jackson.databind.DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES)
				.disable(tools.jackson.databind.MapperFeature.DEFAULT_VIEW_INCLUSION)
				.enable(SerializationFeature.FAIL_ON_EMPTY_BEANS)
				.addModule(new MimeTypeJacksonModule())
				.build();
	}

	private JacksonMapperUtils() {
	}

}
