/*
 * Copyright 2025-present the original author or authors.
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
