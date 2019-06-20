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

import org.springframework.beans.BeanUtils;
import org.springframework.util.ClassUtils;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.MapperFeature;
import com.fasterxml.jackson.databind.Module;
import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * The utilities for Jackson {@link ObjectMapper} instances.
 *
 * @author Artem Bilan
 *
 * @since 2.3
 */
public final class JacksonUtils {

	private static final String UNUSED = "unused";

	/**
	 * Factory for {@link ObjectMapper} instances with registered well-known modules
	 * and disabled {@link MapperFeature#DEFAULT_VIEW_INCLUSION} and
	 * {@link DeserializationFeature#FAIL_ON_UNKNOWN_PROPERTIES} features.
	 * The {@link ClassUtils#getDefaultClassLoader()} is used for loading module classes.
	 * @return the {@link ObjectMapper} instance.
	 */
	public static ObjectMapper enhancedObjectMapper() {
		return enhancedObjectMapper(ClassUtils.getDefaultClassLoader());
	}

	/**
	 * Factory for {@link ObjectMapper} instances with registered well-known modules
	 * and disabled {@link MapperFeature#DEFAULT_VIEW_INCLUSION} and
	 * {@link DeserializationFeature#FAIL_ON_UNKNOWN_PROPERTIES} features.
	 * @param classLoader the {@link ClassLoader} for modules to register.
	 * @return the {@link ObjectMapper} instance.
	 */
	public static ObjectMapper enhancedObjectMapper(ClassLoader classLoader) {
		ObjectMapper objectMapper = new ObjectMapper();
		objectMapper.configure(MapperFeature.DEFAULT_VIEW_INCLUSION, false);
		objectMapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
		registerWellKnownModulesIfAvailable(objectMapper, classLoader);
		return objectMapper;
	}

	@SuppressWarnings("unchecked")
	private static void registerWellKnownModulesIfAvailable(ObjectMapper objectMapper, ClassLoader classLoader) {
		objectMapper.registerModule(new JacksonMimeTypeModule());
		try {
			Class<? extends Module> jdk8Module = (Class<? extends Module>)
					ClassUtils.forName("com.fasterxml.jackson.datatype.jdk8.Jdk8Module", classLoader);
			objectMapper.registerModule(BeanUtils.instantiateClass(jdk8Module));
		}
		catch (@SuppressWarnings(UNUSED) ClassNotFoundException ex) {
			// jackson-datatype-jdk8 not available
		}

		try {
			Class<? extends Module> javaTimeModule = (Class<? extends Module>)
					ClassUtils.forName("com.fasterxml.jackson.datatype.jsr310.JavaTimeModule", classLoader);
			objectMapper.registerModule(BeanUtils.instantiateClass(javaTimeModule));
		}
		catch (@SuppressWarnings(UNUSED) ClassNotFoundException ex) {
			// jackson-datatype-jsr310 not available
		}

		// Joda-Time present?
		if (ClassUtils.isPresent("org.joda.time.LocalDate", classLoader)) {
			try {
				Class<? extends Module> jodaModule = (Class<? extends Module>)
						ClassUtils.forName("com.fasterxml.jackson.datatype.joda.JodaModule", classLoader);
				objectMapper.registerModule(BeanUtils.instantiateClass(jodaModule));
			}
			catch (@SuppressWarnings(UNUSED) ClassNotFoundException ex) {
				// jackson-datatype-joda not available
			}
		}

		// Kotlin present?
		if (ClassUtils.isPresent("kotlin.Unit", classLoader)) {
			try {
				Class<? extends Module> kotlinModule = (Class<? extends Module>)
						ClassUtils.forName("com.fasterxml.jackson.module.kotlin.KotlinModule", classLoader);
				objectMapper.registerModule(BeanUtils.instantiateClass(kotlinModule));
			}
			catch (@SuppressWarnings(UNUSED) ClassNotFoundException ex) {
				//jackson-module-kotlin not available
			}
		}
	}

	private JacksonUtils() {
	}

}
