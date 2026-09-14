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

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.Map;
import java.util.function.Consumer;
import java.util.function.Supplier;

import org.jspecify.annotations.Nullable;
import tools.jackson.databind.DeserializationFeature;
import tools.jackson.databind.MapperFeature;
import tools.jackson.databind.ObjectMapper;
import tools.jackson.databind.SerializationFeature;
import tools.jackson.databind.json.JsonMapper;

import org.springframework.util.Assert;
import org.springframework.util.ClassUtils;

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

	/**
	 * Resolve a {@link JsonMapper} from the configuration map based on key-specific or common properties.
	 * Checks properties in order:
	 * 1. key/value-specific mapper property
	 * 2. key/value-specific method property
	 * 3. common mapper property
	 * 4. common method property
	 *
	 * @param configs the configuration map.
	 * @param isKey whether configuring for a key or value.
	 * @param keyMapperProperty property name for key mapper (e.g. spring.json.key.mapper).
	 * @param valueMapperProperty property name for value mapper (e.g. spring.json.value.mapper).
	 * @param defaultMapperProperty common property name for mapper (e.g. spring.json.mapper).
	 * @param keyMethodProperty property name for key mapper method (e.g. spring.json.key.mapper.method).
	 * @param valueMethodProperty property name for value mapper method (e.g. spring.json.value.mapper.method).
	 * @param defaultMethodProperty common property name for mapper method (e.g. spring.json.mapper.method).
	 * @param classLoader the class loader to use.
	 * @return the resolved {@link JsonMapper}, or {@code null} if no mapper configuration was specified.
	 * @since 4.2
	 */
	public static @Nullable JsonMapper resolveJsonMapper(Map<String, ?> configs, boolean isKey,
			@Nullable String keyMapperProperty, @Nullable String valueMapperProperty,
			@Nullable String defaultMapperProperty,
			@Nullable String keyMethodProperty, @Nullable String valueMethodProperty,
			@Nullable String defaultMethodProperty,
			@Nullable ClassLoader classLoader) {

		ClassLoader cl = classLoader != null ? classLoader : ClassUtils.getDefaultClassLoader();
		String specificMapperKey = isKey ? keyMapperProperty : valueMapperProperty;
		String specificMethodKey = isKey ? keyMethodProperty : valueMethodProperty;

		if (specificMapperKey != null && configs.containsKey(specificMapperKey)) {
			return resolveMapperValue(configs.get(specificMapperKey), configs, isKey, cl);
		}
		if (specificMethodKey != null && configs.containsKey(specificMethodKey)) {
			return invokeStaticMapperMethod(configs.get(specificMethodKey), configs, isKey, cl);
		}
		if (defaultMapperProperty != null && configs.containsKey(defaultMapperProperty)) {
			return resolveMapperValue(configs.get(defaultMapperProperty), configs, isKey, cl);
		}
		if (defaultMethodProperty != null && configs.containsKey(defaultMethodProperty)) {
			return invokeStaticMapperMethod(configs.get(defaultMethodProperty), configs, isKey, cl);
		}
		return null;
	}

	@SuppressWarnings("unchecked")
	private static JsonMapper resolveMapperValue(Object value, Map<String, ?> configs, boolean isKey,
			@Nullable ClassLoader classLoader) {

		if (value instanceof JsonMapper jsonMapper) {
			return jsonMapper;
		}
		if (value instanceof JsonMapper.Builder builder) {
			return builder.build();
		}
		if (value instanceof Supplier<?> supplier) {
			return convertToMapper(supplier.get());
		}
		if (value instanceof Consumer<?> consumer) {
			JsonMapper.Builder builder = enhancedJsonMapper().rebuild();
			((Consumer<JsonMapper.Builder>) consumer).accept(builder);
			return builder.build();
		}
		if (value instanceof Class<?> clazz) {
			return instantiateMapperClass(clazz);
		}
		if (value instanceof String str) {
			if (ClassUtils.isPresent(str, classLoader)) {
				try {
					Class<?> clazz = ClassUtils.forName(str, classLoader);
					return instantiateMapperClass(clazz);
				}
				catch (ClassNotFoundException | LinkageError e) {
					throw new IllegalStateException("Failed to load mapper class: " + str, e);
				}
			}
			if (str.lastIndexOf('.') > 1) {
				return invokeStaticMapperMethod(str, configs, isKey, classLoader);
			}
			throw new IllegalStateException("Cannot resolve '" + str + "' as a class or method for JsonMapper");
		}
		throw new IllegalStateException("Unsupported type for JsonMapper configuration: " + value.getClass());
	}

	@SuppressWarnings("unchecked")
	private static JsonMapper instantiateMapperClass(Class<?> clazz) {
		try {
			if (Supplier.class.isAssignableFrom(clazz)) {
				Supplier<?> supplier = (Supplier<?>) clazz.getDeclaredConstructor().newInstance();
				return convertToMapper(supplier.get());
			}
			if (Consumer.class.isAssignableFrom(clazz)) {
				Consumer<JsonMapper.Builder> customizer =
						(Consumer<JsonMapper.Builder>) clazz.getDeclaredConstructor().newInstance();
				JsonMapper.Builder builder = enhancedJsonMapper().rebuild();
				customizer.accept(builder);
				return builder.build();
			}
			if (JsonMapper.class.isAssignableFrom(clazz)) {
				return (JsonMapper) clazz.getDeclaredConstructor().newInstance();
			}
			throw new IllegalStateException("Class " + clazz.getName()
					+ " must implement Supplier<JsonMapper>, Consumer<JsonMapper.Builder>, or extend JsonMapper");
		}
		catch (Exception e) {
			throw new IllegalStateException("Failed to instantiate mapper class: " + clazz.getName(), e);
		}
	}

	private static JsonMapper invokeStaticMapperMethod(Object methodProp, Map<String, ?> configs, boolean isKey,
			@Nullable ClassLoader classLoader) {

		Assert.isInstanceOf(String.class, methodProp, "'method' configuration must be a String");
		String methodProperty = (String) methodProp;
		int lastDotPosn = methodProperty.lastIndexOf('.');
		Assert.state(lastDotPosn > 1,
				"The method property needs to be a class name followed by the method name, separated by '.'");
		Class<?> clazz;
		try {
			clazz = ClassUtils.forName(methodProperty.substring(0, lastDotPosn), classLoader);
		}
		catch (ClassNotFoundException | LinkageError e) {
			throw new IllegalStateException(e);
		}
		String methodName = methodProperty.substring(lastDotPosn + 1);
		Method method;
		Object[] args;
		try {
			method = clazz.getDeclaredMethod(methodName, Map.class, boolean.class);
			args = new Object[] { configs, isKey };
		}
		catch (NoSuchMethodException e1) {
			try {
				method = clazz.getDeclaredMethod(methodName, Map.class);
				args = new Object[] { configs };
			}
			catch (NoSuchMethodException e2) {
				try {
					method = clazz.getDeclaredMethod(methodName);
					args = new Object[0];
				}
				catch (NoSuchMethodException e3) {
					IllegalStateException ise = new IllegalStateException(
							"The mapper method must take '(Map, boolean)', '(Map)', or '()'", e3);
					ise.addSuppressed(e1);
					ise.addSuppressed(e2);
					throw ise;
				}
			}
		}
		Assert.state(Modifier.isStatic(method.getModifiers()), method + " must be static");
		try {
			Object result = method.invoke(null, args);
			return convertToMapper(result);
		}
		catch (IllegalAccessException | IllegalArgumentException | InvocationTargetException e) {
			throw new IllegalStateException("Failed to invoke mapper method: " + method, e);
		}
	}

	private static JsonMapper convertToMapper(@Nullable Object result) {
		Assert.state(result != null, "Mapper provider returned null");
		if (result instanceof JsonMapper jsonMapper) {
			return jsonMapper;
		}
		if (result instanceof JsonMapper.Builder builder) {
			return builder.build();
		}
		if (result instanceof Supplier<?> supplier) {
			return convertToMapper(supplier.get());
		}
		throw new IllegalStateException("Mapper provider must return JsonMapper or JsonMapper.Builder, but was: "
				+ result.getClass().getName());
	}

	private JacksonMapperUtils() {
	}

}
