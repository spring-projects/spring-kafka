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

package org.springframework.kafka.support.serializer;

import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.serialization.Serializer;

import org.springframework.lang.Nullable;
import org.springframework.util.Assert;
import org.springframework.util.ClassUtils;
import org.springframework.util.StringUtils;

/**
 * A {@link Serializer} that delegates to other serializers based on a serialization
 * selector header.
 *
 * @author Gary Russell
 * @since 2.3
 *
 */
public class DelegatingSerializer implements Serializer<Object> {

	/**
	 * Name of the header containing the serialization selector.
	 */
	public static final String SERIALIZATION_SELECTOR = "spring.kafka.serialization.selector";

	/**
	 * Name of the configuration property containing the serialization selector map with
	 * format {@code selector:class,...}.
	 */
	public static final String SERIALIZATION_SELECTOR_CONFIG = "spring.kafka.serialization.selector.config";

	private final Map<String, Serializer<?>> delegates = new HashMap<>();

	/**
	 * Construct an instance that will be configured in {@link #configure(Map, boolean)}
	 * with a producer property
	 * {@link DelegatingSerializer#SERIALIZATION_SELECTOR_CONFIG}.
	 */
	public DelegatingSerializer() {
		super();
	}

	/**
	 * Construct an instance with the supplied mapping of selectors to delegate
	 * serializers. The selector must be supplied in the
	 * {@link DelegatingSerializer#SERIALIZATION_SELECTOR} header.
	 * @param delegates the map of delegates.
	 */
	public DelegatingSerializer(Map<String, Serializer<?>> delegates) {
		this.delegates.putAll(delegates);
	}

	@SuppressWarnings("unchecked")
	@Override
	public void configure(Map<String, ?> configs, boolean isKey) {
		Object value = configs.get(SERIALIZATION_SELECTOR_CONFIG);
		if (value == null) {
			return;
		}
		else if (value instanceof Map) {
			((Map<String, Object>) value).forEach((selector, serializer) -> {
				if (serializer instanceof Serializer) {
					this.delegates.put(selector, (Serializer<?>) serializer);
				}
				else if (serializer instanceof Class) {
					instantiateAndConfigure(configs, isKey, this.delegates, selector, (Class<?>) serializer);
				}
				else if (serializer instanceof String) {
					createInstanceAndConfigure(configs, isKey, this.delegates, selector, (String) serializer);
				}
				else {
					throw new IllegalStateException(SERIALIZATION_SELECTOR_CONFIG
							+ " map entries must be Serializers or class names, not " + value.getClass());
				}
			});
		}
		else if (value instanceof String) {
			this.delegates.putAll(createDelegates((String) value, configs, isKey));
		}
		else {
			throw new IllegalStateException(
					SERIALIZATION_SELECTOR_CONFIG + " must be a map or String, not " + value.getClass());
		}
	}

	protected static Map<String, Serializer<?>> createDelegates(String mappings, Map<String, ?> configs,
			boolean isKey) {

		Map<String, Serializer<?>> delegateMap = new HashMap<>();
		String[] array = StringUtils.commaDelimitedListToStringArray(mappings);
		for (String entry : array) {
			String[] split = entry.split(":");
			Assert.isTrue(split.length == 2, "Each comma-delimited selector entry must have exactly one ':'");
			createInstanceAndConfigure(configs, isKey, delegateMap, split[0], split[1]);
		}
		return delegateMap;
	}

	protected static void createInstanceAndConfigure(Map<String, ?> configs, boolean isKey,
			Map<String, Serializer<?>> delegateMap, String selector, String className) {

		try {
			Class<?> clazz = ClassUtils.forName(className.trim(), ClassUtils.getDefaultClassLoader());
			instantiateAndConfigure(configs, isKey, delegateMap, selector, clazz);
		}
		catch (ClassNotFoundException | LinkageError e) {
			throw new IllegalArgumentException(e);
		}
	}

	protected static void instantiateAndConfigure(Map<String, ?> configs, boolean isKey,
			Map<String, Serializer<?>> delegateMap, String selector, Class<?> clazz) {

		try {
			Serializer<?> delegate = (Serializer<?>) clazz.newInstance();
			delegate.configure(configs, isKey);
			delegateMap.put(selector.trim(), delegate);
		}
		catch (InstantiationException | IllegalAccessException e) {
			throw new IllegalArgumentException(e);
		}
	}

	public void addDelegate(String selector, Serializer<?> serializer) {
		this.delegates.put(selector, serializer);
	}

	@Nullable
	public Serializer<?> removeDelegate(String selector) {
		return this.delegates.remove(selector);
	}

	@Override
	public byte[] serialize(String topic, Object data) {
		throw new UnsupportedOperationException();
	}


	@Override
	public byte[] serialize(String topic, Headers headers, Object data) {
		byte[] value = headers.lastHeader(SERIALIZATION_SELECTOR).value();
		if (value == null) {
			throw new IllegalStateException("No '" + SERIALIZATION_SELECTOR + "' header present");
		}
		String selector = new String(value).replaceAll("\"", "");
		@SuppressWarnings("unchecked")
		Serializer<Object> serializer = (Serializer<Object>) this.delegates.get(selector);
		if (serializer == null) {
			throw new IllegalStateException(
					"No serializer found for '" + SERIALIZATION_SELECTOR + "' header with value '" + selector + "'");
		}
		return serializer.serialize(topic, headers, data);
	}

	@Override
	public void close() {
		this.delegates.values().forEach(ser -> ser.close());
	}

}
