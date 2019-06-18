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
import org.apache.kafka.common.serialization.Deserializer;

import org.springframework.lang.Nullable;
import org.springframework.util.Assert;
import org.springframework.util.ClassUtils;
import org.springframework.util.StringUtils;

/**
 * A {@link Deserializer} that delegates to other deserializers based on a serialization
 * selector header.
 *
 * @author Gary Russell
 * @since 2.3
 *
 */
public class DelegatingDeserializer implements Deserializer<Object> {

	/**
	 * Name of the configuration property containing the serialization selector map with
	 * format {@code selector:class,...}.
	 */
	public static final String SERIALIZATION_SELECTOR_CONFIG = DelegatingSerializer.SERIALIZATION_SELECTOR_CONFIG;


	private final Map<String, Deserializer<?>> delegates = new HashMap<>();

	/**
	 * Construct an instance that will be configured in {@link #configure(Map, boolean)}
	 * with a consumer property
	 * {@link SERIALIZATION_SELECTOR_CONFIG}.
	 */
	public DelegatingDeserializer() {
		super();
	}

	/**
	 * Construct an instance with the supplied mapping of selectors to delegate
	 * deserializers. The selector must be supplied in the
	 * {@link DelegatingSerializer#SERIALIZATION_SELECTOR} header.
	 * @param delegates the map of delegates.
	 */
	public DelegatingDeserializer(Map<String, Deserializer<?>> delegates) {
		this.delegates.putAll(delegates);
	}

	@SuppressWarnings("unchecked")
	@Override
	public void configure(Map<String, ?> configs, boolean isKey) {
		Object value = configs.get(SERIALIZATION_SELECTOR_CONFIG);
		if (value == null) {
			return;
		}
		if (value instanceof Map) {
			((Map<String, Object>) value).forEach((selector, deser) -> {
				if (deser instanceof Deserializer) {
					this.delegates.put(selector, (Deserializer<?>) deser);
				}
				else if (deser instanceof Class) {
					instantiateAndConfigure(configs, isKey, this.delegates, selector, (Class<?>) deser);
				}
				else if (deser instanceof String) {
					createInstanceAndConfigure(configs, isKey, this.delegates, selector, (String) deser);
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

	protected static Map<String, Deserializer<?>> createDelegates(String mappings, Map<String, ?> configs,
			boolean isKey) {

		Map<String, Deserializer<?>> delegateMap = new HashMap<>();
		String[] array = StringUtils.commaDelimitedListToStringArray(mappings);
		for (String entry : array) {
			String[] split = entry.split(":");
			Assert.isTrue(split.length == 2, "Each comma-delimited selector entry must have exactly one ':'");
			createInstanceAndConfigure(configs, isKey, delegateMap, split[0], split[1]);
		}
		return delegateMap;
	}

	protected static void createInstanceAndConfigure(Map<String, ?> configs, boolean isKey,
			Map<String, Deserializer<?>> delegateMap, String selector, String className) {

		try {
			Class<?> clazz = ClassUtils.forName(className.trim(), ClassUtils.getDefaultClassLoader());
			instantiateAndConfigure(configs, isKey, delegateMap, selector, clazz);
		}
		catch (ClassNotFoundException | LinkageError e) {
			throw new IllegalArgumentException(e);
		}
	}

	protected static void instantiateAndConfigure(Map<String, ?> configs, boolean isKey,
			Map<String, Deserializer<?>> delegateMap, String selector, Class<?> clazz) {

		try {
			Deserializer<?> delegate = (Deserializer<?>) clazz.newInstance();
			delegate.configure(configs, isKey);
			delegateMap.put(selector.trim(), delegate);
		}
		catch (InstantiationException | IllegalAccessException e) {
			throw new IllegalArgumentException(e);
		}
	}

	public void addDelegate(String selector, Deserializer<?>  deserializer) {
		this.delegates.put(selector, deserializer);
	}

	@Nullable
	public Deserializer<?> removeDelegate(String selector) {
		return this.delegates.remove(selector);
	}

	@Override
	public Object deserialize(String topic, byte[] data) {
		throw new UnsupportedOperationException();
	}

	@Override
	public Object deserialize(String topic, Headers headers, byte[] data) {
		byte[] value = headers.lastHeader(DelegatingSerializer.SERIALIZATION_SELECTOR).value();
		if (value == null) {
			throw new IllegalStateException("No '" + DelegatingSerializer.SERIALIZATION_SELECTOR + "' header present");
		}
		String selector = new String(value).replaceAll("\"", "");
		@SuppressWarnings("unchecked")
		Deserializer<Object> deserializer = (Deserializer<Object>) this.delegates.get(selector);
		if (deserializer == null) {
			return data;
		}
		else {
			return deserializer.deserialize(topic, headers, data);
		}
	}

	@Override
	public void close() {
		this.delegates.values().forEach(deser -> deser.close());
	}

}
