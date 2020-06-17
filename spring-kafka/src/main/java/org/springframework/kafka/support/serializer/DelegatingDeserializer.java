/*
 * Copyright 2019-2020 the original author or authors.
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
import java.util.concurrent.ConcurrentHashMap;

import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;

import org.springframework.lang.Nullable;
import org.springframework.util.Assert;
import org.springframework.util.ClassUtils;
import org.springframework.util.StringUtils;

/**
 * A {@link Deserializer} that delegates to other deserializers based on a serialization
 * selector header. It is not necessary to configure standard deserializers supported by
 * {@link Serdes}.
 *
 * @author Gary Russell
 * @since 2.3
 *
 */
public class DelegatingDeserializer implements Deserializer<Object> {

	/**
	 * Name of the configuration property containing the serialization selector map with
	 * format {@code selector:class,...}.
	 * @deprecated Use {@link DelegatingSerializer#VALUE_SERIALIZATION_SELECTOR} or
	 * {@link DelegatingSerializer#KEY_SERIALIZATION_SELECTOR}.
	 */
	@Deprecated
	public static final String SERIALIZATION_SELECTOR_CONFIG = DelegatingSerializer.SERIALIZATION_SELECTOR_CONFIG;


	private final Map<String, Deserializer<? extends Object>> delegates = new ConcurrentHashMap<>();

	private final Map<String, Object> autoConfigs = new HashMap<>();

	private boolean forKeys;

	/**
	 * Construct an instance that will be configured in {@link #configure(Map, boolean)}
	 * with consumer properties
	 * {@link DelegatingSerializer#KEY_SERIALIZATION_SELECTOR_CONFIG} and
	 * {@link DelegatingSerializer#VALUE_SERIALIZATION_SELECTOR_CONFIG}.
	 */
	public DelegatingDeserializer() {
	}

	/**
	 * Construct an instance with the supplied mapping of selectors to delegate
	 * deserializers. The selector must be supplied in the
	 * {@link DelegatingSerializer#SERIALIZATION_SELECTOR} header. It is not necessary to
	 * configure standard deserializers supported by {@link Serdes}.
	 * @param delegates the map of delegates.
	 */
	public DelegatingDeserializer(Map<String, Deserializer<?>> delegates) {
		this.delegates.putAll(delegates);
	}

	@SuppressWarnings("unchecked")
	@Override
	public void configure(Map<String, ?> configs, boolean isKey) {
		this.autoConfigs.putAll(configs);
		this.forKeys = isKey;
		String configKey = configKey();
		Object value = configs.get(configKey);
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
					throw new IllegalStateException(configKey
							+ " map entries must be Serializers or class names, not " + value.getClass());
				}
			});
		}
		else if (value instanceof String) {
			this.delegates.putAll(createDelegates((String) value, configs, isKey));
		}
		else {
			throw new IllegalStateException(configKey + " must be a map or String, not " + value.getClass());
		}
	}

	private String configKey() {
		return this.forKeys
				? DelegatingSerializer.KEY_SERIALIZATION_SELECTOR_CONFIG
				: DelegatingSerializer.VALUE_SERIALIZATION_SELECTOR_CONFIG;
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
			Deserializer<?> delegate = (Deserializer<?>) clazz.getDeclaredConstructor().newInstance();
			delegate.configure(configs, isKey);
			delegateMap.put(selector.trim(), delegate);
		}
		catch (Exception e) {
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
		byte[] value = null;
		String selectorKey = selectorKey();
		Header header = headers.lastHeader(selectorKey);
		if (header != null) {
			value = header.value();
		}
		if (value == null) {
			throw new IllegalStateException("No '" + selectorKey + "' header present");
		}
		String selector = new String(value).replaceAll("\"", "");
		Deserializer<? extends Object> deserializer = this.delegates.get(selector);
		if (deserializer == null) {
			deserializer = trySerdes(selector);
		}
		if (deserializer == null) {
			return data;
		}
		else {
			return deserializer.deserialize(topic, headers, data);
		}
	}

	private String selectorKey() {
		return this.forKeys
				? DelegatingSerializer.KEY_SERIALIZATION_SELECTOR
				: DelegatingSerializer.VALUE_SERIALIZATION_SELECTOR;
	}

	/*
	 * Package for testing.
	 */
	@Nullable
	Deserializer<? extends Object> trySerdes(String key) {
		try {
			Class<?> clazz = ClassUtils.forName(key, ClassUtils.getDefaultClassLoader());
			Serde<? extends Object> serdeFrom = Serdes.serdeFrom(clazz);
			Deserializer<? extends Object> deserializer = serdeFrom.deserializer();
			deserializer.configure(this.autoConfigs, this.forKeys);
			this.delegates.put(key, deserializer);
			return deserializer;
		}
		catch (IllegalStateException | ClassNotFoundException | LinkageError e) {
			this.delegates.put(key, Serdes.serdeFrom(byte[].class).deserializer());
			return null;
		}
	}

	@Override
	public void close() {
		this.delegates.values().forEach(deser -> deser.close());
	}

}
