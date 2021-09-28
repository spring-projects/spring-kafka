/*
 * Copyright 2021 the original author or authors.
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

import java.io.Closeable;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.regex.Pattern;

import org.springframework.core.log.LogAccessor;
import org.springframework.lang.Nullable;
import org.springframework.util.Assert;
import org.springframework.util.ClassUtils;
import org.springframework.util.StringUtils;

/**
 * Base class with common code for delegating by topic serialization.
 *
 * @param <T> the type.
 *
 * @author Gary Russell
 * @since 2.8
 *
 */
public abstract class DelegatingByTopicSerialization<T extends Closeable> implements Closeable {

	private static final LogAccessor LOGGER = new LogAccessor(DelegatingDeserializer.class);

	/**
	 * Name of the configuration property containing the serialization selector map for
	 * values with format {@code selector:class,...}.
	 */
	public static final String VALUE_SERIALIZATION_TOPIC_CONFIG = "spring.kafka.value.serialization.bytopic.config";

	/**
	 * Name of the configuration property containing the serialization topic pattern map for
	 * keys with format {@code pattern:class,...}.
	 */
	public static final String KEY_SERIALIZATION_TOPIC_CONFIG = "spring.kafka.key.serialization.bytopic.config";

	/**
	 * Name of the default delegate for keys when no topic name match is fount.
	 */
	public static final String VALUE_SERIALIZATION_TOPIC_DEFAULT = "spring.kafka.value.serialization.bytopic.default";

	/**
	 * Name of the default delegate for keys when no topic name match is fount.
	 */
	public static final String KEY_SERIALIZATION_TOPIC_DEFAULT = "spring.kafka.key.serialization.bytopic.default";

	/**
	 * Set to false to make topic pattern matching case-insensitive.
	 */
	public static final String CASE_SENSITIVE = "spring.kafka.value.serialization.bytopic.case.insensitive";

	private final Map<Pattern, T> delegates = new ConcurrentHashMap<>();

	private T defaultDelegate;

	protected boolean forKeys;

	private boolean cased = true;

	public DelegatingByTopicSerialization() {
	}

	public DelegatingByTopicSerialization(Map<Pattern, T> delegates, T defaultDelegate) {
		this.delegates.putAll(delegates);
		this.defaultDelegate = defaultDelegate;
	}

	/**
	 * Set to false to make topic name matching case insensitive.
	 * @param caseSensitive false for case insensitive.
	 */
	public void setCaseSensitive(boolean caseSensitive) {
		this.cased = caseSensitive;
	}

	@SuppressWarnings("unchecked")
	protected void configure(Map<String, ?> configs, boolean isKey) {
		this.forKeys = isKey;
		Object insensitive = configs.get(CASE_SENSITIVE);
		if (insensitive instanceof String) {
			this.cased = Boolean.parseBoolean((String) insensitive);
		}
		else if (insensitive instanceof Boolean) {
			this.cased = (Boolean) insensitive;
		}
		String configKey = defaultKey();
		if (configKey != null) {
			buildDefault(configs, configKey, isKey, configs.get(configKey));
		}
		configKey = configKey();
		Object value = configs.get(configKey);
		if (value == null) {
			return;
		}
		else if (value instanceof Map) {
			processMap(configs, isKey, configKey, (Map<Object, Object>) value);
		}
		else if (value instanceof String) {
			this.delegates.putAll(createDelegates((String) value, configs, isKey));
		}
		else {
			throw new IllegalStateException(
					configKey + " must be a map or String, not " + value.getClass());
		}
	}

	private String defaultKey() {
		return this.forKeys ? KEY_SERIALIZATION_TOPIC_DEFAULT : VALUE_SERIALIZATION_TOPIC_DEFAULT;
	}

	private String configKey() {
		return this.forKeys ? KEY_SERIALIZATION_TOPIC_CONFIG : VALUE_SERIALIZATION_TOPIC_CONFIG;
	}

	private void processMap(Map<String, ?> configs, boolean isKey, String configKey, Map<Object, Object> value) {
		value.forEach((key, delegate) -> {
			Pattern pattern = obtainPattern(key);
			build(configs, isKey, configKey, delegate, pattern);
		});
	}

	@SuppressWarnings("unchecked")
	protected void build(Map<String, ?> configs, boolean isKey, String configKey, Object delegate, Pattern pattern) {

		if (isInstance(delegate)) {
			this.delegates.put(pattern, (T) delegate);
			configureDelegate(configs, isKey, (T) delegate);
		}
		else if (delegate instanceof Class) {
			instantiateAndConfigure(configs, isKey, this.delegates, pattern, (Class<?>) delegate);
		}
		else if (delegate instanceof String) {
			createInstanceAndConfigure(configs, isKey, this.delegates, pattern, (String) delegate);
		}
		else {
			throw new IllegalStateException(configKey
					+ " map entries must be Serializers or class names, not " + delegate.getClass());
		}
	}

	@SuppressWarnings("unchecked")
	protected void buildDefault(Map<String, ?> configs, String configKey, boolean isKey, Object delegate) {

		if (isInstance(delegate)) {
			this.defaultDelegate = configureDelegate(configs, isKey, (T) delegate);
		}
		else if (delegate instanceof Class) {
			this.defaultDelegate = instantiateAndConfigure(configs, isKey, this.delegates, null, (Class<?>) delegate);
		}
		else if (delegate instanceof String) {
			this.defaultDelegate = createInstanceAndConfigure(configs, isKey, this.delegates, null, (String) delegate);
		}
		else {
			throw new IllegalStateException(configKey
					+ " map entries must be Serializers or class names, not " + delegate.getClass());
		}
	}

	/**
	 * Configure the delegate.
	 *
	 * @param configs the configs.
	 * @param isKey true if this is for keys.
	 * @param delegate the delegate.
	 * @return the delegate.
	 */
	protected abstract T configureDelegate(Map<String, ?> configs, boolean isKey, T delegate);

	/**
	 * Return true if this object is an instance of T.
	 * @param delegate the delegate.
	 * @return true if a T.
	 */
	protected abstract boolean isInstance(Object delegate);

	private Map<Pattern, T> createDelegates(String mappings, Map<String, ?> configs, boolean isKey) {

		Map<Pattern, T> delegateMap = new HashMap<>();
		String[] array = StringUtils.commaDelimitedListToStringArray(mappings);
		for (String entry : array) {
			String[] split = entry.split(":");
			Assert.isTrue(split.length == 2, "Each comma-delimited selector entry must have exactly one ':'");
			createInstanceAndConfigure(configs, isKey, delegateMap, obtainPattern(split[0]), split[1]);
		}
		return delegateMap;
	}

	private T createInstanceAndConfigure(Map<String, ?> configs, boolean isKey,
			Map<Pattern, T> delegates2, Pattern pattern, String className) {

		try {
			Class<?> clazz = ClassUtils.forName(className.trim(), ClassUtils.getDefaultClassLoader());
			return instantiateAndConfigure(configs, isKey, delegates2, pattern, clazz);
		}
		catch (ClassNotFoundException | LinkageError e) {
			throw new IllegalArgumentException(e);
		}
	}

	private Pattern obtainPattern(Object key) {
		if (key instanceof Pattern) {
			return (Pattern) key;
		}
		else if (key instanceof String) {
			if (this.cased) {
				return Pattern.compile(((String) key).trim());
			}
			else {
				return Pattern.compile(((String) key).trim(), Pattern.CASE_INSENSITIVE);
			}
		}
		else {
			throw new IllegalStateException("Map key must be a Pattern or a String, not a " + key.getClass());
		}
	}

	protected T instantiateAndConfigure(Map<String, ?> configs, boolean isKey, Map<Pattern, T> delegates2,
			@Nullable Pattern pattern, Class<?> clazz) {

		try {
			@SuppressWarnings("unchecked")
			T delegate = (T) clazz.getDeclaredConstructor().newInstance();
			configureDelegate(configs, isKey, delegate);
			if (pattern != null) {
				delegates2.put(pattern, delegate);
			}
			return delegate;
		}
		catch (Exception e) {
			throw new IllegalArgumentException(e);
		}
	}

	public void addDelegate(Pattern pattern, T serializer) {
		this.delegates.put(pattern, serializer);
	}

	@Nullable
	public T removeDelegate(String selector) {
		return this.delegates.remove(selector);
	}

	/**
	 * Determine the delegate for the topic.
	 * @param topic the topic.
	 * @return the delegate.
	 */
	@SuppressWarnings("unchecked")
	protected T findDelegate(String topic) {
		T delegate = null;
		for (Entry<Pattern, T> entry : this.delegates.entrySet()) {
			if (entry.getKey().matcher(topic).matches()) {
				delegate = entry.getValue();
				break;
			}
		}
		if (delegate == null) {
			delegate = this.defaultDelegate;
		}
		if (delegate == null) {
			throw new IllegalStateException(
					"No serializer found for topic '" + topic + "'");
		}
		return delegate;
	}

	@Override
	public void close() {
		this.delegates.values().forEach(delegate -> {
			try {
				delegate.close();
			}
			catch (IOException ex) {
				LOGGER.error(ex, () -> "Failed to close " + delegate);
			}
		});
	}

}
