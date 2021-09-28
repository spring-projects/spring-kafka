/*
 * Copyright 2019-2021 the original author or authors.
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
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.regex.Pattern;

import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.serialization.Serializer;

import org.springframework.core.log.LogAccessor;
import org.springframework.lang.Nullable;
import org.springframework.util.Assert;
import org.springframework.util.ClassUtils;
import org.springframework.util.StringUtils;

/**
 * A {@link Serializer} that delegates to other serializers based on a topic pattern.
 *
 * @author Gary Russell
 * @since 2.8
 *
 */
public class DelegatingByTopicSerializer implements Serializer<Object> {

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
	 * Set to false to make topic pattern matching case-insensitive.
	 */
	public static final String CASE_SENSITIVE = "spring.kafka.value.serialization.bytopic.case.insensitive";

	private final Map<Pattern, Serializer<?>> delegates = new ConcurrentHashMap<>();

	private boolean forKeys;

	private boolean cased = true;

	/**
	 * Construct an instance that will be configured in {@link #configure(Map, boolean)}
	 * with producer properties {@link #VALUE_SERIALIZATION_TOPIC_CONFIG} and
	 * {@link #KEY_SERIALIZATION_TOPIC_CONFIG}.
	 */
	public DelegatingByTopicSerializer() {
	}

	/**
	 * Construct an instance with the supplied mapping of topic patterns to delegate
	 * serializers.
	 * @param delegates the map of delegates.
	 */
	public DelegatingByTopicSerializer(Map<Pattern, Serializer<?>> delegates) {
		this.delegates.putAll(delegates);
	}

	/**
	 * Set to false to make topic name matching case insensitive.
	 * @param caseSensitive false for case insensitive.
	 */
	public void setCaseSensitive(boolean caseSensitive) {
		this.cased = caseSensitive;
	}

	@Override
	public void configure(Map<String, ?> configs, boolean isKey) {
		this.forKeys = isKey;
		Object insensitive = configs.get(CASE_SENSITIVE);
		if (insensitive instanceof String) {
			this.cased = Boolean.parseBoolean((String) insensitive);
		}
		String configKey = configKey();
		Object value = configs.get(configKey);
		if (value == null) {
			return;
		}
		else if (value instanceof Map) {
			processMap(configs, isKey, configKey, value);
		}
		else if (value instanceof String) {
			this.delegates.putAll(createDelegates((String) value, configs, isKey));
		}
		else {
			throw new IllegalStateException(
					configKey + " must be a map or String, not " + value.getClass());
		}
	}

	@SuppressWarnings("unchecked")
	private void processMap(Map<String, ?> configs, boolean isKey, String configKey, Object value) {
		((Map<Object, Object>) value).forEach((key, serializer) -> {
			Pattern pattern = obtainPattern(key);
			if (serializer instanceof Serializer) {
				this.delegates.put(pattern, (Serializer<?>) serializer);
				((Serializer<?>) serializer).configure(configs, isKey);
			}
			else if (serializer instanceof Class) {
				instantiateAndConfigure(configs, isKey, this.delegates, pattern, (Class<?>) serializer);
			}
			else if (serializer instanceof String) {
				createInstanceAndConfigure(configs, isKey, this.delegates, pattern, (String) serializer);
			}
			else {
				throw new IllegalStateException(configKey
						+ " map entries must be Serializers or class names, not " + value.getClass());
			}
		});
	}

	private String configKey() {
		return this.forKeys ? KEY_SERIALIZATION_TOPIC_CONFIG : VALUE_SERIALIZATION_TOPIC_CONFIG;
	}

	protected Map<Pattern, Serializer<?>> createDelegates(String mappings, Map<String, ?> configs,
			boolean isKey) {

		Map<Pattern, Serializer<?>> delegateMap = new HashMap<>();
		String[] array = StringUtils.commaDelimitedListToStringArray(mappings);
		for (String entry : array) {
			String[] split = entry.split(":");
			Assert.isTrue(split.length == 2, "Each comma-delimited selector entry must have exactly one ':'");
			createInstanceAndConfigure(configs, isKey, delegateMap, obtainPattern(split[0]), split[1]);
		}
		return delegateMap;
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

	protected static void createInstanceAndConfigure(Map<String, ?> configs, boolean isKey,
			Map<Pattern, Serializer<?>> delegates2, Pattern pattern, String className) {

		try {
			Class<?> clazz = ClassUtils.forName(className.trim(), ClassUtils.getDefaultClassLoader());
			instantiateAndConfigure(configs, isKey, delegates2, pattern, clazz);
		}
		catch (ClassNotFoundException | LinkageError e) {
			throw new IllegalArgumentException(e);
		}
	}

	protected static void instantiateAndConfigure(Map<String, ?> configs, boolean isKey,
			Map<Pattern, Serializer<?>> delegates2, Pattern pattern, Class<?> clazz) {

		try {
			Serializer<?> delegate = (Serializer<?>) clazz.getDeclaredConstructor().newInstance();
			delegate.configure(configs, isKey);
			delegates2.put(pattern, delegate);
		}
		catch (Exception e) {
			throw new IllegalArgumentException(e);
		}
	}

	public void addDelegate(Pattern pattern, Serializer<?> serializer) {
		this.delegates.put(pattern, serializer);
	}

	@Nullable
	public Serializer<?> removeDelegate(String selector) {
		return this.delegates.remove(selector);
	}

	@Override
	public byte[] serialize(String topic, Object data) {
		throw new UnsupportedOperationException();
	}


	@SuppressWarnings("unchecked")
	@Override
	public byte[] serialize(String topic, Headers headers, Object data) {
		Serializer<Object> serializer = findSerializer(topic);
		return serializer.serialize(topic, headers, data);
	}

	/**
	 * Determine the serializer for the topic.
	 * @param topic the topic.
	 * @return the serializer.
	 */
	@SuppressWarnings("unchecked")
	protected Serializer<Object> findSerializer(String topic) {
		Serializer<Object> serializer = null;
		for (Entry<Pattern, Serializer<?>> entry : this.delegates.entrySet()) {
			if (entry.getKey().matcher(topic).matches()) {
				serializer = (Serializer<Object>) entry.getValue();
				break;
			}
		}
		if (serializer == null) {
			throw new IllegalStateException(
					"No serializer found for topic '" + topic + "'");
		}
		return serializer;
	}

	@Override
	public void close() {
		this.delegates.values().forEach(ser -> ser.close());
	}

}
