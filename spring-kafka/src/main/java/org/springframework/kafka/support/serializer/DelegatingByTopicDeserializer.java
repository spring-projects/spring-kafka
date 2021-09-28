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
import org.apache.kafka.common.serialization.Deserializer;

import org.springframework.lang.Nullable;
import org.springframework.util.Assert;
import org.springframework.util.ClassUtils;
import org.springframework.util.StringUtils;

/**
 * A {@link Deserializer} that delegates to other deserializers based on the topic name.
 *
 * @author Gary Russell
 * @since 2.8
 *
 */
public class DelegatingByTopicDeserializer implements Deserializer<Object> {

	private final Map<Pattern, Deserializer<? extends Object>> delegates = new ConcurrentHashMap<>();

	private boolean forKeys;

	private boolean cased = true;

	/**
	 * Construct an instance that will be configured in {@link #configure(Map, boolean)}
	 * with consumer properties.
	 */
	public DelegatingByTopicDeserializer() {
	}

	/**
	 * Construct an instance with the supplied mapping of topic name patterns to delegate
	 * deserializers.
	 * @param delegates the map of delegates.
	 */
	public DelegatingByTopicDeserializer(Map<Pattern, Deserializer<?>> delegates) {
		this.delegates.putAll(delegates);
	}

	/**
	 * Set to false to make topic name matching case insensitive.
	 * @param caseSensitive false for case insensitive.
	 */
	public void setCaseSensitive(boolean caseSensitive) {
		this.cased = caseSensitive;
	}

	@SuppressWarnings("unchecked")
	@Override
	public void configure(Map<String, ?> configs, boolean isKey) {
		this.forKeys = isKey;
		Object insensitive = configs.get(DelegatingByTopicSerializer.CASE_SENSITIVE);
		if (insensitive instanceof String) {
			this.cased = Boolean.parseBoolean((String) insensitive);
		}
		String configKey = configKey();
		Object value = configs.get(configKey);
		if (value == null) {
			return;
		}
		if (value instanceof Map) {
			processMap(configs, isKey, configKey, value);
		}
		else if (value instanceof String) {
			this.delegates.putAll(createDelegates((String) value, configs, isKey));
		}
		else {
			throw new IllegalStateException(configKey + " must be a map or String, not " + value.getClass());
		}
	}

	@SuppressWarnings("unchecked")
	private void processMap(Map<String, ?> configs, boolean isKey, String configKey, Object value) {
		((Map<Object, Object>) value).forEach((key, deserializer) -> {
			Pattern pattern = obtainPattern(key);
			if (deserializer instanceof Deserializer) {
				this.delegates.put(pattern, (Deserializer<?>) deserializer);
				((Deserializer<?>) deserializer).configure(configs, isKey);
			}
			else if (deserializer instanceof Class) {
				instantiateAndConfigure(configs, isKey, this.delegates, pattern, (Class<?>) deserializer);
			}
			else if (deserializer instanceof String) {
				createInstanceAndConfigure(configs, isKey, this.delegates, pattern, (String) deserializer);
			}
			else {
				throw new IllegalStateException(configKey
						+ " map entries must be Serializers or class names, not " + value.getClass());
			}
		});
	}

	private String configKey() {
		return this.forKeys
				? DelegatingByTopicSerializer.KEY_SERIALIZATION_TOPIC_CONFIG
				: DelegatingByTopicSerializer.VALUE_SERIALIZATION_TOPIC_CONFIG;
	}

	protected Map<Pattern, Deserializer<?>> createDelegates(String mappings, Map<String, ?> configs,
			boolean isKey) {

		Map<Pattern, Deserializer<?>> delegateMap = new HashMap<>();
		String[] array = StringUtils.commaDelimitedListToStringArray(mappings);
		for (String entry : array) {
			String[] split = entry.split(":");
			Assert.isTrue(split.length == 2, "Each comma-delimited entry entry must have exactly one ':'");
			createInstanceAndConfigure(configs, isKey, delegateMap, obtainPattern(split[0]), split[1]);
		}
		return delegateMap;
	}

	protected static void createInstanceAndConfigure(Map<String, ?> configs, boolean isKey,
			Map<Pattern, Deserializer<?>> delegateMap, Pattern pattern, String className) {

		try {
			Class<?> clazz = ClassUtils.forName(className.trim(), ClassUtils.getDefaultClassLoader());
			instantiateAndConfigure(configs, isKey, delegateMap, pattern, clazz);
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

	protected static void instantiateAndConfigure(Map<String, ?> configs, boolean isKey,
			Map<Pattern, Deserializer<?>> delegateMap, Pattern pattern, Class<?> clazz) {

		try {
			Deserializer<?> delegate = (Deserializer<?>) clazz.getDeclaredConstructor().newInstance();
			delegate.configure(configs, isKey);
			delegateMap.put(pattern, delegate);
		}
		catch (Exception e) {
			throw new IllegalArgumentException(e);
		}
	}

	public void addDelegate(Pattern pattern, Deserializer<?>  deserializer) {
		this.delegates.put(pattern, deserializer);
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
		return findDeserializer(topic).deserialize(topic, headers, data);
	}

	/**
	 * Determine the deserializer for the topic.
	 * @param topic the topic.
	 * @return the deserializer.
	 */
	@SuppressWarnings("unchecked")
	protected Deserializer<Object> findDeserializer(String topic) {
		Deserializer<Object> deserializer = null;
		for (Entry<Pattern, Deserializer<?>> entry : this.delegates.entrySet()) {
			if (entry.getKey().matcher(topic).matches()) {
				deserializer = (Deserializer<Object>) entry.getValue();
				break;
			}
		}
		if (deserializer == null) {
			throw new IllegalStateException(
					"No deserializer found for topic '" + topic + "'");
		}
		return deserializer;
	}

	private String selectorKey() {
		return this.forKeys
				? DelegatingSerializer.KEY_SERIALIZATION_SELECTOR
				: DelegatingSerializer.VALUE_SERIALIZATION_SELECTOR;
	}

	@Override
	public void close() {
		this.delegates.values().forEach(deser -> deser.close());
	}

}
