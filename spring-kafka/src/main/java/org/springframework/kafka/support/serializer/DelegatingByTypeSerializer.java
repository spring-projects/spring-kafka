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

import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.stream.Collectors;

import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.serialization.Serializer;

import org.springframework.util.Assert;

/**
 * Delegates to a serializer based on type.
 *
 * @author Gary Russell
 * @since 2.7.9
 *
 */
public class DelegatingByTypeSerializer implements Serializer<Object> {

	private static final String RAWTYPES = "rawtypes";

	@SuppressWarnings(RAWTYPES)
	private final Map<Class<?>, Serializer> delegates = new LinkedHashMap<>();

	private final boolean assignable;

	/**
	 * Construct an instance with the map of delegates; keys matched exactly.
	 * @param delegates the delegates.
	 */
	@SuppressWarnings(RAWTYPES)
	public DelegatingByTypeSerializer(Map<Class<?>, Serializer> delegates) {
		this(delegates, false);
	}

	/**
	 * Construct an instance with the map of delegates; keys matched exactly or if the
	 * target object is assignable to the key, depending on the assignable argument.
	 * If assignable, entries are checked in the natural entry order so an ordered map
	 * such as a {@link LinkedHashMap} is recommended.
	 * @param delegates the delegates.
	 * @param assignable whether the target is assignable to the key.
	 * @since 2.8.3
	 */
	@SuppressWarnings(RAWTYPES)
	public DelegatingByTypeSerializer(Map<Class<?>, Serializer> delegates, boolean assignable) {
		Assert.notNull(delegates, "'delegates' cannot be null");
		Assert.noNullElements(delegates.values(), "Serializers in delegates map cannot be null");
		this.delegates.putAll(delegates);
		this.assignable = assignable;
	}

	/**
	 * Returns true if {@link #findDelegate(Object, Map)} should consider assignability to
	 * the key rather than an exact match.
	 * @return true if assigable.
	 * @since 2.8.3
	 */
	protected boolean isAssignable() {
		return this.assignable;
	}

	@SuppressWarnings("unchecked")
	@Override
	public void configure(Map<String, ?> configs, boolean isKey) {
		this.delegates.values().forEach(del -> del.configure(configs, isKey));
	}

	@SuppressWarnings({ RAWTYPES, "unchecked" })
	@Override
	public byte[] serialize(String topic, Object data) {
		Serializer delegate = findDelegate(data, this.delegates);
		return delegate.serialize(topic, data);
	}

	@SuppressWarnings({ "unchecked", RAWTYPES })
	@Override
	public byte[] serialize(String topic, Headers headers, Object data) {
		Serializer delegate = findDelegate(data, this.delegates);
		return delegate.serialize(topic, headers, data);
	}

	/**
	 * Determine the serializer for the data type.
	 * @param data the data.
	 * @param delegates the available delegates.
	 * @return the delgate.
	 * @throws SerializationException when there is no match.
	 * @since 2.8.3
	 */
	@SuppressWarnings(RAWTYPES)
	protected Serializer findDelegate(Object data, Map<Class<?>, Serializer> delegates) {
		if (!this.assignable) {
			Serializer delegate = delegates.get(data.getClass());
			if (delegate == null) {
				throw new SerializationException("No matching delegate for type: " + data.getClass().getName()
						+ "; supported types: " + this.delegates.keySet().stream()
								.map(clazz -> clazz.getName())
								.collect(Collectors.toList()));
			}
			return delegate;
		}
		else {
			Iterator<Entry<Class<?>, Serializer>> iterator = this.delegates.entrySet().iterator();
			while (iterator.hasNext()) {
				Entry<Class<?>, Serializer> entry = iterator.next();
				if (entry.getKey().isAssignableFrom(data.getClass())) {
					return entry.getValue();
				}
			}
			throw new SerializationException("No matching delegate for type: " + data.getClass().getName()
					+ "; supported types: " + this.delegates.keySet().stream()
							.map(clazz -> clazz.getName())
							.collect(Collectors.toList()));
		}
	}


}
