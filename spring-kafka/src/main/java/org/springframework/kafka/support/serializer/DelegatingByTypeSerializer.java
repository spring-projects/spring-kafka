/*
 * Copyright 2021-2022 the original author or authors.
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
 * @author Artem Bilan
 *
 * @since 2.7.9
 *
 */
public class DelegatingByTypeSerializer implements Serializer<Object> {

	private final Map<Class<?>, Serializer<?>> delegates = new LinkedHashMap<>();

	private final boolean assignable;

	/**
	 * Construct an instance with the map of delegates; keys matched exactly.
	 * @param delegates the delegates.
	 */
	public DelegatingByTypeSerializer(Map<Class<?>, Serializer<?>> delegates) {
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
	public DelegatingByTypeSerializer(Map<Class<?>, Serializer<?>> delegates, boolean assignable) {
		Assert.notNull(delegates, "'delegates' cannot be null");
		Assert.noNullElements(delegates.values(), "Serializers in delegates map cannot be null");
		this.delegates.putAll(delegates);
		this.assignable = assignable;
	}

	/**
	 * Returns true if {@link #findDelegate(Object, Map)} should consider assignability to
	 * the key rather than an exact match.
	 * @return true if assignable.
	 * @since 2.8.3
	 */
	protected boolean isAssignable() {
		return this.assignable;
	}

	@Override
	public void configure(Map<String, ?> configs, boolean isKey) {
		this.delegates.values().forEach(del -> del.configure(configs, isKey));
	}

	@Override
	public byte[] serialize(String topic, Object data) {
		if (data == null) {
			return null;
		}
		Serializer<Object> delegate = findDelegate(data, this.delegates);
		return delegate.serialize(topic, data);
	}

	@Override
	public byte[] serialize(String topic, Headers headers, Object data) {
		if (data == null) {
			return null;
		}
		Serializer<Object> delegate = findDelegate(data, this.delegates);
		return delegate.serialize(topic, headers, data);
	}

	/**
	 * Determine the serializer for the data type.
	 * @param data the data.
	 * @param delegates the available delegates.
	 * @param <T> the data type
	 * @return the delegate.
	 * @throws SerializationException when there is no match.
	 * @since 2.8.3
	 */
	@SuppressWarnings("unchecked")
	protected <T> Serializer<T> findDelegate(T data, Map<Class<?>, Serializer<?>> delegates) {
		if (!this.assignable) {
			Serializer<?> delegate = delegates.get(data.getClass());
			if (delegate == null) {
				throw new SerializationException("No matching delegate for type: " + data.getClass().getName()
						+ "; supported types: " + this.delegates.keySet().stream()
						.map(Class::getName)
						.collect(Collectors.toList()));
			}
			return (Serializer<T>) delegate;
		}
		else {
			for (Entry<Class<?>, Serializer<?>> entry : this.delegates.entrySet()) {
				if (entry.getKey().isAssignableFrom(data.getClass())) {
					return (Serializer<T>) entry.getValue();
				}
			}
			throw new SerializationException("No matching delegate for type: " + data.getClass().getName()
					+ "; supported types: " + this.delegates.keySet().stream()
					.map(Class::getName)
					.collect(Collectors.toList()));
		}
	}

}
