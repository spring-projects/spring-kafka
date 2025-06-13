/*
 * Copyright 2021-2025 the original author or authors.
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

import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.serialization.Serializer;

import org.springframework.util.Assert;

/**
 * Delegates to a serializer based on type.
 *
 * @author Gary Russell
 * @author Artem Bilan
 * @author Wang Zhiyang
 * @author Mahesh Aravind V
 *
 * @since 2.7.9
 *
 */
public class DelegatingByTypeSerializer implements Serializer<Object> {

	private final Map<Class<?>, Serializer<?>> delegates = new LinkedHashMap<>();

	private final boolean assignable;

	private static final Comparator<Entry<Class<?>, Serializer<?>>> DELEGATES_ASSIGNABILITY_COMPARATOR =
			(entry1, entry2) -> {
				Class<?> type1 = entry1.getKey();
				Class<?> type2 = entry2.getKey();

				if (type1.isAssignableFrom(type2)) return 1;
				if (type2.isAssignableFrom(type1)) return -1;

				return 0;
			};

	/**
	 * Construct an instance with the map of delegates; keys matched exactly.
	 * @param delegates the delegates.
	 */
	public DelegatingByTypeSerializer(Map<Class<?>, Serializer<?>> delegates) {
		this(delegates, false);
	}

	/**
	 * Construct an instance with the map of delegates.
	 * If {@code assignable} is {@code false}, only exact key matches are considered.
	 * If {@code assignable} is {@code true}, a delegate is selected if its key class
	 * is assignable from the target object's class. When multiple matches are possible,
	 * the most specific matching class is selected — that is, the closest match in the
	 * class hierarchy.
	 * @since 2.8.3
	 */
	public DelegatingByTypeSerializer(Map<Class<?>, Serializer<?>> delegates, boolean assignable) {
		Assert.notNull(delegates, "'delegates' cannot be null");
		Assert.noNullElements(delegates.values(), "Serializers in delegates map cannot be null");

		if (!assignable) {
			this.delegates.putAll(delegates);
		} else {
			List<Entry<Class<?>, Serializer<?>>> sortedDelegates = delegates.entrySet().stream()
					.sorted(DELEGATES_ASSIGNABILITY_COMPARATOR).toList();

			sortedDelegates.forEach(it -> this.delegates.put(it.getKey(), it.getValue()));
		}

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

	@SuppressWarnings("NullAway") // Dataflow analysis limitation
	@Override
	public byte[] serialize(String topic, Object data) {
		if (data == null) {
			return null;
		}
		Serializer<Object> delegate = findDelegate(data);
		return delegate.serialize(topic, data);
	}

	@SuppressWarnings("NullAway") // Dataflow analysis limitation
	@Override
	public byte[] serialize(String topic, Headers headers, Object data) {
		if (data == null) {
			return null;
		}
		Serializer<Object> delegate = findDelegate(data);
		return delegate.serialize(topic, headers, data);
	}

	protected  <T> Serializer<T> findDelegate(T data) {
		return findDelegate(data, this.delegates);
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
						+ "; supported types: " + delegates.keySet().stream()
						.map(Class::getName)
						.toList());
			}
			return (Serializer<T>) delegate;
		}
		else {
			for (Entry<Class<?>, Serializer<?>> entry : delegates.entrySet()) {
				if (entry.getKey().isAssignableFrom(data.getClass())) {
					return (Serializer<T>) entry.getValue();
				}
			}
			throw new SerializationException("No matching delegate for type: " + data.getClass().getName()
					+ "; supported types: " + delegates.keySet().stream()
					.map(Class::getName)
					.toList());
		}
	}

}
