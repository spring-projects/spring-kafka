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

import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;

import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.serialization.Serializer;

import org.springframework.util.Assert;

/**
 * Delegates to a serializer based on type.
 *
 * @author Gary Russell
 * @since 2.8
 *
 */
public class DelegatingByTypeSerializer implements Serializer<Object> {

	@SuppressWarnings("rawtypes")
	private final Map<Class<?>, Serializer> delegates = new HashMap<>();

	/**
	 * Construct an instance with the map of delegates.
	 * @param delegates the delegates.
	 */
	@SuppressWarnings("rawtypes")
	public DelegatingByTypeSerializer(Map<Class<?>, Serializer> delegates) {
		Assert.notNull(delegates, "'delegates' cannot be null");
		Assert.noNullElements(delegates.values(), "Serializers in delegates map cannot be null");
		this.delegates.putAll(delegates);
	}

	@SuppressWarnings("unchecked")
	@Override
	public void configure(Map<String, ?> configs, boolean isKey) {
		this.delegates.values().forEach(del -> del.configure(configs, isKey));
	}

	@SuppressWarnings({ "rawtypes", "unchecked" })
	@Override
	public byte[] serialize(String topic, Object data) {
		Serializer delegate = findDelegate(data);
		return delegate.serialize(topic, data);
	}

	@SuppressWarnings({ "unchecked", "rawtypes" })
	@Override
	public byte[] serialize(String topic, Headers headers, Object data) {
		Serializer delegate = findDelegate(data);
		return delegate.serialize(topic, headers, data);
	}

	@SuppressWarnings("rawtypes")
	private Serializer findDelegate(Object data) {
		Serializer delegate = this.delegates.get(data.getClass());
		if (delegate == null) {
			throw new SerializationException("No matching delegate for type: " + data.getClass().getName()
					+ "; supported types: " + this.delegates.keySet().stream()
							.map(clazz -> clazz.getName())
							.collect(Collectors.toList()));
		}
		return delegate;
	}


}
