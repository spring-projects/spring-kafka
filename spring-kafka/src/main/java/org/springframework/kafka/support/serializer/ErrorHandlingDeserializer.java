/*
 * Copyright 2018-2019 the original author or authors.
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

import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.serialization.Deserializer;

import org.springframework.util.Assert;
import org.springframework.util.ClassUtils;

/**
 * Delegating key/value deserializer that catches exceptions, returning them
 * in the consumer record.
 *
 * @param <T> class of the entity, representing messages
 *
 * @author Gary Russell
 * @author Artem Bilan
 * @deprecated in favor of {@link ErrorHandlingDeserializer2}.
 *
 * @since 2.2
 *
 */
@Deprecated
public class ErrorHandlingDeserializer<T> implements Deserializer<T> {

	/**
	 * Property name for the delegate key deserializer.
	 */
	public static final String KEY_DESERIALIZER_CLASS = "spring.deserializer.key.delegate.class";

	/**
	 * Property name for the delegate value deserializer.
	 */
	public static final String VALUE_DESERIALIZER_CLASS = "spring.deserializer.value.delegate.class";

	private Deserializer<T> delegate;

	private boolean isForKey;

	public ErrorHandlingDeserializer() {
	}

	public ErrorHandlingDeserializer(Deserializer<T> delegate) {
		this.delegate = setupDelegate(delegate);
	}

	@Override
	public void configure(Map<String, ?> configs, boolean isKey) {
		if (isKey && configs.containsKey(KEY_DESERIALIZER_CLASS)) {
			try {
				Object value = configs.get(KEY_DESERIALIZER_CLASS);
				Class<?> clazz = value instanceof Class ? (Class<?>) value : ClassUtils.forName((String) value, null);
				this.delegate = setupDelegate(clazz.newInstance());
			}
			catch (ClassNotFoundException | LinkageError | InstantiationException | IllegalAccessException e) {
				throw new IllegalStateException(e);
			}
		}
		else if (!isKey && configs.containsKey(VALUE_DESERIALIZER_CLASS)) {
			try {
				Object value = configs.get(VALUE_DESERIALIZER_CLASS);
				Class<?> clazz = value instanceof Class ? (Class<?>) value : ClassUtils.forName((String) value, null);
				this.delegate = setupDelegate(clazz.newInstance());
			}
			catch (ClassNotFoundException | LinkageError | InstantiationException | IllegalAccessException e) {
				throw new IllegalStateException(e);
			}
		}
		Assert.state(this.delegate != null, "No delegate deserializer configured");
		this.delegate.configure(configs, isKey);
		this.isForKey = isKey;
	}

	@SuppressWarnings("unchecked")
	private Deserializer<T> setupDelegate(Object delegate) {
		Assert.isInstanceOf(Deserializer.class, delegate, "'delegate' must be a 'Deserializer', not a ");
		return (Deserializer<T>) delegate;
	}

	@Override
	public T deserialize(String topic, byte[] data) {
		try {
			return this.delegate.deserialize(topic, data);
		}
		catch (Exception e) {
			return deserializationException(null, data, e);
		}
	}

	@Override
	public T deserialize(String topic, Headers headers, byte[] data) {
		try {
			return this.delegate.deserialize(topic, headers, data);
		}
		catch (Exception e) {
			return deserializationException(headers, data, e);
		}
	}

	@Override
	public void close() {
		this.delegate.close();
	}

	@SuppressWarnings("unchecked")
	private T deserializationException(Headers headers, byte[] data, Exception e) {
		// We need this container to trick a generic type. It doesn't matter at runtime anyway.
		AtomicReference<T> reference =
				(AtomicReference<T>) new AtomicReference<>(
						new DeserializationException("Failed to deserialize", headers, data, this.isForKey, e));
		return reference.get();
	}

}
