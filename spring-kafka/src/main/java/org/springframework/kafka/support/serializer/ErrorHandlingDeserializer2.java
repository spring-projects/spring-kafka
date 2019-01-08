/*
 * Copyright 2018 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.springframework.kafka.support.serializer;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.util.Map;
import java.util.function.BiFunction;

import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.ExtendedDeserializer;

import org.springframework.util.Assert;
import org.springframework.util.ClassUtils;

/**
 * Delegating key/value deserializer that catches exceptions, returning them
 * in the headers as serialized java objects.
 *
 * @param <T> class of the entity, representing messages
 *
 * @author Gary Russell
 * @author Artem Bilan
 *
 * @since 2.2
 *
 */
public class ErrorHandlingDeserializer2<T> implements ExtendedDeserializer<T> {

	/**
	 * Header name for deserialization exceptions.
	 */
	public static final String KEY_DESERIALIZER_EXCEPTION_HEADER_PREFIX = "springDeserializerException";

	/**
	 * Header name for deserialization exceptions.
	 */
	public static final String KEY_DESERIALIZER_EXCEPTION_HEADER = KEY_DESERIALIZER_EXCEPTION_HEADER_PREFIX + "Key";

	/**
	 * Heaader name for deserialization exceptions.
	 */
	public static final String VALUE_DESERIALIZER_EXCEPTION_HEADER = KEY_DESERIALIZER_EXCEPTION_HEADER_PREFIX + "Value";

	/**
	 * Supplier for a T when deserialization fails.
	 */
	public static final String KEY_FUNCTION = "spring.deserializer.key.function";

	/**
	 * Supplier for a T when deserialization fails.
	 */
	public static final String VALUE_FUNCTION = "spring.deserializer.value.function";

	/**
	 * Property name for the delegate key deserializer.
	 */
	public static final String KEY_DESERIALIZER_CLASS = "spring.deserializer.key.delegate.class";

	/**
	 * Property name for the delegate value deserializer.
	 */
	public static final String VALUE_DESERIALIZER_CLASS = "spring.deserializer.value.delegate.class";

	private ExtendedDeserializer<T> delegate;

	private boolean isKey;

	private BiFunction<byte[], Headers, T> failedDeserializationFunction;

	public ErrorHandlingDeserializer2() {
		super();
	}

	public ErrorHandlingDeserializer2(Deserializer<T> delegate) {
		this.delegate = setupDelegate(delegate);
	}

	public void setFailedDeserializationFunction(BiFunction<byte[], Headers, T> failedDeserializationFunction) {
		this.failedDeserializationFunction = failedDeserializationFunction;
	}

	public boolean isKey() {
		return this.isKey;
	}

	/**
	 * Set to true if this deserializer is to be used as a key deserializer when
	 * configuring outside of Kafka.
	 * @param isKey true for a key deserializer, false otherwise.
	 * @since 2.2.3
	 */
	public void setKey(boolean isKey) {
		this.isKey = isKey;
	}

	/**
	 * Set to true if this deserializer is to be used as a key deserializer when
	 * configuring outside of Kafka.
	 * @param isKey true for a key deserializer, false otherwise.
	 * @return this
	 * @since 2.2.3
	 */
	public ErrorHandlingDeserializer2<T> keyDeserializer(boolean isKey) {
		this.isKey = isKey;
		return this;
	}

	@Override
	public void configure(Map<String, ?> configs, boolean isKey) {
		setupDelegate(configs, isKey ? KEY_DESERIALIZER_CLASS : VALUE_DESERIALIZER_CLASS);
		Assert.state(this.delegate != null, "No delegate deserializer configured");
		this.delegate.configure(configs, isKey);
		this.isKey = isKey;
		setupFunction(configs, isKey ? KEY_FUNCTION : VALUE_FUNCTION);
	}

	public void setupDelegate(Map<String, ?> configs, String configKey) {
		if (configs.containsKey(configKey)) {
			try {
				Object value = configs.get(configKey);
				Class<?> clazz = value instanceof Class ? (Class<?>) value : ClassUtils.forName((String) value, null);
				this.delegate = setupDelegate(clazz.newInstance());
			}
			catch (ClassNotFoundException | LinkageError | InstantiationException | IllegalAccessException e) {
				throw new IllegalStateException(e);
			}
		}
	}

	@SuppressWarnings("unchecked")
	private ExtendedDeserializer<T> setupDelegate(Object delegate) {
		Assert.isInstanceOf(Deserializer.class, delegate, "'delegate' must be a 'Deserializer', not a ");
		return delegate instanceof ExtendedDeserializer
				? (ExtendedDeserializer<T>) delegate
				: ExtendedDeserializer.Wrapper.ensureExtended((Deserializer<T>) delegate);
	}

	@SuppressWarnings("unchecked")
	private void setupFunction(Map<String, ?> configs, String configKey) {
		if (configs.containsKey(configKey)) {
			try {
				Object value = configs.get(configKey);
				Class<?> clazz = value instanceof Class ? (Class<?>) value : ClassUtils.forName((String) value, null);
				Assert.isTrue(BiFunction.class.isAssignableFrom(clazz), "'function' must be a 'BiFunction	', not a "
						+ clazz.getName());
				this.failedDeserializationFunction = (BiFunction<byte[], Headers, T>) clazz.newInstance();
			}
			catch (ClassNotFoundException | LinkageError | InstantiationException | IllegalAccessException e) {
				throw new IllegalStateException(e);
			}
		}
	}

	@Override
	public T deserialize(String topic, byte[] data) {
		try {
			return this.delegate.deserialize(topic, data);
		}
		catch (Exception e) {
			return this.failedDeserializationFunction != null
					? this.failedDeserializationFunction.apply(data, null)
					: null;
		}
	}

	@Override
	public T deserialize(String topic, Headers headers, byte[] data) {
		try {
			return this.delegate.deserialize(topic, headers, data);
		}
		catch (Exception e) {
			deserializationException(headers, data, e);
			return this.failedDeserializationFunction != null
					? this.failedDeserializationFunction.apply(data, headers)
					: null;
		}
	}

	@Override
	public void close() {
		this.delegate.close();
	}

	private void deserializationException(Headers headers, byte[] data, Exception e) {
		ByteArrayOutputStream stream = new ByteArrayOutputStream();
		DeserializationException exception = new DeserializationException("failed to deserialize", data, this.isKey, e);
		try {
			new ObjectOutputStream(stream).writeObject(exception);
		}
		catch (IOException ex) {
			try {
				exception = new DeserializationException("failed to deserialize",
						data, this.isKey, new RuntimeException("Could not deserialize type "
								+ e.getClass().getName() + " with message " + e.getMessage()
								+ " failure: " + ex.getMessage()));
				new ObjectOutputStream(stream).writeObject(exception);
			}
			catch (IOException ex2) {
				throw new IllegalStateException("Could not serialize a DeserializationException", ex2); // NOSONAR
			}
		}
		headers.add(
				new RecordHeader(this.isKey ? KEY_DESERIALIZER_EXCEPTION_HEADER : VALUE_DESERIALIZER_EXCEPTION_HEADER,
						stream.toByteArray()));
	}

}
