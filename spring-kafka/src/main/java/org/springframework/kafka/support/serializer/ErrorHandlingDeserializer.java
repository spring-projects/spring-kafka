/*
 * Copyright 2018-2021 the original author or authors.
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
import java.util.function.Function;

import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.serialization.Deserializer;

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
 * @author Victor Perez Rey
 *
 * @since 2.2
 *
 */
public class ErrorHandlingDeserializer<T> implements Deserializer<T> {

	/**
	 * Header name for deserialization exceptions.
	 * @deprecated in favor of {@link SerializationUtils#DESERIALIZER_EXCEPTION_HEADER_PREFIX}.
	 */
	@Deprecated
	public static final String KEY_DESERIALIZER_EXCEPTION_HEADER_PREFIX =
			SerializationUtils.DESERIALIZER_EXCEPTION_HEADER_PREFIX;

	/**
	 * Header name for deserialization exceptions.
	 * @deprecated in favor of {@link SerializationUtils#KEY_DESERIALIZER_EXCEPTION_HEADER}.
	 */
	@Deprecated
	public static final String KEY_DESERIALIZER_EXCEPTION_HEADER = SerializationUtils.KEY_DESERIALIZER_EXCEPTION_HEADER;

	/**
	 * Header name for deserialization exceptions.
	 * @deprecated in favor of {@link SerializationUtils#VALUE_DESERIALIZER_EXCEPTION_HEADER}.
	 */
	@Deprecated
	public static final String VALUE_DESERIALIZER_EXCEPTION_HEADER = SerializationUtils.VALUE_DESERIALIZER_EXCEPTION_HEADER;

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

	private Deserializer<T> delegate;

	private boolean isForKey;

	private Function<FailedDeserializationInfo, T> failedDeserializationFunction;

	public ErrorHandlingDeserializer() {
	}

	public ErrorHandlingDeserializer(Deserializer<T> delegate) {
		this.delegate = setupDelegate(delegate);
	}

	/**
	 * Provide an alternative supplying mechanism when deserialization fails.
	 * @param failedDeserializationFunction the {@link Function} to use.
	 * @since 2.2.8
	 */
	public void setFailedDeserializationFunction(Function<FailedDeserializationInfo, T> failedDeserializationFunction) {
		this.failedDeserializationFunction = failedDeserializationFunction;
	}

	public boolean isForKey() {
		return this.isForKey;
	}

	/**
	 * Set to true if this deserializer is to be used as a key deserializer when
	 * configuring outside of Kafka.
	 * @param isKey true for a key deserializer, false otherwise.
	 * @since 2.2.3
	 */
	public void setForKey(boolean isKey) {
		this.isForKey = isKey;
	}

	/**
	 * Set to true if this deserializer is to be used as a key deserializer when
	 * configuring outside of Kafka.
	 * @param isKey true for a key deserializer, false otherwise.
	 * @return this
	 * @since 2.2.3
	 */
	public ErrorHandlingDeserializer<T> keyDeserializer(boolean isKey) {
		this.isForKey = isKey;
		return this;
	}

	@Override
	public void configure(Map<String, ?> configs, boolean isKey) {
		if (this.delegate == null) {
			setupDelegate(configs, isKey ? KEY_DESERIALIZER_CLASS : VALUE_DESERIALIZER_CLASS);
		}
		Assert.state(this.delegate != null, "No delegate deserializer configured");
		this.delegate.configure(configs, isKey);
		this.isForKey = isKey;
		if (this.failedDeserializationFunction == null) {
			setupFunction(configs, isKey ? KEY_FUNCTION : VALUE_FUNCTION);
		}
	}

	public void setupDelegate(Map<String, ?> configs, String configKey) {
		if (configs.containsKey(configKey)) {
			try {
				Object value = configs.get(configKey);
				Class<?> clazz = value instanceof Class ? (Class<?>) value : ClassUtils.forName((String) value, null);
				this.delegate = setupDelegate(clazz.getDeclaredConstructor().newInstance());
			}
			catch (Exception e) {
				throw new IllegalStateException(e);
			}
		}
	}

	@SuppressWarnings("unchecked")
	private Deserializer<T> setupDelegate(Object delegate) {
		Assert.isInstanceOf(Deserializer.class, delegate, "'delegate' must be a 'Deserializer', not a ");
		return (Deserializer<T>) delegate;
	}

	@SuppressWarnings("unchecked")
	private void setupFunction(Map<String, ?> configs, String configKey) {
		if (configs.containsKey(configKey)) {
			try {
				Object value = configs.get(configKey);
				Class<?> clazz = value instanceof Class ? (Class<?>) value : ClassUtils.forName((String) value, null);
				Assert.isTrue(Function.class.isAssignableFrom(clazz), "'function' must be a 'Function ', not a "
						+ clazz.getName());
				this.failedDeserializationFunction = (Function<FailedDeserializationInfo, T>)
						clazz.getDeclaredConstructor().newInstance();
			}
			catch (Exception e) {
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
			return recoverFromSupplier(topic, null, data, e);
		}
	}

	@Override
	public T deserialize(String topic, Headers headers, byte[] data) {
		try {
			if (this.isForKey) {
				headers.remove(SerializationUtils.KEY_DESERIALIZER_EXCEPTION_HEADER);
			}
			else {
				headers.remove(SerializationUtils.VALUE_DESERIALIZER_EXCEPTION_HEADER);
			}
			return this.delegate.deserialize(topic, headers, data);
		}
		catch (Exception e) {
			SerializationUtils.deserializationException(headers, data, e, this.isForKey);
			return recoverFromSupplier(topic, headers, data, e);
		}
	}

	private T recoverFromSupplier(String topic, Headers headers, byte[] data, Exception exception) {
		if (this.failedDeserializationFunction != null) {
			FailedDeserializationInfo failedDeserializationInfo =
					new FailedDeserializationInfo(topic, headers, data, this.isForKey, exception);
			return this.failedDeserializationFunction.apply(failedDeserializationInfo);
		}
		else {
			return null;
		}
	}

	@Override
	public void close() {
		if (this.delegate != null) {
			this.delegate.close();
		}
	}

}
