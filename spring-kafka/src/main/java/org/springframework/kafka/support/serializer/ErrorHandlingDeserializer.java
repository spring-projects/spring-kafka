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

import java.util.Map;

import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.serialization.ExtendedDeserializer;

import org.springframework.util.Assert;
import org.springframework.util.ClassUtils;

/**
 * Delegating key/value deserializer that catches exceptions, returning them
 * in the consumer record.
 *
 * @author Gary Russell
 * @since 2.2
 *
 */
public class ErrorHandlingDeserializer implements ExtendedDeserializer<Object> {

	/**
	 * Property name for the delegate key deserializer.
	 */
	public static final String KEY_DESERIALIZER_CLASS = "spring.deserializer.key.delegate.class";

	/**
	 * Property name for the delegate value deserializer.
	 */
	public static final String VALUE_DESERIALIZER_CLASS = "spring.deserializer.value.delegate.class";

	private ExtendedDeserializer<Object> delegate;

	private boolean isKey;

	public ErrorHandlingDeserializer() {
		super();
	}

	public ErrorHandlingDeserializer(ExtendedDeserializer<Object> delegate) {
		this.delegate = delegate;
	}

	@SuppressWarnings("unchecked")
	@Override
	public void configure(Map<String, ?> configs, boolean isKey) {
		if (isKey && configs.containsKey(KEY_DESERIALIZER_CLASS)) {
				try {
					Object value = configs.get(KEY_DESERIALIZER_CLASS);
					Class<?> clazz = value instanceof Class ? (Class<?>) value : ClassUtils.forName((String) value, null);
					this.delegate = (ExtendedDeserializer<Object>) clazz.newInstance();
				}
				catch (ClassNotFoundException | LinkageError | InstantiationException | IllegalAccessException e) {
					throw new IllegalStateException(e);
				}
		}
		else if (!isKey && configs.containsKey(VALUE_DESERIALIZER_CLASS)) {
			try {
				Object value = configs.get(VALUE_DESERIALIZER_CLASS);
				Class<?> clazz = value instanceof Class ? (Class<?>) value : ClassUtils.forName((String) value, null);
				this.delegate = (ExtendedDeserializer<Object>) clazz.newInstance();
			}
			catch (ClassNotFoundException | LinkageError | InstantiationException | IllegalAccessException e) {
				throw new IllegalStateException(e);
			}
		}
		Assert.state(this.delegate != null, "No delegate deserializer configured");
		this.delegate.configure(configs, isKey);
		this.isKey = isKey;
	}

	@Override
	public Object deserialize(String topic, byte[] data) {
		try {
			return this.delegate.deserialize(topic, data);
		}
		catch (Exception e) {
			return new DeserializationException("Failed to deserialize", data, this.isKey, e);
		}
	}

	@Override
	public void close() {
		this.delegate.close();
	}

	@Override
	public Object deserialize(String topic, Headers headers, byte[] data) {
		try {
			return this.delegate.deserialize(topic, headers, data);
		}
		catch (Exception e) {
			return new DeserializationException("Failed to deserialize", data, this.isKey, e);
		}
	}

}
