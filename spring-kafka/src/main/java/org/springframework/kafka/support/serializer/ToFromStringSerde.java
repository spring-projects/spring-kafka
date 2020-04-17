/*
 * Copyright 2020 the original author or authors.
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

import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;

import org.springframework.util.Assert;

/**
 * A Serde that delegates to a {@link ToStringSerializer} and
 * {@link ParseStringDeserializer}.
 *
 * @param <T> the type.
 *
 * @author Gary Russell
 * @since 2.5
 *
 */
public class ToFromStringSerde<T> implements Serde<T> {

	private final ToStringSerializer<T> toStringSerializer;

	private final ParseStringDeserializer<T> fromStringDeserializer;

	/**
	 * Construct an instance with the provided properties.
	 * @param toStringSerializer the {@link ToStringSerializer}.
	 * @param fromStringDeserializer the {@link ParseStringDeserializer}.
	 */
	public ToFromStringSerde(ToStringSerializer<T> toStringSerializer,
			ParseStringDeserializer<T> fromStringDeserializer) {

		Assert.notNull(toStringSerializer, "'toStringSerializer' must not be null.");
		Assert.notNull(fromStringDeserializer, "'fromStringDeserializer' must not be null.");
		this.toStringSerializer = toStringSerializer;
		this.fromStringDeserializer = fromStringDeserializer;
	}

	@Override
	public void configure(Map<String, ?> configs, boolean isKey) {
		this.toStringSerializer.configure(configs, isKey);
		this.fromStringDeserializer.configure(configs, isKey);
	}

	@Override
	public Serializer<T> serializer() {
		return this.toStringSerializer;
	}

	@Override
	public Deserializer<T> deserializer() {
		return this.fromStringDeserializer;
	}

}
