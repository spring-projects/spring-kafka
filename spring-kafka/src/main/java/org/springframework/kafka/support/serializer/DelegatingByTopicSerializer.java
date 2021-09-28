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

import java.util.Map;
import java.util.regex.Pattern;

import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.serialization.Serializer;

/**
 * A {@link Serializer} that delegates to other serializers based on a topic pattern.
 *
 * @author Gary Russell
 * @since 2.8
 *
 */
public class DelegatingByTopicSerializer extends DelegatingByTopicSerialization<Serializer<?>>
		implements Serializer<Object> {

	/**
	 * Construct an instance that will be configured in {@link #configure(Map, boolean)}
	 * with producer properties {@link #VALUE_SERIALIZATION_TOPIC_CONFIG} and
	 * {@link #KEY_SERIALIZATION_TOPIC_CONFIG}.
	 */
	public DelegatingByTopicSerializer() {
	}

	/**
	 * Construct an instance with the supplied mapping of topic patterns to delegate
	 * serializers.
	 * @param delegates the map of delegates.
	 * @param defaultDelegate the default to use when no topic name match.
	 */
	public DelegatingByTopicSerializer(Map<Pattern, Serializer<?>> delegates, Serializer<?> defaultDelegate) {
		super(delegates, defaultDelegate);
	}

	@Override
	public void configure(Map<String, ?> configs, boolean isKey) {
		super.configure(configs, isKey);
	}

	@Override
	protected Serializer<?> configureDelegate(Map<String, ?> configs, boolean isKey, Serializer<?> delegate) {
		delegate.configure(configs, isKey);
		return delegate;
	}

	@Override
	protected boolean isInstance(Object instance) {
		return instance instanceof Serializer;
	}

	@Override
	public byte[] serialize(String topic, Object data) {
		throw new UnsupportedOperationException();
	}

	@SuppressWarnings("unchecked")
	@Override
	public byte[] serialize(String topic, Headers headers, Object data) {
		return ((Serializer<Object>) findDelegate(topic)).serialize(topic, headers, data);
	}

}
