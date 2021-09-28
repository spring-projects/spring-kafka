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
import org.apache.kafka.common.serialization.Deserializer;

/**
 * A {@link Deserializer} that delegates to other deserializers based on the topic name.
 *
 * @author Gary Russell
 * @since 2.8
 *
 */
public class DelegatingByTopicDeserializer extends DelegatingByTopicSerialization<Deserializer<?>>
		implements Deserializer<Object> {

	/**
	 * Construct an instance that will be configured in {@link #configure(Map, boolean)}
	 * with consumer properties.
	 */
	public DelegatingByTopicDeserializer() {
	}

	/**
	 * Construct an instance with the supplied mapping of topic name patterns to delegate
	 * deserializers.
	 * @param delegates the map of delegates.
	 * @param defaultDelegate the default to use when no topic name match.
	 */
	public DelegatingByTopicDeserializer(Map<Pattern, Deserializer<?>> delegates, Deserializer<?> defaultDelegate) {
		super(delegates, defaultDelegate);
	}

	@Override
	public void configure(Map<String, ?> configs, boolean isKey) {
		super.configure(configs, isKey);
	}

	@Override
	protected Deserializer<?> configureDelegate(Map<String, ?> configs, boolean isKey, Deserializer<?> delegate) {
		delegate.configure(configs, isKey);
		return delegate;
	}

	@Override
	protected boolean isInstance(Object delegate) {
		return delegate instanceof Deserializer;
	}

	@Override
	public Object deserialize(String topic, byte[] data) {
		throw new UnsupportedOperationException();
	}

	@Override
	public Object deserialize(String topic, Headers headers, byte[] data) {
		return findDelegate(topic).deserialize(topic, headers, data);
	}

}
