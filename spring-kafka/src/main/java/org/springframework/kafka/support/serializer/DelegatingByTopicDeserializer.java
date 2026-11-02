/*
 * Copyright 2019-present the original author or authors.
 */

package org.springframework.kafka.support.serializer;

import java.nio.ByteBuffer;
import java.util.Map;
import java.util.regex.Pattern;

import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.serialization.Deserializer;
import org.jspecify.annotations.Nullable;

/**
 * A {@link Deserializer} that delegates to other deserializers based on the topic name.
 *
 * @author Gary Russell
 * @author Wang Zhiyang
 * @author Ngoc Nhan
 *
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
	public @Nullable Object deserialize(String topic, byte @Nullable [] data) {
		return findDelegate(topic).deserialize(topic, data);
	}

	@Override
	public @Nullable Object deserialize(String topic, Headers headers, byte @Nullable [] data) {
		return findDelegate(topic).deserialize(topic, headers, data);
	}

	@Override
	public @Nullable Object deserialize(String topic, Headers headers, @Nullable ByteBuffer data) {
		return findDelegate(topic).deserialize(topic, headers, data);
	}

}
