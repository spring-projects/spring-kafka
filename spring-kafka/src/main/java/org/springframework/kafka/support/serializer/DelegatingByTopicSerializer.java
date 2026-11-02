/*
 * Copyright 2019-present the original author or authors.
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

	@SuppressWarnings({"unchecked", "NullAway"}) // Dataflow analysis limitation
	@Override
	public byte[] serialize(String topic, Headers headers, Object data) {
		if (data == null) {
			return null;
		}
		return ((Serializer<Object>) findDelegate(topic)).serialize(topic, headers, data);
	}

}
