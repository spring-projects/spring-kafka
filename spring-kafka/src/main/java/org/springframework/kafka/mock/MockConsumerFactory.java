/*
 * Copyright 2023-present the original author or authors.
 */

package org.springframework.kafka.mock;

import java.util.Collections;
import java.util.Map;
import java.util.Properties;
import java.util.function.Supplier;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.MockConsumer;
import org.jspecify.annotations.Nullable;

import org.springframework.kafka.core.ConsumerFactory;

/**
 * Support the use of {@link MockConsumer} in tests.
 *
 * @param <K> the key type.
 * @param <V> the value type.
 *
 * @author Gary Russell
 * @since 3.0.7
 *
 */
public class MockConsumerFactory<K, V> implements ConsumerFactory<K, V> {

	private final Supplier<MockConsumer<K, V>> consumerProvider;

	/**
	 * Create an instance with the supplied consumer provider.
	 * @param consumerProvider the consumer provider.
	 */
	public MockConsumerFactory(Supplier<MockConsumer<K, V>> consumerProvider) {
		this.consumerProvider = consumerProvider;
	}

	@Override
	public Map<String, Object> getConfigurationProperties() {
		return Collections.emptyMap();
	}

	@Override
	public Consumer<K, V> createConsumer(@Nullable String groupId, @Nullable String clientIdPrefix,
			@Nullable String clientIdSuffix) {

		return this.consumerProvider.get();
	}

	@Override
	public Consumer<K, V> createConsumer(@Nullable String groupId, @Nullable String clientIdPrefix,
			@Nullable String clientIdSuffix, @Nullable Properties properties) {

		return this.consumerProvider.get();
	}

	@Override
	public boolean isAutoCommit() {
		return false;
	}

}
