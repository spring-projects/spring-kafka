/*
 * Copyright 2021-present the original author or authors.
 */

package org.springframework.kafka.listener;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.jspecify.annotations.Nullable;

/**
 * An interceptor for batches of records.
 *
 * @param <K> the key type.
 * @param <V> the value type.
 *
 * @author Gary Russell
 * @since 2.7
 *
 */
@FunctionalInterface
public interface BatchInterceptor<K, V> extends ThreadStateProcessor {

	/**
	 * Perform some action on the records or return a different one. If null is returned
	 * the records will be skipped. Invoked before the listener.
	 * IMPORTANT: If transactions are being used, and this method throws an exception, it
	 * cannot be used with the container's {@code interceptBeforeTx} property set to true.
	 * @param records the records.
	 * @param consumer the consumer.
	 * @return the records or null.
	 */
	@Nullable
	ConsumerRecords<K, V> intercept(ConsumerRecords<K, V> records, Consumer<K, V> consumer);

	/**
	 * Called after the listener exits normally.
	 * @param records the records.
	 * @param consumer the consumer.
	 */
	default void success(ConsumerRecords<K, V> records, Consumer<K, V> consumer) {
	}

	/**
	 * Called after the listener throws an exception.
	 * @param records the records.
	 * @param exception the exception.
	 * @param consumer the consumer.
	 */
	default void failure(ConsumerRecords<K, V> records, Exception exception, Consumer<K, V> consumer) {
	}

}
