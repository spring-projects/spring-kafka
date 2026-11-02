/*
 * Copyright 2019-present the original author or authors.
 */

package org.springframework.kafka.listener;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.jspecify.annotations.Nullable;

/**
 * An interceptor for {@link ConsumerRecord} invoked by the listener
 * container before and after invoking the listener.
 *
 * @param <K> the key type.
 * @param <V> the value type.
 *
 * @author Gary Russell
 * @since 2.2.7
 *
 */
@FunctionalInterface
public interface RecordInterceptor<K, V> extends ThreadStateProcessor {

	/**
	 * Perform some action on the record or return a different one. If null is returned
	 * the record will be skipped. Invoked before the listener. IMPORTANT; if this method
	 * returns a different record, the topic, partition and offset must not be changed to
	 * avoid undesirable side-effects.
	 * <p>
	 * IMPORTANT: If transactions are being used, and this method throws an exception, it
	 * cannot be used with the container's {@code interceptBeforeTx} property set to true.
	 * @param record the record.
	 * @param consumer the consumer.
	 * @return the record or null.
	 * @since 2.7
	 */
	@Nullable
	ConsumerRecord<K, V> intercept(ConsumerRecord<K, V> record, Consumer<K, V> consumer);

	/**
	 * Called after the listener exits normally.
	 * @param record the record.
	 * @param consumer the consumer.
	 * @since 2.7
	 */
	default void success(ConsumerRecord<K, V> record, Consumer<K, V> consumer) {
	}

	/**
	 * Called after the listener throws an exception.
	 * @param record the record.
	 * @param exception the exception.
	 * @param consumer the consumer.
	 * @since 2.7
	 */
	default void failure(ConsumerRecord<K, V> record, Exception exception, Consumer<K, V> consumer) {
	}

	/**
	 * Called when processing the record is complete either
	 * {@link #success(ConsumerRecord, Consumer)} or
	 * {@link #failure(ConsumerRecord, Exception, Consumer)}.
	 * @param record the record.
	 * @param consumer the consumer.
	 * @since 2.8
	 */
	default void afterRecord(ConsumerRecord<K, V> record, Consumer<K, V> consumer) {
	}

}
