/*
 * Copyright 2016-present the original author or authors.
 */

package org.springframework.kafka.listener;

import org.apache.kafka.clients.consumer.Consumer;
import org.jspecify.annotations.Nullable;

import org.springframework.kafka.support.Acknowledgment;

/**
 * Top level interface for listeners.
 *
 * @param <T> the type received by the listener.
 *
 * @author Gary Russell
 * @since 1.1
 *
 */
@FunctionalInterface
public interface GenericMessageListener<T> {

	/**
	 * Invoked with data from kafka.
	 * @param data the data to be processed.
	 */
	void onMessage(T data);

	/**
	 * Invoked with data from kafka. The default implementation throws
	 * {@link UnsupportedOperationException}.
	 * @param data the data to be processed.
	 * @param acknowledgment the acknowledgment.
	 */
	default void onMessage(T data, @Nullable Acknowledgment acknowledgment) {
		throw new UnsupportedOperationException("Container should never call this");
	}

	/**
	 * Invoked with data from kafka and provides access to the {@link Consumer}. The
	 * default implementation throws {@link UnsupportedOperationException}.
	 * @param data the data to be processed.
	 * @param consumer the consumer.
	 * @since 2.0
	 */
	default void onMessage(T data, @Nullable Consumer<?, ?> consumer) {
		throw new UnsupportedOperationException("Container should never call this");
	}

	/**
	 * Invoked with data from kafka and provides access to the {@link Consumer}. The
	 * default implementation throws {@link UnsupportedOperationException}.
	 * @param data the data to be processed.
	 * @param acknowledgment the acknowledgment.
	 * @param consumer the consumer.
	 * @since 2.0
	 */
	default void onMessage(T data, @Nullable Acknowledgment acknowledgment, @Nullable Consumer<?, ?> consumer) {
		throw new UnsupportedOperationException("Container should never call this");
	}

}
