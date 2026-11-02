/*
 * Copyright 2016-present the original author or authors.
 */

package org.springframework.kafka.listener;

import org.apache.kafka.clients.consumer.Consumer;

/**
 * A general interface for managing thread-bound resources when a {@link Consumer} is
 * available.
 *
 * @author Karol Dowbecki
 * @author Gary Russell
 * @since 2.8
 *
 */
public interface ThreadStateProcessor {

	/**
	 * Call to set up thread-bound resources which will be available for the
	 * entire duration of enclosed operation involving a {@link Consumer}.
	 *
	 * @param consumer the consumer.
	 */
	default void setupThreadState(Consumer<?, ?> consumer) {
	}

	/**
	 * Call to clear thread-bound resources which were set up in
	 * {@link #setupThreadState(Consumer)}.
	 *
	 * @param consumer the consumer.
	 */
	default void clearThreadState(Consumer<?, ?> consumer) {
	}

}
