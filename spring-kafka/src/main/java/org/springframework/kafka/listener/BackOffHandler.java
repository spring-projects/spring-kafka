/*
 * Copyright 2022-present the original author or authors.
 */

package org.springframework.kafka.listener;

import org.apache.kafka.common.TopicPartition;
import org.jspecify.annotations.Nullable;

/**
 * Handler for the provided back off time, listener container and exception.
 * Also supports back off for individual partitions.
 *
 * @author Jan Marincek
 * @author Gary Russell
 * @since 2.9
 */
public interface BackOffHandler {

	/**
	 * Perform the next back off.
	 * @param container the container.
	 * @param exception the exception.
	 * @param nextBackOff the next back off.
	 */
	default void onNextBackOff(@Nullable MessageListenerContainer container, @Nullable Exception exception, long nextBackOff) {
		throw new UnsupportedOperationException();
	}

	/**
	 * Perform the next back off for a partition.
	 * @param container the container.
	 * @param partition the partition.
	 * @param nextBackOff the next back off.
	 */
	default void onNextBackOff(MessageListenerContainer container, TopicPartition partition,
			long nextBackOff) {

		throw new UnsupportedOperationException();
	}

}
