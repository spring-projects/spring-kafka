/*
 * Copyright 2021-present the original author or authors.
 */

package org.springframework.kafka.listener;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.jspecify.annotations.Nullable;

/**
 * Called to determine whether a record should be skipped.
 *
 * @author Gary Russell
 * @since 2.7
 */
@FunctionalInterface
public interface RecoveryStrategy {

	/**
	 * Return true if the record should be skipped because it was successfully
	 * recovered.
	 * @param record the record.
	 * @param ex the exception.
	 * @param container the container (or parent if a child container).
	 * @param consumer the consumer.
	 * @return true to skip.
	 * @throws InterruptedException if the thread is interrupted.
	 */
	boolean recovered(ConsumerRecord<?, ?> record, @Nullable Exception ex, @Nullable MessageListenerContainer container,
			@Nullable Consumer<?, ?> consumer) throws InterruptedException;

}
