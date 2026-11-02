/*
 * Copyright 2022-present the original author or authors.
 */

package org.springframework.kafka.listener;

import org.apache.kafka.clients.consumer.Consumer;
import org.jspecify.annotations.Nullable;

import org.springframework.kafka.support.Acknowledgment;
import org.springframework.messaging.Message;

/**
 * A {@link KafkaListenerErrorHandler} that supports manual acks.
 *
 * @author Gary Russell
 * @since 2.9
 *
 */
@FunctionalInterface
public interface ManualAckListenerErrorHandler extends KafkaListenerErrorHandler {

	@Override
	default Object handleError(Message<?> message, ListenerExecutionFailedException exception) {
		throw new UnsupportedOperationException("Adapter should never call this");
	}

	@Override
	Object handleError(Message<?> message, ListenerExecutionFailedException exception, @Nullable Consumer<?, ?> consumer,
			@Nullable Acknowledgment ack);

}
