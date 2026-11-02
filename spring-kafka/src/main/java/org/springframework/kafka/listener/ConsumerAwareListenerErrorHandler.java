/*
 * Copyright 2017-present the original author or authors.
 */

package org.springframework.kafka.listener;

import org.apache.kafka.clients.consumer.Consumer;
import org.jspecify.annotations.Nullable;

import org.springframework.messaging.Message;

/**
 * An error handler that has access to the consumer. IMPORTANT: do not perform seek
 * operations on the consumer, the container won't be aware. Use a container-level error
 * handler such as the {@link DefaultErrorHandler} for such situations.
 *
 * @author Gary Russell
 * @since 2.0
 *
 */
@FunctionalInterface
public interface ConsumerAwareListenerErrorHandler extends KafkaListenerErrorHandler {

	@Override
	default Object handleError(Message<?> message, ListenerExecutionFailedException exception) {
		throw new UnsupportedOperationException("Adapter should never call this");
	}

	@Override
	Object handleError(Message<?> message, ListenerExecutionFailedException exception, @Nullable Consumer<?, ?> consumer);

}
