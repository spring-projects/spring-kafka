/*
 * Copyright 2019-present the original author or authors.
 */

package org.springframework.kafka.streams.messaging;

import org.springframework.messaging.Message;

/**
 * A function that receives a spring-messaging {@link Message} and returns
 * a {@link Message}.
 *
 * @author Gary Russell
 * @since 2.3
 *
 */
@FunctionalInterface
public interface MessagingFunction {

	Message<?> exchange(Message<?> message);

}
