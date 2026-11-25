/*
 * Copyright and licensing information for this source code can be found in
 * the NOTICE and LICENSE files distributed with this project.
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
