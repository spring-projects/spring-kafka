/*
 * Copyright 2017-present the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
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
