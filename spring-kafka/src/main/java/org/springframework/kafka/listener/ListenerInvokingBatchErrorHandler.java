/*
 * Copyright 2020-2022 the original author or authors.
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
import org.apache.kafka.clients.consumer.ConsumerRecords;

import org.springframework.lang.Nullable;

/**
 * A batch error handler that is capable of invoking the listener during error handling.
 *
 * @author Gary Russell
 * @since 2.3.7
 * @deprecated in favor of {@link CommonErrorHandler}.
 *
 */
@Deprecated(since = "2.8", forRemoval = true) // in 3.1
@FunctionalInterface
public interface ListenerInvokingBatchErrorHandler extends ContainerAwareBatchErrorHandler {

	@Override
	default void handle(Exception thrownException, @Nullable ConsumerRecords<?, ?> data, Consumer<?, ?> consumer,
			MessageListenerContainer container) {

		throw new UnsupportedOperationException("Container should never call this");
	}

	@Override
	void handle(Exception thrownException, @Nullable ConsumerRecords<?, ?> records,
			Consumer<?, ?> consumer, MessageListenerContainer container, Runnable invokeListener);

}
