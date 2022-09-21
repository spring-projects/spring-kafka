/*
 * Copyright 2017-2022 the original author or authors.
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

import java.util.List;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import org.springframework.lang.Nullable;

/**
 * An error handler that has access to the unprocessed records from the last poll
 * (including the failed record), the consumer, and the container.
 * The records passed to the handler will not be passed to the listener
 * (unless re-fetched if the handler performs seeks).
 *
 * @author Gary Russell
 * @since 2.1
 * @deprecated in favor of {@link CommonErrorHandler}.
 *
 */
@Deprecated(since = "2.8", forRemoval = true) // in 3.1
@FunctionalInterface
public interface ContainerAwareErrorHandler extends RemainingRecordsErrorHandler {

	@Override
	default void handle(Exception thrownException, @Nullable List<ConsumerRecord<?, ?>> records,
			Consumer<?, ?> consumer) {

		throw new UnsupportedOperationException("Container should never call this");
	}

	@Override
	void handle(Exception thrownException, @Nullable List<ConsumerRecord<?, ?>> records, Consumer<?, ?> consumer,
			MessageListenerContainer container);

}
