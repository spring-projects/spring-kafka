/*
 * Copyright 2016-2021 the original author or authors.
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

/**
 * An interceptor for consumer poll operation.
 *
 * @param <K> the key type.
 * @param <V> the value type.
 *
 * @author Gary Russell
 * @author Karol Dowbecki
 * @author Artem Bilan
 * @since 2.8
 *
 */
public interface BeforeAfterPollProcessor<K, V> {

	/**
	 * Called before consumer is polled.
	 * <p>
	 * It can be used to set up thread-bound resources which will be available for the
	 * entire duration of the consumer poll operation e.g. logging with MDC.
	 * </p>
	 *
	 * @param consumer the consumer.
	 */
	default void beforePoll(Consumer<K, V> consumer) {
	}

	/**
	 * Called after records were processed by listener and error handler.
	 * <p>
	 * It can be used to clear thread-bound resources which were set up in {@link #beforePoll(Consumer)}.
	 * This is the last method called by the {@link MessageListenerContainer} before the next record
	 * processing cycle starts.
	 * </p>
	 *
	 * @param consumer the consumer.
	 */
	default void clearThreadState(Consumer<K, V> consumer) {
	}

}
