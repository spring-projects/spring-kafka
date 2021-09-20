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
 * A general interface for managing thread-bound resources when a {@link Consumer} is
 * available.
 *
 * @author Karol Dowbecki
 * @author Gary Russell
 * @since 2.8
 *
 */
public interface ThreadStateProcessor {

	/**
	 * Call to set up thread-bound resources which will be available for the
	 * entire duration of enclosed operation involving a {@link Consumer}.
	 *
	 * @param consumer the consumer.
	 */
	default void setupThreadState(Consumer<?, ?> consumer) {
	}

	/**
	 * Call to clear thread-bound resources which were set up in
	 * {@link #setupThreadState(Consumer)}.
	 *
	 * @param consumer the consumer.
	 */
	default void clearThreadState(Consumer<?, ?> consumer) {
	}

}
