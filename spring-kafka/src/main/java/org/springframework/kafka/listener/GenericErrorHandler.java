/*
 * Copyright 2016-2017 the original author or authors.
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
 * A generic error handler.
 *
 * @param <T> the data type.
 *
 * @author Gary Russell
 * @since 1.1
 *
 */
@FunctionalInterface
public interface GenericErrorHandler<T> {

	/**
	 * Handle the exception.
	 * @param thrownException The exception.
	 * @param data the data.
	 */
	void handle(Exception thrownException, T data);

	/**
	 * Handle the exception.
	 * @param thrownException The exception.
	 * @param data the data.
	 * @param consumer the consumer.
	 */
	default void handle(Exception thrownException, T data, Consumer<?, ?> consumer) {
		handle(thrownException, data);
	}

}
