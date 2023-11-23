/*
 * Copyright 2023-2023 the original author or authors.
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

package org.springframework.kafka.core;

import org.springframework.lang.Nullable;

/**
 * An exception thrown by no transaction producer available exception, when set
 * {@link DefaultKafkaProducerFactory} maxCache greater than 0.
 *
 * @author Wang Zhiyang
 * @since 3.1.1
 *
 */
public class NoProducerAvailableException extends RuntimeException {

	private static final long serialVersionUID = 1L;

	/**
	 * Constructs a new no producer available exception with the specified detail message and cause.
	 * @param message the message.
	 */
	public NoProducerAvailableException(String message) {
		this(message, null);
	}

	/**
	 * Constructs a new no producer available exception with the specified detail message and cause.
	 * @param message the message.
	 * @param cause the cause.
	 */
	public NoProducerAvailableException(String message, @Nullable Throwable cause) {
		super(message, cause);
	}

}
