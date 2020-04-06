/*
 * Copyright 2016-2020 the original author or authors.
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

package org.springframework.kafka;

import org.springframework.core.NestedRuntimeException;
import org.springframework.lang.Nullable;

/**
 * The Spring Kafka specific {@link NestedRuntimeException} implementation.
 *
 * @author Gary Russell
 * @author Artem Bilan
 */
@SuppressWarnings("serial")
public class KafkaException extends NestedRuntimeException {

	/**
	 * Construct an instance with the provided properties.
	 * @param message the message.
	 */
	public KafkaException(String message) {
		super(message);
	}

	/**
	 * Construct an instance with the provided properties.
	 * @param message the message.
	 * @param cause the cause.
	 */
	public KafkaException(String message, @Nullable Throwable cause) {
		super(message, cause);
	}

}
