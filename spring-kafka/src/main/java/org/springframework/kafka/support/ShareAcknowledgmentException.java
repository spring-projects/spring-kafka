/*
 * Copyright 2025-present the original author or authors.
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

package org.springframework.kafka.support;

import org.springframework.kafka.KafkaException;

/**
 * Exception thrown when share group record acknowledgment fails.
 * <p>
 * This exception indicates that an attempt to acknowledge a record
 * in a share group encountered an error. Common causes include:
 * <ul>
 * <li>Network issues communicating with the broker</li>
 * <li>Record state conflicts (e.g., record already acknowledged by another consumer)</li>
 * <li>Share session errors</li>
 * <li>Broker-side errors processing the acknowledgment</li>
 * </ul>
 *
 * @author Soby Chacko
 * @since 4.0
 */
public class ShareAcknowledgmentException extends KafkaException {

	/**
	 * Construct an instance with the provided message.
	 *
	 * @param message the message
	 */
	public ShareAcknowledgmentException(String message) {
		super(message);
	}

	/**
	 * Construct an instance with the provided message and cause.
	 *
	 * @param message the message
	 * @param cause the cause
	 */
	public ShareAcknowledgmentException(String message, Throwable cause) {
		super(message, cause);
	}

}
