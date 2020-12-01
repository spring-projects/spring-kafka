/*
 * Copyright 2018-2020 the original author or authors.
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

package org.springframework.kafka.support.serializer;

import org.apache.kafka.common.header.Headers;

import org.springframework.kafka.KafkaException;
import org.springframework.lang.Nullable;

/**
 * Exception returned in the consumer record value or key when a deserialization failure
 * occurs.
 *
 * @author Gary Russell
 * @author Artem Bilan
 *
 * @since 2.2
 *
 */
@SuppressWarnings("serial")
public class DeserializationException extends KafkaException {

	@Nullable
	private transient Headers headers;

	private final byte[] data;

	private final boolean isKey;

	/**
	 * Construct an instance with the provided properties.
	 * @param message the message.
	 * @param data the data (value or key).
	 * @param isKey true if the exception occurred while deserializing the key.
	 * @param cause the cause.
	 */
	public DeserializationException(String message, byte[] data, boolean isKey, Throwable cause) { // NOSONAR array reference
		super(message, cause);
		this.data = data; // NOSONAR array reference
		this.isKey = isKey;
	}

	/**
	 * Get the headers.
	 * @return the headers.
	 */
	@Nullable
	public Headers getHeaders() {
		return this.headers;
	}

	/**
	 * Set the headers.
	 * @param headers the headers.
	 */
	public void setHeaders(@Nullable Headers headers) {
		this.headers = headers;
	}

	/**
	 * Get the data that failed deserialization (value or key).
	 * @return the data.
	 */
	public byte[] getData() {
		return this.data; // NOSONAR array reference
	}

	/**
	 * True if deserialization of the key failed, otherwise deserialization of the value
	 * failed.
	 * @return true for the key.
	 */
	public boolean isKey() {
		return this.isKey;
	}

}
