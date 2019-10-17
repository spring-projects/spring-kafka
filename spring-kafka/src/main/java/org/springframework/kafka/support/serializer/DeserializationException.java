/*
 * Copyright 2018-2019 the original author or authors.
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

	public DeserializationException(String message, byte[] data, boolean isKey, Throwable cause) {
		this(message, null, data, isKey, cause);
	}

	public DeserializationException(String message, @Nullable Headers headers, byte[] data, // NOSONAR array reference
			boolean isKey, Throwable cause) {

		super(message, cause);
		this.headers = headers;
		this.data = data; // NOSONAR array reference
		this.isKey = isKey;
	}

	@Nullable
	public Headers getHeaders() {
		return this.headers;
	}

	public void setHeaders(@Nullable Headers headers) {
		this.headers = headers;
	}

	public byte[] getData() {
		return this.data; // NOSONAR array reference
	}

	public boolean isKey() {
		return this.isKey;
	}

}
