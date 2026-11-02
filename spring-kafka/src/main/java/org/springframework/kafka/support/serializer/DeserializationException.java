/*
 * Copyright 2018-present the original author or authors.
 */

package org.springframework.kafka.support.serializer;

import org.apache.kafka.common.header.Headers;
import org.jspecify.annotations.Nullable;

import org.springframework.kafka.KafkaException;

/**
 * Exception returned in the consumer record value or key when a deserialization failure
 * occurs.
 *
 * @author Gary Russell
 * @author Artem Bilan
 * @author Ngoc Nhan
 *
 * @since 2.2
 *
 */
@SuppressWarnings("serial")
public class DeserializationException extends KafkaException {

	@Nullable
	private transient Headers headers;

	private final byte @Nullable [] data;

	private final boolean isKey;

	/**
	 * Construct an instance with the provided properties.
	 * @param message the message.
	 * @param data the data (value or key).
	 * @param isKey true if the exception occurred while deserializing the key.
	 * @param cause the cause.
	 */
	public DeserializationException(String message, byte @Nullable [] data, boolean isKey, @Nullable Throwable cause) { // NOSONAR array reference
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
	public byte @Nullable [] getData() {
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
