/*
 * Copyright 2018-present the original author or authors.
 */

package org.springframework.kafka.listener;

import java.io.Serial;
import java.time.Instant;

import org.springframework.kafka.KafkaException;

/**
 * A {@link KafkaException} that records the timestamp
 * of when it was thrown.
 *
 * @author Tomaz Fernandes
 * @since 2.7
 */
public class TimestampedException extends KafkaException {

	@Serial
	private static final long serialVersionUID = -2544217643924234282L;

	private final long timestamp;

	/**
	 * Constructs an instance with the provided cause
	 * and the current time.
	 * @param ex the exception cause.
	 */
	public TimestampedException(Exception ex) {
		this(ex, Instant.now());
	}

	/**
	 * Creates an instance with the timestamp of when it was thrown and its cause.
	 * @param ex the exception cause.
	 * @param timestamp the millis from epoch of when the exception was thrown.
	 * @since 2.7.13
	 */
	public TimestampedException(Exception ex, long timestamp) {
		super("Exception thrown at " + Instant.ofEpochMilli(timestamp), ex);
		this.timestamp = timestamp;
	}

	/**
	 * Creates an instance with the Instant of when it was thrown and its cause.
	 * @param ex the exception cause.
	 * @param now the Instant of when the exception was thrown.
	 * @since 2.7.13
	 */
	public TimestampedException(Exception ex, Instant now) {
		super("Exception thrown at " + now, ex);
		this.timestamp = now.toEpochMilli();
	}

	public long getTimestamp() {
		return this.timestamp;
	}
}
