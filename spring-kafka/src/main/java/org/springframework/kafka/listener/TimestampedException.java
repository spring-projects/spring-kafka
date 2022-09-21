/*
 * Copyright 2018-2022 the original author or authors.
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
