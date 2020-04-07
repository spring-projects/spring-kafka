/*
 * Copyright 2020 the original author or authors.
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

import org.apache.kafka.clients.consumer.ConsumerRecord;

import org.springframework.kafka.KafkaException;
import org.springframework.lang.Nullable;

/**
 * An exception thrown by user code to inform the framework which record in a batch has
 * failed.
 *
 * @author Gary Russell
 * @since 2.5
 *
 */
public class BatchListenerFailedException extends KafkaException {

	private static final long serialVersionUID = 1L;

	private final ConsumerRecord<?, ?> record;

	private final int index;

	/**
	 * Construct an instance with the provided properties.
	 * @param message the message.
	 * @param index the index in the batch of the failed record.
	 */
	public BatchListenerFailedException(String message, int index) {
		this(message, null, index);
	}

	/**
	 * Construct an instance with the provided properties.
	 * @param message the message.
	 * @param cause the cause.
	 * @param index the index in the batch of the failed record.
	 */
	public BatchListenerFailedException(String message, @Nullable Throwable cause, int index) {
		super(message, cause);
		this.index = index;
		this.record = null;
	}

	/**
	 * Construct an instance with the provided properties.
	 * @param message the message.
	 * @param record the failed record.
	 */
	public BatchListenerFailedException(String message, ConsumerRecord<?, ?> record) {
		this(message, null, record);
	}

	/**
	 * Construct an instance with the provided properties.
	 * @param message the message.
	 * @param cause the cause.
	 * @param record the failed record.
	 */
	public BatchListenerFailedException(String message, @Nullable Throwable cause, ConsumerRecord<?, ?> record) {
		super(message, cause);
		this.record = record;
		this.index = -1;
	}

	/**
	 * Return the failed record.
	 * @return the record.
	 */
	@Nullable
	public ConsumerRecord<?, ?> getRecord() {
		return this.record;
	}

	/**
	 * Return the index in the batch of  the failed record.
	 * @return the index.
	 */
	public int getIndex() {
		return this.index;
	}

	@Override
	public String getMessage() {
		return super.getMessage() + (this.record != null
				? (this.record.topic() + "-" + this.record.partition() + "@" + this.record.offset())
				: (" @-" + this.index));
	}

}
