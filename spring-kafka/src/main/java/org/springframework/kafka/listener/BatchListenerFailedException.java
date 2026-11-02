/*
 * Copyright 2020-present the original author or authors.
 */

package org.springframework.kafka.listener;

import java.io.Serial;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.jspecify.annotations.Nullable;

import org.springframework.kafka.KafkaException;

/**
 * An exception thrown by user code to inform the framework which record in a batch has
 * failed.
 *
 * @author Gary Russell
 * @author Wang Zhiyang
 * @since 2.5
 *
 */
public class BatchListenerFailedException extends KafkaException {

	@Serial
	private static final long serialVersionUID = 1L;

	private final int index;

	private transient @Nullable ConsumerRecord<?, ?> record;

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
		return super.getMessage() + " " + (this.record != null
				? (this.record.topic() + "-" + this.record.partition() + "@" + this.record.offset())
				: ("@-" + this.index));
	}

}
