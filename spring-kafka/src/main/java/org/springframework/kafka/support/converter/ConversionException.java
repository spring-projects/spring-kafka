/*
 * Copyright 2016-present the original author or authors.
 */

package org.springframework.kafka.support.converter;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.jspecify.annotations.Nullable;

import org.springframework.kafka.KafkaException;
import org.springframework.messaging.Message;

/**
 * Exception for conversions.
 *
 * @author Gary Russell
 *
 */
@SuppressWarnings("serial")
public class ConversionException extends KafkaException {

	private transient @Nullable ConsumerRecord<?, ?> record;

	private transient @Nullable List<ConsumerRecord<?, ?>> records = new ArrayList<>();

	private transient @Nullable Message<?> message;

	/**
	 * Construct an instance with the provided properties.
	 * @param message A text message describing the reason.
	 * @param cause the cause.
	 */
	public ConversionException(String message, @Nullable Throwable cause) {
		super(message, cause);
		this.record = null;
		this.message = null;
	}

	/**
	 * Construct an instance with the provided properties.
	 * @param message A text message describing the reason.
	 * @param record the consumer record.
	 * @param cause the cause.
	 * @since 2.7.2
	 */
	public ConversionException(String message, ConsumerRecord<?, ?> record, Throwable cause) {
		super(message, cause);
		this.record = record;
		this.message = null;
	}

	/**
	 * Construct an instance with the provided properties.
	 * @param message A text message describing the reason.
	 * @param records the consumer records.
	 * @param cause the cause.
	 * @since 2.7.2
	 */
	public ConversionException(String message, List<ConsumerRecord<?, ?>> records, Throwable cause) {
		super(message, cause);
		this.record = null;
		if (this.records != null) {
			this.records.addAll(records);
		}
		this.message = null;
	}

	/**
	 * Construct an instance with the provided properties.
	 * @param message A text message describing the reason.
	 * @param msg a {@link Message} converted from a consumer record.
	 * @param cause the cause.
	 * @since 2.7.2
	 */
	public ConversionException(String message, Message<?> msg, Throwable cause) {
		super(message, cause);
		this.record = null;
		this.message = msg;
	}

	/**
	 * Return the consumer record, if available.
	 * @return the record.
	 * @since 2.7.2
	 */
	@Nullable
	public ConsumerRecord<?, ?> getRecord() {
		return this.record;
	}

	/**
	 * Return the consumer record, if available.
	 * @return the record.
	 * @since 2.7.2
	 */
	public List<ConsumerRecord<?, ?>> getRecords() {
		return Collections.unmodifiableList(this.records);
	}

	/**
	 * Return the {@link Message}, if available.
	 * @return the message.
	 * @since 2.7.2
	 */
	@Nullable
	public Message<?> getMsg() {
		return this.message;
	}

}
