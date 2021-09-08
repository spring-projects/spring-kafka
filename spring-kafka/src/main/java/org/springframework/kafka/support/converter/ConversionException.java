/*
 * Copyright 2016-2021 the original author or authors.
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

package org.springframework.kafka.support.converter;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.kafka.clients.consumer.ConsumerRecord;

import org.springframework.kafka.KafkaException;
import org.springframework.lang.Nullable;
import org.springframework.messaging.Message;

/**
 * Exception for conversions.
 *
 * @author Gary Russell
 *
 */
@SuppressWarnings("serial")
public class ConversionException extends KafkaException {

	private transient ConsumerRecord<?, ?> record;

	private transient List<ConsumerRecord<?, ?>> records = new ArrayList<>();

	private transient Message<?> message;

	/**
	 * Construct an instance with the provided properties.
	 * @param message A text message describing the reason.
	 * @param cause the cause.
	 */
	public ConversionException(String message, Throwable cause) {
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
		this.records.addAll(records);
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
