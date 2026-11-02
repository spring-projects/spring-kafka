/*
 * Copyright 2020-present the original author or authors.
 */

package org.springframework.kafka.listener;

import java.io.Serial;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.jspecify.annotations.Nullable;

import org.springframework.kafka.KafkaException;

/**
 * An exception thrown by a batch listener to indicate which record in the batch failed.
 * The framework will commit the offsets of all records before the failed record and seek
 * the remaining records (starting from the failed one) for redelivery.
 *
 * <p><strong>Important contract:</strong> throwing this exception carries an implicit
 * assertion that every record before the indicated index (or before the provided
 * {@link ConsumerRecord}) has been fully and irreversibly processed. The framework
 * commits those preceding offsets immediately; they will not be redelivered.
 *
 * <p>This exception is <strong>not</strong> appropriate when:
 * <ul>
 *   <li>records are processed in parallel (e.g. using virtual threads or an executor),
 *       because preceding records may still be in-flight when the exception is thrown;</li>
 *   <li>processing involves multiple steps (e.g. transform then produce to an output
 *       topic), because earlier records may have completed only some steps;</li>
 *   <li>the listener is transactional and a full rollback is required on any failure.</li>
 * </ul>
 * In those cases, throw a plain {@link RuntimeException} instead so that the entire batch
 * is redelivered.
 *
 * @author Gary Russell
 * @author Wang Zhiyang
 * @author Aditya Pal
 *
 * @since 2.5
 *
 * @see DefaultErrorHandler
 * @see FailedBatchProcessor
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
