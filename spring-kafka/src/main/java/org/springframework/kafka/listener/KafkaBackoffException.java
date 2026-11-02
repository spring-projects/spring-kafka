/*
 * Copyright 2018-present the original author or authors.
 */

package org.springframework.kafka.listener;

import java.io.Serial;

import org.apache.kafka.common.TopicPartition;

import org.springframework.kafka.KafkaException;

/**
 * Exception thrown when the consumer should not yet consume the message due to backOff.
 *
 * @author Tomaz Fernandes
 * @since 2.7
 */
public class KafkaBackoffException extends KafkaException {

	@Serial
	private static final long serialVersionUID = 1L;

	private final String listenerId;

	private final TopicPartition topicPartition;

	private final long dueTimestamp;

	/**
	 * Constructor with data from the BackOff event.
	 *
	 * @param message the error message.
	 * @param topicPartition the partition that was backed off.
	 * @param listenerId the listenerId for the consumer that was backed off.
	 * @param dueTimestamp the time at which the message should be consumed.
	 */
	public KafkaBackoffException(String message, TopicPartition topicPartition, String listenerId, long dueTimestamp) {
		super(message);
		this.listenerId = listenerId;
		this.topicPartition = topicPartition;
		this.dueTimestamp = dueTimestamp;
	}

	public String getListenerId() {
		return this.listenerId;
	}

	public TopicPartition getTopicPartition() {
		return this.topicPartition;
	}

	public long getDueTimestamp() {
		return this.dueTimestamp;
	}
}
