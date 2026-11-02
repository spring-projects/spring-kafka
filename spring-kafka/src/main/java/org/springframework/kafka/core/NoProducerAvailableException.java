/*
 * Copyright 2023-present the original author or authors.
 */

package org.springframework.kafka.core;

import java.io.Serial;

import org.springframework.kafka.KafkaException;

/**
 * Exception when no producer is available.
 *
 * @author Wang Zhiyang
 * @author Ilya Starchenko
 *
 * @since 3.2
 */
public class NoProducerAvailableException extends KafkaException {

	@Serial
	private static final long serialVersionUID = 1L;

	private final String txIdPrefix;

	/**
	 * Constructs a new no producer available exception with the specified detail message.
	 * @param message the message.
	 * @param txIdPrefix the transaction id prefix.
	 */
	public NoProducerAvailableException(String message, String txIdPrefix) {
		super(message);
		this.txIdPrefix = txIdPrefix;
	}

	/**
	 * Return the transaction id prefix that was used to create the producer and failed.
	 * @return the transaction id prefix.
	 */
	public String getTxIdPrefix() {
		return this.txIdPrefix;
	}

}
