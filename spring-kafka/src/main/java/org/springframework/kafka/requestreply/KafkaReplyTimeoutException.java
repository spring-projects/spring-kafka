/*
 * Copyright 2019-present the original author or authors.
 */

package org.springframework.kafka.requestreply;

import java.io.Serial;

import org.springframework.kafka.KafkaException;

/**
 * Exception when a reply is not received within a timeout.
 *
 * @author Gary Russell
 * @since 2.3
 *
 */
public class KafkaReplyTimeoutException extends KafkaException {

	@Serial
	private static final long serialVersionUID = 1L;

	public KafkaReplyTimeoutException(String message) {
		super(message);
	}

}
