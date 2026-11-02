/*
 * Copyright 2016-present the original author or authors.
 */

package org.springframework.kafka.support.converter;

import java.util.Map;

import org.springframework.messaging.MessageHeaders;

/**
 * Overload of message headers configurable for adding id and timestamp headers.
 *
 * @author Marius Bogoevici
 * @author Gary Russell
 * @since 1.1
 *
 */
@SuppressWarnings("serial")
public class KafkaMessageHeaders extends MessageHeaders {

	/**
	 * Construct headers with or without id and/or timestamp.
	 * @param generateId true to add an ID header.
	 * @param generateTimestamp true to add a timestamp header.
	 */
	KafkaMessageHeaders(boolean generateId, boolean generateTimestamp) {
		super(null, generateId ? null : ID_VALUE_NONE, generateTimestamp ? null : -1L);
	}

	@Override
	public Map<String, Object> getRawHeaders() { //NOSONAR - not useless, widening to public
		return super.getRawHeaders();
	}

}
