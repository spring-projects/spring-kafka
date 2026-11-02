/*
 * Copyright 2017-present the original author or authors.
 */

package org.springframework.kafka.support;

import java.util.Map;

import org.apache.kafka.common.header.Headers;

import org.springframework.messaging.MessageHeaders;

/**
 *  Header mapper for Apache Kafka.
 *
 * @author Gary Russell
 * @since 1.3
 *
 */
public interface KafkaHeaderMapper {

	/**
	 * Map from the given {@link MessageHeaders} to the specified target headers.
	 * @param headers the abstracted MessageHeaders.
	 * @param target the native target headers.
	 */
	void fromHeaders(MessageHeaders headers, Headers target);

	/**
	 * Map from the given native headers to a map of headers for the eventual
	 * {@link MessageHeaders}.
	 * @param source the native headers.
	 * @param target the target headers.
	 */
	void toHeaders(Headers source, Map<String, Object> target);

}
