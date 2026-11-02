/*
 * Copyright 2018-present the original author or authors.
 */

package org.springframework.kafka.listener.adapter;

import java.util.Map;

import org.jspecify.annotations.Nullable;

/**
 * A strategy for configuring which headers, if any, should be set in a reply message.
 *
 * @author Gary Russell
 * @since 2.2
 *
 */
@FunctionalInterface
public interface ReplyHeadersConfigurer {

	/**
	 * Return true if the header should be copied to the reply message.
	 * {@link org.springframework.kafka.support.KafkaHeaders#CORRELATION_ID} will not be
	 * offered; it is always copied.
	 * {@link org.springframework.messaging.MessageHeaders#ID} and
	 * {@link org.springframework.messaging.MessageHeaders#TIMESTAMP} are never copied.
	 * {@code KafkaHeaders.RECEIVED*} headers are never copied.
	 * @param headerName the header name.
	 * @param headerValue the header value.
	 * @return true to copy.
	 */
	boolean shouldCopy(String headerName, Object headerValue);

	/**
	 * A map of additional headers to add to the reply message.
	 * IMPORTANT: Any existing headers with the same name will be replaced by those
	 * returned by this method.
	 * @return the headers.
	 */
	@Nullable
	default Map<String, Object> additionalHeaders() {
		return null;
	}

}
