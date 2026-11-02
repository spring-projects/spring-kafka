/*
 * Copyright 2018-present the original author or authors.
 */

package org.springframework.kafka.retrytopic;

/**
 *
 * Contains the headers that will be used in the forwarded messages.
 *
 * @author Tomaz Fernandes
 * @since 2.7
 *
 */
public abstract class RetryTopicHeaders {

	/**
	 * The default header for the backoff due timestamp.
	 */
	public static final String DEFAULT_HEADER_BACKOFF_TIMESTAMP = "retry_topic-backoff-timestamp";

	/**
	 * The default header for the attempts.
	 */
	public static final String DEFAULT_HEADER_ATTEMPTS = "retry_topic-attempts";

	/**
	 * The default header for the original message's timestamp.
	 */
	public static final String DEFAULT_HEADER_ORIGINAL_TIMESTAMP = "retry_topic-original-timestamp";

}
