/*
 * Copyright 2018-present the original author or authors.
 */

package org.springframework.kafka.retrytopic;

/**
 *
 * Constants for the RetryTopic functionality.
 *
 * @author Tomaz Fernandes
 * @since 2.7
 *
 */
public abstract class RetryTopicConstants {

	/**
	 * Default suffix for retry topics.
	 */
	public static final String DEFAULT_RETRY_SUFFIX = "-retry";

	/**
	 * Default suffix for dlt.
	 */
	public static final String DEFAULT_DLT_SUFFIX = "-dlt";

	/**
	 * Default number of times the message processing should be attempted,
	 * including the first.
	 */
	public static final int DEFAULT_MAX_ATTEMPTS = 3;

	/**
	 * Constant to represent that the integer property is not set.
	 */
	public static final int NOT_SET = -1;

}
