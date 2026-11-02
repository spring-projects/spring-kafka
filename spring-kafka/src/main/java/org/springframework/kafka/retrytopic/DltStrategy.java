/*
 * Copyright 2018-present the original author or authors.
 */

package org.springframework.kafka.retrytopic;

/**
 *
 * Strategies for handling DLT processing.
 *
 * @author Tomaz Fernandes
 * @since 2.7
 *
 */
public enum DltStrategy {

	/**
	 * Don't create a DLT.
	 */
	NO_DLT,

	/**
	 * Always send the message back to the DLT for reprocessing in case of failure in
	 * DLT processing.
	 */
	ALWAYS_RETRY_ON_ERROR,

	/**
	 * Fail if DLT processing throws an error.
	 */
	FAIL_ON_ERROR

}
