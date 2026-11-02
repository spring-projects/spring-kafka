/*
 * Copyright 2021-present the original author or authors.
 */

package org.springframework.kafka.retrytopic;

/**
 *
 * Strategy for topic reuse when multiple, sequential retries have the same backoff
 * interval.
 *
 * <p>
 * It can be used only when the retries that have the same interval are located
 * in the end of the retry chain (it cannot be used for retries with the same
 * interval in the middle of the retry chain).
 *
 * @author João Lima
 * @since 3.0.4
 *
 */
public enum SameIntervalTopicReuseStrategy {

	/**
	 * Uses a single retry topic for sequential attempts with the same interval.
	 */
	SINGLE_TOPIC,

	/**
	 * Uses one separate topic per retry attempt.
	 */
	MULTIPLE_TOPICS

}
