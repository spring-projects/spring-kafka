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
public enum TopicSuffixingStrategy {

	/**
	 * Suffixes the topics with their index in the retry topics.
	 * E.g. my-retry-topic-0, my-retry-topic-1, my-retry-topic-2.
	 */
	SUFFIX_WITH_INDEX_VALUE,

	/**
	 * Suffixes the topics the delay value for the topic.
	 * E.g. my-retry-topic-1000, my-retry-topic-2000, my-retry-topic-4000.
	 */
	SUFFIX_WITH_DELAY_VALUE

}
