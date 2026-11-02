/*
 * Copyright 2020-present the original author or authors.
 */

package org.springframework.kafka.listener;

import org.springframework.kafka.support.TopicPartitionOffset;

/**
 * A component implementing this interface can provide the next delivery attempt.
 *
 * @author Gary Russell
 * @since 2.5
 *
 */
@FunctionalInterface
public interface DeliveryAttemptAware {

	/**
	 * Return the next delivery attempt for the topic/partition/offset.
	 * @param topicPartitionOffset the topic/partition/offset.
	 * @return the next delivery attempt.
	 */
	int deliveryAttempt(TopicPartitionOffset topicPartitionOffset);

}
