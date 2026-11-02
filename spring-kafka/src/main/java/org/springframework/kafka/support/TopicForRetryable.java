/*
 * Copyright 2022-present the original author or authors.
 */

package org.springframework.kafka.support;

import org.apache.kafka.clients.admin.NewTopic;

/**
 * Marker to indicate this {@link NewTopic} is for retryable topics; admin will ignore these if
 * a regular {@link NewTopic} exist.
 *
 * @author Gary Russell
 * @since 2.8.10
 *
 */
public class TopicForRetryable extends NewTopic {

	/**
	 * Create an instance with the provided properties.
	 * @param topic the topic.
	 * @param numPartitions the partitions.
	 * @param replicationFactor the replication factor.
	 */
	public TopicForRetryable(String topic, int numPartitions, short replicationFactor) {
		super(topic, numPartitions, replicationFactor);
	}

}
