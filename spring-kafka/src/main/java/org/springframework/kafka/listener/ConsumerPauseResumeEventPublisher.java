/*
 * Copyright 2022-present the original author or authors.
 */

package org.springframework.kafka.listener;

import java.util.Collection;

import org.apache.kafka.common.TopicPartition;

/**
 * Objects that can publish consumer pause/resume events.
 *
 * @author Gary Russell
 * @since 2.8.10
 *
 */
public interface ConsumerPauseResumeEventPublisher {

	/**
	 * Publish a consumer paused event.
	 * @param partitions the paused partitions.
	 * @param reason the reason.
	 */
	void publishConsumerPausedEvent(Collection<TopicPartition> partitions, String reason);

	/**
	 * Publish a consumer resumed event.
	 * @param partitions the resumed partitions.
	 */
	void publishConsumerResumedEvent(Collection<TopicPartition> partitions);

}
