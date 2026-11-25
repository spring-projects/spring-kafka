/*
 * Copyright and licensing information for this source code can be found in
 * the NOTICE and LICENSE files distributed with this project.
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
