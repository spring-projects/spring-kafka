/*
 * Copyright 2022 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
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
