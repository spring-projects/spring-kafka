/*
 * Copyright 2016-2017 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.springframework.kafka.listener;

import java.util.Map;

import org.apache.kafka.common.TopicPartition;

/**
 * Listeners that implement this interface are provided with a
 * {@link ConsumerSeekCallback} which can be used to perform a
 * seek operation.
 *
 * @author Gary Russell
 * @since 1.1
 *
 */
public interface ConsumerSeekAware {

	/**
	 * Register the callback to use when seeking at some arbitrary time. When used with a
	 * {@code ConcurrentMessageListenerContainer} or the same listener instance in multiple
	 * containers listeners should store the callback in a {@code ThreadLocal}.
	 * @param callback the callback.
	 */
	void registerSeekCallback(ConsumerSeekCallback callback);

	/**
	 * When using group management, called when partition assignments change.
	 * @param assignments the new assignments and their current offsets.
	 * @param callback the callback to perform an initial seek after assignment.
	 */
	void onPartitionsAssigned(Map<TopicPartition, Long> assignments, ConsumerSeekCallback callback);

	/**
	 * If the container is configured to emit idle container events, this method is called
	 * when the container idle event is emitted - allowing a seek operation.
	 * @param assignments the new assignments and their current offsets.
	 * @param callback the callback to perform a seek.
	 */
	void onIdleContainer(Map<TopicPartition, Long> assignments, ConsumerSeekCallback callback);

	/**
	 * A callback that a listener can invoke to seek to a specific offset.
	 */
	interface ConsumerSeekCallback {

		/**
		 * Queue a seek operation to the consumer. The seek will occur after any pending
		 * offset commits. The consumer must be currently assigned the specified partition.
		 * @param topic the topic.
		 * @param partition the partition.
		 * @param offset the offset (absolute).
		 */
		void seek(String topic, int partition, long offset);

		/**
		 * Queue a seekToBeginning operation to the consumer. The seek will occur after
		 * any pending offset commits. The consumer must be currently assigned the
		 * specified partition.
		 * @param topic the topic.
		 * @param partition the partition.
		 */
		void seekToBeginning(String topic, int partition);

		/**
		 * Queue a seekToEnd operation to the consumer. The seek will occur after any pending
		 * offset commits. The consumer must be currently assigned the specified partition.
		 * @param topic the topic.
		 * @param partition the partition.
		 */
		void seekToEnd(String topic, int partition);

	}

}
