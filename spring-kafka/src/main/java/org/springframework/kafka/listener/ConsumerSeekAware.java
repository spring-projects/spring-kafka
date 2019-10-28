/*
 * Copyright 2016-2019 the original author or authors.
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
	default void registerSeekCallback(ConsumerSeekCallback callback) {
		// do nothing
	}

	/**
	 * When using group management, called when partition assignments change.
	 * @param assignments the new assignments and their current offsets.
	 * @param callback the callback to perform an initial seek after assignment.
	 */
	default void onPartitionsAssigned(Map<TopicPartition, Long> assignments, ConsumerSeekCallback callback) {
		// do nothing
	}

	/**
	 * When using group management, called when partition assignments are revoked.
	 * Listeners should discard any callback saved from
	 * {@link #registerSeekCallback(ConsumerSeekCallback)} on this thread.
	 * @param partitions the partitions that have been revoked.
	 * @since 2.3
	 */
	default void onPartitionsRevoked(Collection<TopicPartition> partitions) {
		// do nothing
	}

	/**
	 * If the container is configured to emit idle container events, this method is called
	 * when the container idle event is emitted - allowing a seek operation.
	 * @param assignments the new assignments and their current offsets.
	 * @param callback the callback to perform a seek.
	 */
	default void onIdleContainer(Map<TopicPartition, Long> assignments, ConsumerSeekCallback callback) {
		// do nothing
	}

	/**
	 * Called when the listener consumer terminates allowing implementations to clean up
	 * state, such as thread locals.
	 * @since 2.4
	 */
	default void unregisterSeekCallback() {
		// do nothing
	}

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
		 * Queue a seekToBeginning operation to the consumer for each
		 * {@link TopicPartition}. The seek will occur after any pending offset commits.
		 * The consumer must be currently assigned the specified partition(s).
		 * @param partitions the {@link TopicPartition}s.
		 * @since 2.3.4
		 */
		default void seekToBeginning(Collection<TopicPartition> partitions) {
			throw new UnsupportedOperationException();
		}

		/**
		 * Queue a seekToEnd operation to the consumer. The seek will occur after any pending
		 * offset commits. The consumer must be currently assigned the specified partition.
		 * @param topic the topic.
		 * @param partition the partition.
		 */
		void seekToEnd(String topic, int partition);

		/**
		 * Queue a seekToEnd operation to the consumer for each {@link TopicPartition}.
		 * The seek will occur after any pending offset commits. The consumer must be
		 * currently assigned the specified partition(s).
		 * @param partitions the {@link TopicPartition}s.
		 * @since 2.3.4
		 */
		default void seekToEnd(Collection<TopicPartition> partitions) {
			throw new UnsupportedOperationException();
		}

		/**
		 * Queue a seek to a position relative to the start or end of the current position.
		 * @param topic the topic.
		 * @param partition the partition.
		 * @param offset the offset; positive values are relative to the start, negative
		 * values are relative to the end, unless toCurrent is true.
		 * @param toCurrent true for the offset to be relative to the current position rather
		 * than the beginning or end.
		 * @since 2.3
		 */
		void seekRelative(String topic, int partition, long offset, boolean toCurrent);

		/**
		 * Seek to the first offset greater than or equal to the time stamp.
		 * Use {@link #seekToTimestamp(Collection, long)} when seeking multiple partitions
		 * because the offset lookup is blocking.
		 * @param topic the topic.
		 * @param partition the partition.
		 * @param timestamp the time stamp.
		 * @since 2.3
		 * @see #seekToTimestamp(Collection, long)
		 */
		void seekToTimestamp(String topic, int partition, long timestamp);

		/**
		 * Seek to the first offset greater than or equal to the time stamp.
		 * @param topicPartitions the topic/partitions.
		 * @param timestamp the time stamp.
		 * @since 2.3
		 */
		void seekToTimestamp(Collection<TopicPartition> topicPartitions, long timestamp);

	}

}
