/*
 * Copyright 2019-present the original author or authors.
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

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.kafka.common.TopicPartition;
import org.jspecify.annotations.Nullable;

/**
 * Manages the {@link ConsumerSeekAware.ConsumerSeekCallback} s for the listener. If the
 * listener subclasses this class, it can easily seek arbitrary topics/partitions without
 * having to keep track of the callbacks itself.
 *
 * @author Gary Russell
 * @author Borahm Lee
 * @since 2.3
 *
 */
public abstract class AbstractConsumerSeekAware implements ConsumerSeekAware {

	private final Map<Thread, ConsumerSeekCallback> callbackForThread = new ConcurrentHashMap<>();

	private final Map<TopicPartition, List<ConsumerSeekCallback>> topicToCallbacks = new ConcurrentHashMap<>();

	private final Map<ConsumerSeekCallback, List<TopicPartition>> callbackToTopics = new ConcurrentHashMap<>();

	@Override
	public void registerSeekCallback(ConsumerSeekCallback callback) {
		this.callbackForThread.put(Thread.currentThread(), callback);
	}

	@Override
	public void onPartitionsAssigned(Map<TopicPartition, Long> assignments, ConsumerSeekCallback callback) {
		ConsumerSeekCallback threadCallback = this.callbackForThread.get(Thread.currentThread());
		if (threadCallback != null) {
			assignments.keySet().forEach(tp -> {
				this.topicToCallbacks.computeIfAbsent(tp, key -> new ArrayList<>()).add(threadCallback);
				this.callbackToTopics.computeIfAbsent(threadCallback, key -> new LinkedList<>()).add(tp);
			});
		}
	}

	@Override
	public void onPartitionsRevoked(@Nullable Collection<TopicPartition> partitions) {
		if (partitions != null) {
			partitions.forEach(tp -> {
				List<ConsumerSeekCallback> removedCallbacks = this.topicToCallbacks.remove(tp);
				if (removedCallbacks != null && !removedCallbacks.isEmpty()) {
					removedCallbacks.forEach(cb -> {
						List<TopicPartition> topics = this.callbackToTopics.get(cb);
						if (topics != null) {
							topics.remove(tp);
							if (topics.isEmpty()) {
								this.callbackToTopics.remove(cb);
							}
						}
					});
				}
			});
		}
	}

	@Override
	public void unregisterSeekCallback() {
		this.callbackForThread.remove(Thread.currentThread());
	}

	/**
     * Return the callbacks for the specified topic/partition.
     * @param topicPartition the topic/partition.
     * @return the callbacks (or null if there is no assignment).
	 * @since 3.3
	 */
	@Nullable
	protected List<ConsumerSeekCallback> getSeekCallbacksFor(TopicPartition topicPartition) {
		return this.topicToCallbacks.get(topicPartition);
	}

	/**
	 * The map of callbacks for all currently assigned partitions.
	 * @return the map.
	 * @since 3.3
	*/
	protected Map<TopicPartition, List<ConsumerSeekCallback>> getTopicsAndCallbacks() {
		return Collections.unmodifiableMap(this.topicToCallbacks);
	}

	/**
	 * Return the currently registered callbacks and their associated {@link TopicPartition}(s).
	 * @return the map of callbacks and partitions.
	 * @since 2.6
	 */
	protected Map<ConsumerSeekCallback, List<TopicPartition>> getCallbacksAndTopics() {
		return Collections.unmodifiableMap(this.callbackToTopics);
	}

	/**
	 * Seek all assigned partitions to the beginning.
	 * @since 2.6
	 */
	public void seekToBeginning() {
		getCallbacksAndTopics().forEach(ConsumerSeekCallback::seekToBeginning);
	}

	/**
	 * Seek all assigned partitions to the end.
	 * @since 2.6
	 */
	public void seekToEnd() {
		getCallbacksAndTopics().forEach(ConsumerSeekCallback::seekToEnd);
	}

	/**
	 * Seek all assigned partitions to the offset represented by the timestamp.
	 * @param time the time to seek to.
	 * @since 2.6
	 */
	public void seekToTimestamp(long time) {
		getCallbacksAndTopics().forEach((cb, topics) -> cb.seekToTimestamp(topics, time));
	}

}
