/*
 * Copyright 2019 the original author or authors.
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
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.kafka.common.TopicPartition;

import org.springframework.lang.Nullable;

/**
 * Manages the {@link ConsumerSeekAware.ConsumerSeekCallback} s for the listener. If the
 * listener subclasses this class, it can easily seek arbitrary topics/partitions without
 * having to keep track of the callbacks itself.
 *
 * @author Gary Russell
 * @since 2.3
 *
 */
public abstract class AbstractConsumerSeekAware implements ConsumerSeekAware {

	private final ThreadLocal<ConsumerSeekCallback> callbackForThread = new ThreadLocal<>();

	private final Map<TopicPartition, ConsumerSeekCallback> callbacks = new ConcurrentHashMap<>();

	@Override
	public void registerSeekCallback(ConsumerSeekCallback callback) {
		this.callbackForThread.set(callback);
	}

	@Override
	public void onPartitionsAssigned(Map<TopicPartition, Long> assignments, ConsumerSeekCallback callback) {
		ConsumerSeekCallback threadCallback = this.callbackForThread.get();
		if (threadCallback != null) {
			assignments.keySet().forEach(tp -> this.callbacks.put(tp, threadCallback));
		}
	}

	@Override
	public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
		partitions.forEach(tp -> this.callbacks.remove(tp));
	}

	@Override
	public void unregisterSeekCallback() {
		this.callbackForThread.remove();
	}

	/**
	 * Return the callback for the specified topic/partition.
	 * @param topicPartition the topic/partition.
	 * @return the callback (or null if there is no assignment).
	 */
	@Nullable
	protected ConsumerSeekCallback getSeekCallbackFor(TopicPartition topicPartition) {
		return this.callbacks.get(topicPartition);
	}

	/**
	 * The map of callbacks for all currently assigned partitions.
	 * @return the map.
	 */
	protected Map<TopicPartition, ConsumerSeekCallback> getSeekCallbacks() {
		return Collections.unmodifiableMap(this.callbacks);
	}

}
