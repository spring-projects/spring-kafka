/*
 * Copyright 2020 the original author or authors.
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

package org.springframework.kafka.event;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.common.TopicPartition;

/**
 * An event that is emitted when a container is no longer idle if configured to publish
 * idle events.
 *
 * @author Gary Russell
 * @since 2.6.2
 */
public class ListenerContainerNoLongerIdleEvent extends KafkaEvent {

	private static final long serialVersionUID = 1L;

	private final long idleTime;

	private final String listenerId;

	private final List<TopicPartition> topicPartitions;

	private transient Consumer<?, ?> consumer;

	/**
	 * Construct an instance with the provided arguments.
	 * @param source the container instance that generated the event.
	 * @param container the container or the parent container if the container is a child.
	 * @param idleTime how long the container was idle.
	 * @param id the container id.
	 * @param topicPartitions the topics/partitions currently assigned.
	 * @param consumer the consumer.
	 */
	public ListenerContainerNoLongerIdleEvent(Object source, Object container, long idleTime, String id,
			Collection<TopicPartition> topicPartitions, Consumer<?, ?> consumer) {

		super(source, container);
		this.idleTime = idleTime;
		this.listenerId = id;
		this.topicPartitions = topicPartitions == null ? null : new ArrayList<>(topicPartitions);
		this.consumer = consumer;
	}

	/**
	 * The TopicPartitions the container is listening to.
	 * @return the TopicPartition list.
	 */
	public Collection<TopicPartition> getTopicPartitions() {
		return this.topicPartitions == null ? null : Collections.unmodifiableList(this.topicPartitions);
	}

	/**
	 * How long the container was idle.
	 * @return the time in milliseconds.
	 */
	public long getIdleTime() {
		return this.idleTime;
	}

	/**
	 * The id of the listener (if {@code @KafkaListener}) or the container bean name.
	 * @return the id.
	 */
	public String getListenerId() {
		return this.listenerId;
	}

	/**
	 * Retrieve the consumer. Only populated if the listener is consumer-aware.
	 * Allows the listener to resume a paused consumer.
	 * @return the consumer.
	 */
	public Consumer<?, ?> getConsumer() {
		return this.consumer;
	}

	@Override
	public String toString() {
		return "ListenerContainerNoLongerIdleEvent [idleTime="
				+ ((float) this.idleTime / 1000) + "s, listenerId=" + this.listenerId // NOSONAR magic #
				+ ", container=" + getSource()
				+ ", topicPartitions=" + this.topicPartitions + "]";
	}

}
