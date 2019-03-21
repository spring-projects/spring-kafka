/*
 * Copyright 2016-2017 the original author or authors.
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
 * An event that is emitted when a container is idle if the container
 * is configured to do so.
 *
 * @author Gary Russell
 *
 */
@SuppressWarnings("serial")
public class ListenerContainerIdleEvent extends KafkaEvent {

	private final long idleTime;

	private final String listenerId;

	private final List<TopicPartition> topicPartitions;

	private transient Consumer<?, ?> consumer;

	public ListenerContainerIdleEvent(Object source, long idleTime, String id,
			Collection<TopicPartition> topicPartitions, Consumer<?, ?> consumer) {
		super(source);
		this.idleTime = idleTime;
		this.listenerId = id;
		this.topicPartitions = topicPartitions == null ? null : new ArrayList<>(topicPartitions);
		this.consumer = consumer;
	}

	/**
	 * How long the container has been idle.
	 * @return the time in milliseconds.
	 */
	public long getIdleTime() {
		return this.idleTime;
	}

	/**
	 * The TopicPartitions the container is listening to.
	 * @return the TopicPartition list.
	 */
	public Collection<TopicPartition> getTopicPartitions() {
		return this.topicPartitions == null ? null : Collections.unmodifiableList(this.topicPartitions);
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
	 * @since 2.0
	 */
	public Consumer<?, ?> getConsumer() {
		return this.consumer;
	}

	@Override
	public String toString() {
		return "ListenerContainerIdleEvent [idleTime="
				+ ((float) this.idleTime / 1000) + "s, listenerId=" + this.listenerId
				+ ", container=" + getSource()
				+ ", topicPartitions=" + this.topicPartitions + "]";
	}

}
