/*
 * Copyright 2017 the original author or authors.
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
 * An event that is emitted when a consumer is not responding to
 * the poll; a possible indication that the broker is down.
 *
 * @author Gary Russell
 * @since 1.3.1
 *
 */
@SuppressWarnings("serial")
public class NonResponsiveConsumerEvent extends KafkaEvent {

	private final long timeSinceLastPoll;

	private final String listenerId;

	private final List<TopicPartition> topicPartitions;

	private transient Consumer<?, ?> consumer;

	public NonResponsiveConsumerEvent(Object source, long timeSinceLastPoll, String id,
			Collection<TopicPartition> topicPartitions, Consumer<?, ?> consumer) {
		super(source);
		this.timeSinceLastPoll = timeSinceLastPoll;
		this.listenerId = id;
		this.topicPartitions = topicPartitions == null ? null : new ArrayList<>(topicPartitions);
		this.consumer = consumer;
	}

	/**
	 * How long since the last poll.
	 * @return the time in milliseconds.
	 */
	public long getTimeSinceLastPoll() {
		return this.timeSinceLastPoll;
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
	 */
	public Consumer<?, ?> getConsumer() {
		return this.consumer;
	}

	@Override
	public String toString() {
		return "NonResponsiveConsumerEvent [timeSinceLastPoll="
				+ ((float) this.timeSinceLastPoll / 1000) + "s, listenerId=" + this.listenerId
				+ ", container=" + getSource()
				+ ", topicPartitions=" + this.topicPartitions + "]";
	}

}
