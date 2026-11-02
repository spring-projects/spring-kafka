/*
 * Copyright 2017-present the original author or authors.
 */

package org.springframework.kafka.event;

import java.io.Serial;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.common.TopicPartition;
import org.jspecify.annotations.Nullable;

/**
 * An event that is emitted when a consumer is not responding to the poll; with early
 * versions of the kafka-clients, this was a possible indication that the broker is down.
 *
 * @author Gary Russell
 * @since 1.3.1
 *
 */
public class NonResponsiveConsumerEvent extends KafkaEvent {

	@Serial
	private static final long serialVersionUID = 1L;

	private final long timeSinceLastPoll;

	private final String listenerId;

	private final transient @Nullable List<TopicPartition> topicPartitions;

	private transient final Consumer<?, ?> consumer;

	/**
	 * Construct an instance with the provided properties.
	 * @param source the container instance that generated the event.
	 * @param container the container or the parent container if the container is a child.
	 * @param timeSinceLastPoll the time since the last poll.
	 * @param id the container id.
	 * @param topicPartitions the topic partitions.
	 * @param consumer the consumer.
	 * @since 2.2.1
	 */
	public NonResponsiveConsumerEvent(Object source, Object container,
			long timeSinceLastPoll, String id,
			@Nullable Collection<TopicPartition> topicPartitions, Consumer<?, ?> consumer) {

		super(source, container);
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
	public @Nullable Collection<TopicPartition> getTopicPartitions() {
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
				+ ((float) this.timeSinceLastPoll / 1000) + "s, listenerId=" + this.listenerId // NOSONAR magic #
				+ ", container=" + getSource()
				+ ", topicPartitions=" + this.topicPartitions + "]";
	}

}
