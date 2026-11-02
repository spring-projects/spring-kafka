/*
 * Copyright 2020-present the original author or authors.
 */

package org.springframework.kafka.event;

import java.io.Serial;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.common.TopicPartition;

/**
 * An event that is emitted when a partition is no longer idle if configured to publish
 * idle events.
 *
 * @author Gary Russell
 * @author Borahm Lee
 * @since 2.6.2
 */
public class ListenerContainerPartitionNoLongerIdleEvent extends KafkaEvent {

	@Serial
	private static final long serialVersionUID = 1L;

	private final long idleTime;

	private final String listenerId;

	private final TopicPartition topicPartition;

	private transient final Consumer<?, ?> consumer;

	/**
	 * Construct an instance with the provided arguments.
	 * @param source the container instance that generated the event.
	 * @param container the container or the parent container if the container is a child.
	 * @param idleTime how long the container was idle.
	 * @param id the container id.
	 * @param topicPartition the idle topic/partition.
	 * @param consumer the consumer.
	 */
	public ListenerContainerPartitionNoLongerIdleEvent(Object source, Object container, long idleTime, String id,
													TopicPartition topicPartition, Consumer<?, ?> consumer) {

		super(source, container);
		this.idleTime = idleTime;
		this.listenerId = id;
		this.topicPartition = topicPartition;
		this.consumer = consumer;
	}

	/**
	 * The idle TopicPartition.
	 * @return the TopicPartition.
	 */
	public TopicPartition getTopicPartition() {
		return this.topicPartition;
	}

	/**
	 * How long the partition was idle.
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
		return "ListenerContainerPartitionNoLongerIdleEvent [idleTime="
				+ ((float) this.idleTime / 1000) + "s, listenerId=" + this.listenerId // NOSONAR magic #
				+ ", container=" + getSource()
				+ ", topicPartition=" + this.topicPartition + "]";
	}
}
