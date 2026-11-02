/*
 * Copyright 2018-present the original author or authors.
 */

package org.springframework.kafka.event;

import java.io.Serial;
import java.util.Collection;

import org.apache.kafka.common.TopicPartition;
import org.jspecify.annotations.Nullable;

/**
 * An event published when a consumer is paused.
 *
 * @author Gary Russell
 * @since 2.1.5
 *
 */
public class ConsumerPausedEvent extends KafkaEvent {

	@Serial
	private static final long serialVersionUID = 1L;

	private final @Nullable String reason;

	private transient Collection<TopicPartition> partitions;

	/**
	 * Construct an instance with the provided source and partitions.
	 * @param source the container instance that generated the event.
	 * @param container the container or the parent container if the container is a child.
	 * @param partitions the partitions.
	 * @since 2.2.1
	 */
	public ConsumerPausedEvent(Object source, Object container, Collection<TopicPartition> partitions) {
		super(source, container);
		this.partitions = partitions;
		this.reason = null;
	}

	/**
	 * Construct an instance with the provided source and partitions.
	 * @param source the container instance that generated the event.
	 * @param container the container or the parent container if the container is a child.
	 * @param partitions the partitions.
	 * @param reason the reason for the pause.
	 * @since 2.8.9
	 */
	public ConsumerPausedEvent(Object source, Object container, Collection<TopicPartition> partitions,
			String reason) {

		super(source, container);
		this.partitions = partitions;
		this.reason = reason;
	}

	/**
	 * Return the paused partitions.
	 * @return the partitions.
	 */
	public Collection<TopicPartition> getPartitions() {
		return this.partitions;
	}

	@Override
	public String toString() {
		return "ConsumerPausedEvent [reason=" + this.reason + ", partitions=" + this.partitions + "]";
	}

}
