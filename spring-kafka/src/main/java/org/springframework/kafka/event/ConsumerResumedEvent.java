/*
 * Copyright 2018-present the original author or authors.
 */

package org.springframework.kafka.event;

import java.io.Serial;
import java.util.Collection;

import org.apache.kafka.common.TopicPartition;

/**
 * An event published when a consumer is resumed.
 *
 * @author Gary Russell
 * @since 2.1.5
 *
 */
public class ConsumerResumedEvent extends KafkaEvent {

	@Serial
	private static final long serialVersionUID = 1L;

	private transient Collection<TopicPartition> partitions;

	/**
	 * Construct an instance with the provided source and partitions.
	 * @param source the container instance that generated the event.
	 * @param container the container or the parent container if the container is a child.
	 * @param partitions the partitions.
	 * @since 2.2.1
	 */
	public ConsumerResumedEvent(Object source, Object container,
			Collection<TopicPartition> partitions) {
		super(source, container);
		this.partitions = partitions;
	}

	public Collection<TopicPartition> getPartitions() {
		return this.partitions;
	}

	@Override
	public String toString() {
		return "ConsumerResumedEvent [partitions=" + this.partitions + "]";
	}

}
