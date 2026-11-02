/*
 * Copyright 2018-present the original author or authors.
 */

package org.springframework.kafka.event;

import java.io.Serial;

import org.apache.kafka.common.TopicPartition;

/**
 * An event published when a consumer partition is resumed.
 *
 * @author Tomaz Fernandes
 * @since 2.7
 *
 */
public class ConsumerPartitionResumedEvent extends KafkaEvent {

	@Serial
	private static final long serialVersionUID = 1L;

	private final TopicPartition partition;

	/**
	 * Construct an instance with the provided source and partition.
	 * @param source the container instance that generated the event.
	 * @param container the container or the parent container if the container is a child.
	 * @param partition the partition.
	 * @since 2.7
	 */
	public ConsumerPartitionResumedEvent(Object source, Object container,
										TopicPartition partition) {
		super(source, container);
		this.partition = partition;
	}

	public TopicPartition getPartition() {
		return this.partition;
	}

	@Override
	public String toString() {
		return "ConsumerPartitionResumedEvent [partition=" + this.partition + "]";
	}

}
