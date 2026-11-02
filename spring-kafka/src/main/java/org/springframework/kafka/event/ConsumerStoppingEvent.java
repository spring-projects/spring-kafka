/*
 * Copyright 2018-present the original author or authors.
 */

package org.springframework.kafka.event;

import java.io.Serial;
import java.util.Collection;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.common.TopicPartition;
import org.jspecify.annotations.Nullable;

/**
 * An event published when a consumer is stopped. While it is best practice to use
 * stateless listeners, you can consume this event to clean up any thread-based resources
 * (remove ThreadLocals, destroy thread-scoped beans etc), as long as the context event
 * multicaster is not modified to use an async task executor.
 *
 * @author Gary Russell
 * @since 2.2
 *
 */
public class ConsumerStoppingEvent extends KafkaEvent {

	@Serial
	private static final long serialVersionUID = 1L;

	private transient final Consumer<?, ?> consumer;

	private transient final @Nullable Collection<TopicPartition> partitions;

	/**
	 * Construct an instance with the provided source, consumer and partitions.
	 * @param source the container instance that generated the event.
	 * @param container the container or the parent container if the container is a child.
	 * @param consumer the consumer.
	 * @param partitions the partitions.
	 * @since 2.2.1
	 */
	public ConsumerStoppingEvent(Object source, Object container,
			Consumer<?, ?> consumer, @Nullable Collection<TopicPartition> partitions) {
		super(source, container);
		this.consumer = consumer;
		this.partitions = partitions;
	}

	public Consumer<?, ?> getConsumer() {
		return this.consumer;
	}

	public @Nullable Collection<TopicPartition> getPartitions() {
		return this.partitions;
	}

	@Override
	public String toString() {
		return "ConsumerStoppingEvent [consumer=" + this.consumer + ", partitions=" + this.partitions + "]";
	}

}
