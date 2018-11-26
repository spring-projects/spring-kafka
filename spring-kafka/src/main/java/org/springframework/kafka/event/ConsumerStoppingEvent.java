/*
 * Copyright 2018 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.springframework.kafka.event;

import java.util.Collection;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.common.TopicPartition;

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

	private static final long serialVersionUID = 1L;

	private transient Consumer<?, ?> consumer;

	private final Collection<TopicPartition> partitions;

	/**
	 * Construct an instance with the provided source, consumer and partitions.
	 * @param source the container.
	 * @param consumer the consumer.
	 * @param partitions the partitions.
	 */
	@Deprecated
	public ConsumerStoppingEvent(Object source, Consumer<?, ?> consumer, Collection<TopicPartition> partitions) {
		this(source, null, consumer, partitions); // NOSONAR
	}

	/**
	 * Construct an instance with the provided source, consumer and partitions.
	 * @param source the container instance that generated the event.
	 * @param container the container or the parent container if the container is a child.
	 * @param consumer the consumer.
	 * @param partitions the partitions.
	 * @since 2.2.1
	 */
	public ConsumerStoppingEvent(Object source, Object container,
			Consumer<?, ?> consumer, Collection<TopicPartition> partitions) {
		super(source, container);
		this.consumer = consumer;
		this.partitions = partitions;
	}

	public Consumer<?, ?> getConsumer() {
		return this.consumer;
	}

	public Collection<TopicPartition> getPartitions() {
		return this.partitions;
	}

	@Override
	public String toString() {
		return "ConsumerStoppingEvent [consumer=" + this.consumer + ", partitions=" + this.partitions + "]";
	}

}
