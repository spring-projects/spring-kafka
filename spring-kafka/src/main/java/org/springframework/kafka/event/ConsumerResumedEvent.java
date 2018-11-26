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

import org.apache.kafka.common.TopicPartition;

/**
 * An event published when a consumer is resumed.
 *
 * @author Gary Russell
 * @since 2.1.5
 *
 */
public class ConsumerResumedEvent extends KafkaEvent {

	private static final long serialVersionUID = 1L;

	private final Collection<TopicPartition> partitions;

	/**
	 * Construct an instance with the provided source and partitions.
	 * @param source the container.
	 * @param partitions the partitions.
	 */
	@Deprecated
	public ConsumerResumedEvent(Object source, Collection<TopicPartition> partitions) {
		this(source, null, partitions); // NOSONAR
	}

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
