/*
 * Copyright 2016 the original author or authors.
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

package org.springframework.kafka.support;

import java.util.Objects;

import org.apache.kafka.common.TopicPartition;

/**
 * Identifies the current position of a TopicPartition.
 *
 * @author Artem Bilan
 */
public class TopicPartitionCurrentOffset {

	private final TopicPartition topicPartition;

	private final long currentOffset;

	/**
	 * Construct an instance.
	 * @param topicPartition the topic/partition.
	 * @param offset the offset.
	 */
	public TopicPartitionCurrentOffset(TopicPartition topicPartition, long offset) {
		this.topicPartition = topicPartition;
		this.currentOffset = offset;
	}

	public TopicPartition topicPartition() {
		return this.topicPartition;
	}

	public int partition() {
		return this.topicPartition.partition();
	}

	public String topic() {
		return this.topicPartition.topic();
	}

	public long currentOffset() {
		return this.currentOffset;
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) {
			return true;
		}
		if (o == null || getClass() != o.getClass()) {
			return false;
		}
		TopicPartitionCurrentOffset that = (TopicPartitionCurrentOffset) o;
		return Objects.equals(this.topicPartition, that.topicPartition);
	}

	@Override
	public int hashCode() {
		return this.topicPartition.hashCode();
	}

	@Override
	public String toString() {
		return "TopicPartitionInitialOffset{" +
				"topicPartition=" + this.topicPartition +
				", currentOffset=" + this.currentOffset +
				'}';
	}

}
