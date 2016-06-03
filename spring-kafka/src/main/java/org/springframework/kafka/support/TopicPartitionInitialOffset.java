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
 * A configuration container to represent topic name, partition number and initial offset for it.
 * The initial offset can be:
 * - {@code null} - do nothing; rely on the {@code auto.offset.reset} of {@code Consumer} config;
 * - positive (including {@code 0}) - absolute offset from the begging of partition;
 * - negative - the offset from the end of partition: {@code consumer.seekToEnd() - initialOffset};
 *
 * @author Artem Bilan
 */
public class TopicPartitionInitialOffset {

	private final TopicPartition topicPartition;

	private final Long initialOffset;

	public TopicPartitionInitialOffset(String topic, int partition) {
		this(topic, partition, null);
	}

	public TopicPartitionInitialOffset(String topic, int partition, Long initialOffset) {
		this.topicPartition = new TopicPartition(topic, partition);
		this.initialOffset = initialOffset;
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

	public Long initialOffset() {
		return this.initialOffset;
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) {
			return true;
		}
		if (o == null || getClass() != o.getClass()) {
			return false;
		}
		TopicPartitionInitialOffset that = (TopicPartitionInitialOffset) o;
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
				", initialOffset=" + this.initialOffset +
				'}';
	}

}
