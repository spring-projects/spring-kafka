/*
 * Copyright 2016-2019 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.springframework.kafka.support;

import org.apache.kafka.common.TopicPartition;

/**
 * See {@link TopicPartitionOffset}.
 *
 * @deprecated in favor of {@link TopicPartitionOffset}.
 *
 * @author Artem Bilan
 * @author Gary Russell
 *
 */
@Deprecated
public class TopicPartitionInitialOffset extends TopicPartitionOffset {

	public TopicPartitionInitialOffset(String topic, int partition, Long offset, boolean relativeToCurrent) {
		super(topic, partition, offset, relativeToCurrent);
	}

	public TopicPartitionInitialOffset(String topic, int partition, Long offset, SeekPosition position) {
		super(topic, partition, offset, position);
	}

	public TopicPartitionInitialOffset(String topic, int partition, Long offset) {
		super(topic, partition, offset);
	}

	public TopicPartitionInitialOffset(String topic, int partition, SeekPosition position) {
		super(topic, partition, position);
	}

	public TopicPartitionInitialOffset(String topic, int partition) {
		super(topic, partition);
	}

	public TopicPartition topicPartition() {
		return getTopicPartition();
	}

	public int partition() {
		return getPartition();
	}

	public String topic() {
		return getTopic();
	}

	public Long initialOffset() {
		return getOffset();
	}

}
