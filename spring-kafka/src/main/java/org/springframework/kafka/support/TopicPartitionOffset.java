/*
 * Copyright 2019-2020 the original author or authors.
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

import java.util.Objects;

import org.apache.kafka.common.TopicPartition;

import org.springframework.lang.Nullable;

/**
 * A configuration container to represent a topic name, partition number and, optionally,
 * an offset for it. The offset can be:
 * <ul>
 * <li>{@code null} - do nothing;</li>
 * <li>positive (including {@code 0}) - seek to EITHER the absolute offset within the
 * partition or an offset relative to the current position for this consumer, depending
 * on {@link #isRelativeToCurrent()}.
 * </li>
 * <li>negative - seek to EITHER the offset relative to the current last offset within
 * the partition: {@code consumer.seekToEnd() + initialOffset} OR the relative to the
 * current offset for this consumer (if any), depending on
 * {@link #isRelativeToCurrent()}.</li>
 * </ul>
 * Offsets are applied when the container is {@code start()}ed.
 * This class is used when manually assigning partitions and for deferred seek operations.
 *
 * @author Artem Bilan
 * @author Gary Russell
 *
 * @since 2.3
 */
public class TopicPartitionOffset {

	/**
	 * Enumeration for "special" seeks.
	 */
	public enum SeekPosition {

		/**
		 * Seek to the beginning.
		 */
		BEGINNING,

		/**
		 * Seek to the end.
		 */
		END,

		/**
		 * Seek to the time stamp.
		 */
		TIMESTAMP

	}

	private final TopicPartition topicPartition;

	private final SeekPosition position;

	private Long offset;

	private boolean relativeToCurrent;

	/**
	 * Construct an instance with no initial offset management.
	 * @param topic the topic.
	 * @param partition the partition.
	 */
	public TopicPartitionOffset(String topic, int partition) {
		this(topic, partition, null, false);
	}

	/**
	 * Construct an instance with the provided initial offset with
	 * {@link #isRelativeToCurrent()} false.
	 * @param topic the topic.
	 * @param partition the partition.
	 * @param offset the offset.
	 * @see #TopicPartitionOffset(String, int, Long, boolean)
	 */
	public TopicPartitionOffset(String topic, int partition, Long offset) {
		this(topic, partition, offset, false);
	}

	/**
	 * Construct an instance with the provided initial offset.
	 * @param topic the topic.
	 * @param partition the partition.
	 * @param offset the initial offset.
	 * @param relativeToCurrent true for the initial offset to be relative to
	 * the current consumer position, false for a positive initial offset to
	 * be absolute and a negative offset relative to the current end of the
	 * partition.
	 */
	public TopicPartitionOffset(String topic, int partition, Long offset, boolean relativeToCurrent) {
		this.topicPartition = new TopicPartition(topic, partition);
		this.offset = offset;
		this.relativeToCurrent = relativeToCurrent;
		this.position = null;
	}

	/**
	 * Construct an instance with the provided {@link SeekPosition}.
	 * @param topic the topic.
	 * @param partition the partition.
	 * @param position {@link SeekPosition}.
	 */
	public TopicPartitionOffset(String topic, int partition, SeekPosition position) {
		this.topicPartition = new TopicPartition(topic, partition);
		this.offset = null;
		this.relativeToCurrent = false;
		this.position = position;
	}

	/**
	 * Construct an instance with the provided {@link SeekPosition}.
	 * @param topic the topic.
	 * @param partition the partition.
	 * @param offset the offset from the seek position (or timestamp for
	 * {@link SeekPosition#TIMESTAMP}).
	 * @param position {@link SeekPosition}.
	 * @since 2.3
	 */
	public TopicPartitionOffset(String topic, int partition, Long offset, @Nullable SeekPosition position) {
		this(new TopicPartition(topic, partition), offset, position);
	}

	/**
	 * Construct an instance with the provided {@link SeekPosition}.
	 * @param topicPartition the topic/partition.
	 * @param offset the offset from the seek position (or timestamp for
	 * {@link SeekPosition#TIMESTAMP}).
	 * @param position {@link SeekPosition}.
	 * @since 2.3
	 */
	public TopicPartitionOffset(TopicPartition topicPartition, Long offset, @Nullable SeekPosition position) {
		this.topicPartition = topicPartition;
		this.offset = offset;
		this.relativeToCurrent = false;
		this.position = position;
	}

	public TopicPartition getTopicPartition() {
		return this.topicPartition;
	}

	public int getPartition() {
		return this.topicPartition.partition();
	}

	public String getTopic() {
		return this.topicPartition.topic();
	}

	public Long getOffset() {
		return this.offset;
	}

	/**
	 * Set the offset.
	 * @param offset the offset.
	 * @since 2.5.5
	 */
	public void setOffset(Long offset) {
		this.offset = offset;
	}

	public boolean isRelativeToCurrent() {
		return this.relativeToCurrent;
	}

	/**
	 * Set whether the offset is relative to the current position.
	 * @param relativeToCurrent true for relative to current.
	 * @since 2.5.5
	 */
	public void setRelativeToCurrent(boolean relativeToCurrent) {
		this.relativeToCurrent = relativeToCurrent;
	}

	public SeekPosition getPosition() {
		return this.position;
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) {
			return true;
		}
		if (o == null || getClass() != o.getClass()) {
			return false;
		}
		TopicPartitionOffset that = (TopicPartitionOffset) o;
		return Objects.equals(this.topicPartition, that.topicPartition)
				&& Objects.equals(this.position, that.position);
	}

	@Override
	public int hashCode() {
		return this.topicPartition.hashCode() + this.position.hashCode();
	}

	@Override
	public String toString() {
		return "TopicPartitionOffset{" +
				"topicPartition=" + this.topicPartition +
				", offset=" + this.offset +
				", relativeToCurrent=" + this.relativeToCurrent +
				(this.position == null ? "" : (", position=" + this.position.name())) +
				'}';
	}

}
