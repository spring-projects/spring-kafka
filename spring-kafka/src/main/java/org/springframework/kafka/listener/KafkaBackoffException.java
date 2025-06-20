/*
 * Copyright 2018-present the original author or authors.
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

package org.springframework.kafka.listener;

import org.apache.kafka.common.TopicPartition;

import org.springframework.kafka.KafkaException;

/**
 * Exception thrown when the consumer should not yet consume the message due to backOff.
 *
 * @author Tomaz Fernandes
 * @since 2.7
 */
public class KafkaBackoffException extends KafkaException {

	private static final long serialVersionUID = 1L;

	private final String listenerId;

	private final TopicPartition topicPartition;

	private final long dueTimestamp;

	/**
	 * Constructor with data from the BackOff event.
	 *
	 * @param message the error message.
	 * @param topicPartition the partition that was backed off.
	 * @param listenerId the listenerId for the consumer that was backed off.
	 * @param dueTimestamp the time at which the message should be consumed.
	 */
	public KafkaBackoffException(String message, TopicPartition topicPartition, String listenerId, long dueTimestamp) {
		super(message);
		this.listenerId = listenerId;
		this.topicPartition = topicPartition;
		this.dueTimestamp = dueTimestamp;
	}

	public String getListenerId() {
		return this.listenerId;
	}

	public TopicPartition getTopicPartition() {
		return this.topicPartition;
	}

	public long getDueTimestamp() {
		return this.dueTimestamp;
	}
}
