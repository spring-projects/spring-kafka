/*
 * Copyright 2015-present the original author or authors.
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

import java.time.Duration;

import org.springframework.kafka.listener.ContainerProperties.AckMode;

/**
 * Handle for acknowledging the processing of a
 * {@link org.apache.kafka.clients.consumer.ConsumerRecord}. Recipients can store the
 * reference in asynchronous scenarios, but the internal state should be assumed transient
 * (i.e. it cannot be serialized and deserialized later)
 *
 * @author Marius Bogoevici
 * @author Gary Russell
 */
public interface Acknowledgment {

	/**
	 * Invoked when the record or batch for which the acknowledgment has been created has
	 * been processed. Calling this method implies that all the previous messages in the
	 * partition have been processed already.
	 */
	void acknowledge();

	/**
	 * Negatively acknowledge the current record - discard remaining records from the poll
	 * and re-seek all partitions so that this record will be redelivered after the sleep
	 * duration. This will pause reading for the entire message listener for the specified
	 * sleep duration and is not limited to a single partition.
	 * Must be called on the consumer thread.
	 * <p>
	 * @param sleep the duration to sleep; the actual sleep time will be larger of this value
	 * and the container's {@code pollTimeout}, which defaults to 5 seconds.
	 * @since 2.8.7
	 */
	default void nack(Duration sleep) {
		throw new UnsupportedOperationException("nack(sleep) is not supported by this Acknowledgment");
	}

	/**
	 * Acknowledge the record at an index in the batch - commit the offset(s) of records
	 * in the batch up to and including the index. Requires
	 * {@link AckMode#MANUAL_IMMEDIATE}. The index must be greater than any previous
	 * partial batch acknowledgment index for this batch and in the range of the record
	 * list. This method must be called on the listener thread.
	 * @param index the index of the record to acknowledge.
	 * @since 3.0.10
	 */
	default void acknowledge(int index) {
		throw new UnsupportedOperationException("ack(index) is not supported by this Acknowledgment");
	}

	/**
	 * Negatively acknowledge the record at an index in a batch - commit the offset(s) of
	 * records before the index and re-seek the partitions so that the record at the index
	 * and subsequent records will be redelivered after the sleep duration.
	 * Must be called on the consumer thread.
	 * <p>
	 * @param index the index of the failed record in the batch.
	 * @param sleep the duration to sleep; the actual sleep time will be larger of this value
	 * and the container's {@code pollTimeout}, which defaults to 5 seconds.
	 * @since 2.8.7
	 */
	default void nack(int index, Duration sleep) {
		throw new UnsupportedOperationException("nack(index, sleep) is not supported by this Acknowledgment");
	}

	default boolean isOutOfOrderCommit() {
		return false;
	}

}
