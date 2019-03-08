/*
 * Copyright 2018-2019 the original author or authors.
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

package org.springframework.kafka.listener;

import java.util.List;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;

/**
 * Invoked by a listener container with remaining, unprocessed, records
 * (including the failed record). Implementations should seek the desired
 * topics/partitions so that records will be re-fetched on the next
 * poll. When used with a batch listener, the entire batch of records is
 * provided.
 *
 * @param <K> the key type.
 * @param <V> the value type.
 *
 * @author Gary Russell
 *
 * @since 1.3.5
 *
 */
@FunctionalInterface
public interface AfterRollbackProcessor<K, V> {

	/**
	 * Process the remaining records. Recoverable will be true if the container is
	 * processing individual records; this allows the processor to recover (skip) the
	 * failed record rather than re-seeking it. This is not possible with a batch listener
	 * since only the listener itself knows which record in the batch keeps failing.
	 * IMPORTANT: If invoked in a transaction when the listener was invoked with a single
	 * record, the transaction id will be based on the container group.id and the
	 * topic/partition of the failed record, to avoid issues with zombie fencing. So,
	 * generally, only its offset should be sent to the transaction. For other behavior
	 * the process method should manage its own transaction.
	 * @param records the records.
	 * @param consumer the consumer.
	 * @param exception the exception
	 * @param recoverable the recoverable.
	 * @since 2.2
	 * @see #isProcessInTransaction()
	 */
	void process(List<ConsumerRecord<K, V>> records, Consumer<K, V> consumer, Exception exception, boolean recoverable);

	/**
	 * Optional method to clear thread state; will be called just before a consumer
	 * thread terminates.
	 * @since 2.2
	 */
	default void clearThreadState() {
		// NOSONAR
	}

	/**
	 * Return true to invoke {@link #process(List, Consumer, Exception, boolean)} in a new
	 * transaction. Because the container cannot infer the desired behavior, the processor
	 * is responsible for sending the offset to the transaction if it decides to skip the
	 * failing record.
	 * @return true to run in a transaction; default false.
	 * @since 2.2.5
	 * @see #process(List, Consumer, Exception, boolean)
	 */
	default boolean isProcessInTransaction() {
		return false;
	}

}
