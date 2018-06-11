/*
 * Copyright 2015-2017 the original author or authors.
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
import org.apache.kafka.clients.consumer.ConsumerRecords;

import org.springframework.kafka.support.Acknowledgment;
import org.springframework.lang.Nullable;

/**
 * Listener for handling a batch of incoming Kafka messages; the list
 * is created from the consumer records object returned by a poll.
 *
 * @param <K> the key type.
 * @param <V> the value type.
 *
 * @author Marius Bogoevici
 * @author Gary Russell
 *
 * @since 1.1
 */
@FunctionalInterface
public interface BatchMessageListener<K, V> extends GenericMessageListener<List<ConsumerRecord<K, V>>> {


	/**
	 * Listener receives the original {@link ConsumerRecords} object instead of a
	 * list of {@link ConsumerRecord}.
	 * @param records the records.
	 * @param acknowledgment the acknowledgment (null if not manual acks)
	 * @param consumer the consumer.
	 * @since 2.2
	 */
	default void onMessage(ConsumerRecords<K, V> records, @Nullable Acknowledgment acknowledgment,
			Consumer<K, V> consumer) {
		throw new UnsupportedOperationException("This batch listener doesn't support ConsumerRecords");
	}

	/**
	 * Return true if this listener wishes to receive the original {@link ConsumerRecords}
	 * object instead of a list of {@link ConsumerRecord}.
	 * @return true for consumer records.
	 * @since 2.2
	 */
	default boolean wantsPollResult() {
		return false;
	}

}
