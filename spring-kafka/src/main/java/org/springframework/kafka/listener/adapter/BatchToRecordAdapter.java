/*
 * Copyright 2020 the original author or authors.
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

package org.springframework.kafka.listener.adapter;

import java.util.List;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import org.springframework.kafka.support.Acknowledgment;
import org.springframework.messaging.Message;

/**
 * An adapter that adapts a batch listener to a record listener method. Use this, for
 * example, if you want a batch to be processed in a single transaction but wish to invoke
 * the listener with each message individually.
 *
 * @param <K> the key type.
 * @param <V> the value type.
 *
 * @author Gary Russell
 * @since 2.4.2
 *
 */
@FunctionalInterface
public interface BatchToRecordAdapter<K, V> {

	/**
	 * Adapt the list and invoke the callback for each message.
	 * @param messages the messages.
	 * @param records the records.
	 * @param ack the acknowledgment.
	 * @param consumer the consumer.
	 * @param callback the callback.
	 */
	void adapt(List<Message<?>> messages, List<ConsumerRecord<K, V>> records, Acknowledgment ack,
			Consumer<?, ?> consumer, Callback<K, V> callback);

	/**
	 * A callback for each message.
	 *
	 * @param <K> the key type.
	 * @param <V> the value type.
	 */
	@FunctionalInterface
	interface Callback<K, V> {

		/**
		 * Handle each message.
		 * @param record the record.
		 * @param ack the acknowledgment.
		 * @param consumer the consumer.
		 * @param message the message.
		 */
		void invoke(ConsumerRecord<K, V> record, Acknowledgment ack, Consumer<?, ?> consumer,
				Message<?> message);

	}

}
