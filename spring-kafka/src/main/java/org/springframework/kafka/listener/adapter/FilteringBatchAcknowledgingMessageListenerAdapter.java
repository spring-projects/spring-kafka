/*
 * Copyright 2016 the original author or authors.
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

import java.util.Iterator;
import java.util.List;

import org.apache.kafka.clients.consumer.ConsumerRecord;

import org.springframework.kafka.listener.BatchAcknowledgingMessageListener;
import org.springframework.kafka.support.Acknowledgment;

/**
 * A {@link BatchAcknowledgingMessageListener} adapter that implements filter logic via a
 * {@link RecordFilterStrategy}. Note that any discarded records will be ack'd when the
 * delegate listener invokes {@code acknowledge()} on the {@link Acknowledgment}.
 *
 * @param <K> the key type.
 * @param <V> the value type.
 *
 * @author Gary Russell
 *
 */
public class FilteringBatchAcknowledgingMessageListenerAdapter<K, V>
		extends AbstractFilteringMessageListener<K, V, BatchAcknowledgingMessageListener<K, V>>
		implements BatchAcknowledgingMessageListener<K, V> {

	private final boolean ackDiscarded;

	/**
	 * Create an instance with the supplied strategy and delegate listener; when all
	 * messages in a batch are discarded, an empty list is passed to the delegate so it
	 * can decide whether or not to acknowledge.
	 * @param delegate the delegate.
	 * @param recordFilterStrategy the filter.
	 */
	public FilteringBatchAcknowledgingMessageListenerAdapter(BatchAcknowledgingMessageListener<K, V> delegate,
			RecordFilterStrategy<K, V> recordFilterStrategy) {
		this(delegate, recordFilterStrategy, false);
	}

	/**
	 * Create an instance with the supplied strategy, delegate listener and discard
	 * action. When 'ackDiscarded' is false, and all messages are filtered, an empty list
	 * is passed to the delegate (so it can decided whether or not to ack); when true, a
	 * completely filtered batch is ack'd by this class, and no call is made to the delegate.
	 * @param delegate the delegate.
	 * @param recordFilterStrategy the filter.
	 * @param ackDiscarded true to ack automatically if all records are filtered.
	 */
	public FilteringBatchAcknowledgingMessageListenerAdapter(BatchAcknowledgingMessageListener<K, V> delegate,
			RecordFilterStrategy<K, V> recordFilterStrategy, boolean ackDiscarded) {
		super(delegate, recordFilterStrategy);
		this.ackDiscarded = ackDiscarded;
	}

	@Override
	public void onMessage(List<ConsumerRecord<K, V>> consumerRecords, Acknowledgment acknowledgment) {
		Iterator<ConsumerRecord<K, V>> iterator = consumerRecords.iterator();
		while (iterator.hasNext()) {
			if (filter(iterator.next())) {
				iterator.remove();
			}
		}
		if (consumerRecords.size() > 0 || !this.ackDiscarded) {
			this.delegate.onMessage(consumerRecords, acknowledgment);
		}
		else {
			acknowledgment.acknowledge();
		}
	}

}
