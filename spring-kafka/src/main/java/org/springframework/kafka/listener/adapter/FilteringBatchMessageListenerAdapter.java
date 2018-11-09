/*
 * Copyright 2016-2017 the original author or authors.
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

package org.springframework.kafka.listener.adapter;

import java.util.Iterator;
import java.util.List;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import org.springframework.kafka.listener.BatchAcknowledgingConsumerAwareMessageListener;
import org.springframework.kafka.listener.BatchMessageListener;
import org.springframework.kafka.listener.ListenerType;
import org.springframework.kafka.support.Acknowledgment;

/**
 * A {@link BatchMessageListener} adapter that implements filter logic
 * via a {@link RecordFilterStrategy}.
 *
 * @param <K> the key type.
 * @param <V> the value type.
 *
 * @author Gary Russell
 *
 */
public class FilteringBatchMessageListenerAdapter<K, V>
		extends AbstractFilteringMessageListener<K, V, BatchMessageListener<K, V>>
		implements BatchAcknowledgingConsumerAwareMessageListener<K, V> {

	private final boolean ackDiscarded;

	/**
	 * Create an instance with the supplied strategy and delegate listener.
	 * @param delegate the delegate.
	 * @param recordFilterStrategy the filter.
	 */
	public FilteringBatchMessageListenerAdapter(BatchMessageListener<K, V> delegate,
			RecordFilterStrategy<K, V> recordFilterStrategy) {
		super(delegate, recordFilterStrategy);
		this.ackDiscarded = false;
	}

	/**
	 * Create an instance with the supplied strategy and delegate listener.
	 * When 'ackDiscarded' is false, and all messages are filtered, an empty list
	 * is passed to the delegate (so it can decide whether or not to ack); when true, a
	 * completely filtered batch is ack'd by this class, and no call is made to the delegate.
	 * @param delegate the delegate.
	 * @param recordFilterStrategy the filter.
	 * @param ackDiscarded true to ack (commit offset for) discarded messages when the
	 * listener is configured for manual acks.
	 */
	public FilteringBatchMessageListenerAdapter(BatchMessageListener<K, V> delegate,
			RecordFilterStrategy<K, V> recordFilterStrategy, boolean ackDiscarded) {
		super(delegate, recordFilterStrategy);
		this.ackDiscarded = ackDiscarded;
	}

	@Override
	public void onMessage(List<ConsumerRecord<K, V>> consumerRecords, Acknowledgment acknowledgment,
			Consumer<?, ?> consumer) {
		Iterator<ConsumerRecord<K, V>> iterator = consumerRecords.iterator();
		while (iterator.hasNext()) {
			if (filter(iterator.next())) {
				iterator.remove();
			}
		}
		boolean consumerAware = this.delegateType.equals(ListenerType.ACKNOWLEDGING_CONSUMER_AWARE)
						|| this.delegateType.equals(ListenerType.CONSUMER_AWARE);
		/*
		 *  An empty list goes to the listener if ackDiscarded is false and the listener can ack
		 *  either through the acknowledgment
		 */
		if (consumerRecords.size() > 0 || consumerAware
				|| (!this.ackDiscarded && this.delegateType.equals(ListenerType.ACKNOWLEDGING))) {
			invokeDelagate(consumerRecords, acknowledgment, consumer);
		}
		else {
			if (this.ackDiscarded && acknowledgment != null) {
				acknowledgment.acknowledge();
			}
		}
	}

	private void invokeDelagate(List<ConsumerRecord<K, V>> consumerRecords, Acknowledgment acknowledgment,
			Consumer<?, ?> consumer) {
		switch (this.delegateType) {
			case ACKNOWLEDGING_CONSUMER_AWARE:
				this.delegate.onMessage(consumerRecords, acknowledgment, consumer);
				break;
			case ACKNOWLEDGING:
				this.delegate.onMessage(consumerRecords, acknowledgment);
				break;
			case CONSUMER_AWARE:
				this.delegate.onMessage(consumerRecords, consumer);
				break;
			case SIMPLE:
				this.delegate.onMessage(consumerRecords);
		}
	}

	/*
	 * Since the container uses the delegate's type to determine which method to call, we
	 * must implement them all.
	 */

	@Override
	public void onMessage(List<ConsumerRecord<K, V>> data) {
		onMessage(data, null, null);
	}

	@Override
	public void onMessage(List<ConsumerRecord<K, V>> data, Acknowledgment acknowledgment) {
		onMessage(data, acknowledgment, null);
	}

	@Override
	public void onMessage(List<ConsumerRecord<K, V>> data, Consumer<?, ?> consumer) {
		onMessage(data, null, consumer);
	}

}
