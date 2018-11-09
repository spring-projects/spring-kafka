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

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import org.springframework.kafka.listener.AcknowledgingConsumerAwareMessageListener;
import org.springframework.kafka.listener.MessageListener;
import org.springframework.kafka.support.Acknowledgment;

/**
 * A {@link MessageListener} adapter that implements filter logic
 * via a {@link RecordFilterStrategy}.
 *
 * @param <K> the key type.
 * @param <V> the value type.
 *
 * @author Gary Russell
 *
 */
public class FilteringMessageListenerAdapter<K, V>
		extends AbstractFilteringMessageListener<K, V, MessageListener<K, V>>
		implements AcknowledgingConsumerAwareMessageListener<K, V> {

	private final boolean ackDiscarded;

	/**
	 * Create an instance with the supplied strategy and delegate listener.
	 * @param delegate the delegate.
	 * @param recordFilterStrategy the filter.
	 */
	public FilteringMessageListenerAdapter(MessageListener<K, V> delegate,
			RecordFilterStrategy<K, V> recordFilterStrategy) {
		super(delegate, recordFilterStrategy);
		this.ackDiscarded = false;
	}

	/**
	 * Create an instance with the supplied strategy and delegate listener.
	 * @param delegate the delegate.
	 * @param recordFilterStrategy the filter.
	 * @param ackDiscarded true to ack (commit offset for) discarded messages when the
	 * listener is configured for manual acks.
	 */
	public FilteringMessageListenerAdapter(MessageListener<K, V> delegate,
			RecordFilterStrategy<K, V> recordFilterStrategy, boolean ackDiscarded) {
		super(delegate, recordFilterStrategy);
		this.ackDiscarded = ackDiscarded;
	}

	@Override
	public void onMessage(ConsumerRecord<K, V> consumerRecord, Acknowledgment acknowledgment, Consumer<?, ?> consumer) {
		if (!filter(consumerRecord)) {
			switch (this.delegateType) {
				case ACKNOWLEDGING_CONSUMER_AWARE:
					this.delegate.onMessage(consumerRecord, acknowledgment, consumer);
					break;
				case ACKNOWLEDGING:
					this.delegate.onMessage(consumerRecord, acknowledgment);
					break;
				case CONSUMER_AWARE:
					this.delegate.onMessage(consumerRecord, consumer);
					break;
				case SIMPLE:
					this.delegate.onMessage(consumerRecord);
			}
		}
		else {
			ackFilteredIfNecessary(acknowledgment);
		}
	}

	private void ackFilteredIfNecessary(Acknowledgment acknowledgment) {
		switch (this.delegateType) {
			case ACKNOWLEDGING_CONSUMER_AWARE:
			case ACKNOWLEDGING:
				if (this.ackDiscarded && acknowledgment != null) {
					acknowledgment.acknowledge();
				}
				break;
			case CONSUMER_AWARE:
			case SIMPLE:
		}
	}

	/*
	 * Since the container uses the delegate's type to determine which method to call, we
	 * must implement them all.
	 */

	@Override
	public void onMessage(ConsumerRecord<K, V> data) {
		onMessage(data, null, null);
	}

	@Override
	public void onMessage(ConsumerRecord<K, V> data, Acknowledgment acknowledgment) {
		onMessage(data, acknowledgment, null);
	}

	@Override
	public void onMessage(ConsumerRecord<K, V> data, Consumer<?, ?> consumer) {
		onMessage(data, null, consumer);
	}

}
