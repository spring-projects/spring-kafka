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

import org.apache.kafka.clients.consumer.ConsumerRecord;

import org.springframework.kafka.listener.AcknowledgingMessageListener;
import org.springframework.kafka.support.Acknowledgment;

/**
 * A {@link AcknowledgingMessageListener} adapter that implements filter logic
 * via a {@link RecordFilterStrategy}.
 *
 * @param <K> the key type.
 * @param <V> the value type.
 *
 * @author Gary Russell
 *
 */
public class FilteringAcknowledgingMessageListenerAdapter<K, V> extends AbstractFilteringMessageListener<K, V>
		implements AcknowledgingMessageListener<K, V> {

	private final AcknowledgingMessageListener<K, V> delegate;

	private final boolean ackDiscarded;

	/**
	 * Create an instance with the supplied strategy and delegate listener.
	 * @param delegate the delegate.
	 * @param recordFilterStrategy the filter.
	 * @param ackDiscarded true to ack (commit offset for) discarded messages.
	 */
	public FilteringAcknowledgingMessageListenerAdapter(AcknowledgingMessageListener<K, V> delegate,
			RecordFilterStrategy<K, V> recordFilterStrategy, boolean ackDiscarded) {
		super(recordFilterStrategy);
		this.delegate = delegate;
		this.ackDiscarded = ackDiscarded;
	}

	@Override
	public void onMessage(ConsumerRecord<K, V> consumerRecord, Acknowledgment acknowledgment) {
		if (!filter(consumerRecord)) {
			this.delegate.onMessage(consumerRecord, acknowledgment);
		}
		else {
			if (this.ackDiscarded) {
				acknowledgment.acknowledge();
			}
		}
	}

}
