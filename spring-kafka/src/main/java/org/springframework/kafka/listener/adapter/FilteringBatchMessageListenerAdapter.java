/*
 * Copyright 2016-present the original author or authors.
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
import org.jspecify.annotations.Nullable;

import org.springframework.kafka.listener.BatchAcknowledgingConsumerAwareMessageListener;
import org.springframework.kafka.listener.BatchMessageListener;
import org.springframework.kafka.listener.ListenerType;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.kafka.support.FilterAwareAcknowledgment;
import org.springframework.util.Assert;

/**
 * A {@link BatchMessageListener} adapter that implements filter logic
 * via a {@link RecordFilterStrategy}.
 *
 * @param <K> the key type.
 * @param <V> the value type.
 *
 * @author Gary Russell
 * @author Sanghyeok An
 *
 */
public class FilteringBatchMessageListenerAdapter<K, V>
		extends AbstractFilteringMessageListener<K, V, BatchMessageListener<K, V>>
		implements BatchAcknowledgingConsumerAwareMessageListener<K, V> {

	private final boolean ackDiscarded;

	private final boolean consumerAware;

	/**
	 * Create an instance with the supplied strategy and delegate listener.
	 * @param delegate the delegate.
	 * @param recordFilterStrategy the filter.
	 */
	public FilteringBatchMessageListenerAdapter(BatchMessageListener<K, V> delegate,
			RecordFilterStrategy<K, V> recordFilterStrategy) {
		this(delegate, recordFilterStrategy, false);
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
		this.consumerAware = this.delegateType.equals(ListenerType.ACKNOWLEDGING_CONSUMER_AWARE) ||
							this.delegateType.equals(ListenerType.CONSUMER_AWARE);
	}

	@Override
	public void onMessage(List<ConsumerRecord<K, V>> records, @Nullable Acknowledgment acknowledgment,
			@Nullable Consumer<?, ?> consumer) {

		final RecordFilterStrategy<K, V> recordFilterStrategy = getRecordFilterStrategy();
		final List<ConsumerRecord<K, V>> consumerRecords = recordFilterStrategy.filterBatch(records);
		Assert.state(consumerRecords != null, "filter returned null from filterBatch");

		// Mark filtered records
		if (acknowledgment instanceof FilterAwareAcknowledgment faa && consumerRecords.size() < records.size()) {
			records.stream()
				.filter(record -> !consumerRecords.contains(record))
				.forEach(faa::markFiltered);
		}

		if (recordFilterStrategy.ignoreEmptyBatch() &&
			consumerRecords.isEmpty() &&
			acknowledgment != null) {
			// All records were filtered but ignoreEmptyBatch is true
			if (acknowledgment instanceof FilterAwareAcknowledgment faa) {
				faa.markFiltered(records);
			}
			acknowledgment.acknowledge();
		}
		else if (!consumerRecords.isEmpty() || this.consumerAware
			|| (!this.ackDiscarded && this.delegateType.equals(ListenerType.ACKNOWLEDGING))) {
			invokeDelegate(consumerRecords, acknowledgment, consumer);
		}
		else {
			// All records were filtered and ignoreEmptyBatch is false
			if (acknowledgment instanceof FilterAwareAcknowledgment faa) {
				faa.markFiltered(records);
			}
			if (this.ackDiscarded && acknowledgment != null) {
				acknowledgment.acknowledge();
			}
		}
	}

	private void invokeDelegate(List<ConsumerRecord<K, V>> consumerRecords, @Nullable Acknowledgment acknowledgment,
			@Nullable Consumer<?, ?> consumer) {

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
		onMessage(data, null, null); // NOSONAR
	}

	@Override
	public void onMessage(List<ConsumerRecord<K, V>> data, @Nullable Acknowledgment acknowledgment) {
		onMessage(data, acknowledgment, null); // NOSONAR
	}

	@Override
	public void onMessage(List<ConsumerRecord<K, V>> data, @Nullable Consumer<?, ?> consumer) {
		onMessage(data, null, consumer);
	}

}
