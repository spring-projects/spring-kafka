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

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.jspecify.annotations.Nullable;

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
 * @author Chaedong Im
 *
 */
public class FilteringMessageListenerAdapter<K, V>
		extends AbstractFilteringMessageListener<K, V, MessageListener<K, V>>
		implements AcknowledgingConsumerAwareMessageListener<K, V>, FilteringAware<K, V> {

	private final boolean ackDiscarded;

	@SuppressWarnings("rawtypes")
	private static final ThreadLocal<FilterResult> LAST = new ThreadLocal<>();

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
	public void onMessage(ConsumerRecord<K, V> consumerRecord, @Nullable Acknowledgment acknowledgment,
			@Nullable Consumer<?, ?> consumer) {

		boolean filtered = filter(consumerRecord);
		LAST.set(new FilterResult<>(consumerRecord, filtered));


		if (!filtered) {
			switch (this.delegateType) {
				case ACKNOWLEDGING_CONSUMER_AWARE -> this.delegate.onMessage(consumerRecord, acknowledgment, consumer);
				case ACKNOWLEDGING -> this.delegate.onMessage(consumerRecord, acknowledgment);
				case CONSUMER_AWARE -> this.delegate.onMessage(consumerRecord, consumer);
				case SIMPLE -> this.delegate.onMessage(consumerRecord);
			}
		}
		else {
			ackFilteredIfNecessary(acknowledgment);
		}
	}

	private void ackFilteredIfNecessary(@Nullable Acknowledgment acknowledgment) {
		switch (this.delegateType) {
			case ACKNOWLEDGING_CONSUMER_AWARE, ACKNOWLEDGING -> {
				if (this.ackDiscarded && acknowledgment != null) {
					acknowledgment.acknowledge();
				}
			}
			case CONSUMER_AWARE, SIMPLE -> {
			}
		}
	}

	@Override
	public boolean wasFiltered(ConsumerRecord<K, V> record) {
		@SuppressWarnings("unchecked")
		FilterResult<K, V> result = (FilterResult<K, V>) LAST.get();
		return result != null
				&& result.record.topic().equals(record.topic())
				&& result.record.partition() == record.partition()
				&& result.record.offset() == record.offset()
				&& result.wasFiltered;
	}

	/*
	 * Since the container uses the delegate's type to determine which method to call, we
	 * must implement them all.
	 */

	@Override
	public void onMessage(ConsumerRecord<K, V> data) {
		onMessage(data, null, null); // NOSONAR
	}

	@Override
	public void onMessage(ConsumerRecord<K, V> data, @Nullable Acknowledgment acknowledgment) {
		onMessage(data, acknowledgment, null); // NOSONAR
	}

	@Override
	public void onMessage(ConsumerRecord<K, V> data, @Nullable Consumer<?, ?> consumer) {
		onMessage(data, null, consumer);
	}

	private static class FilterResult<K, V> {

		final ConsumerRecord<K, V> record;

		final boolean wasFiltered;

		FilterResult(ConsumerRecord<K, V> record, boolean wasFiltered) {
			this.record = record;
			this.wasFiltered = wasFiltered;
		}

	}
}
