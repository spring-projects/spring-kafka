/*
 * Copyright 2016-present the original author or authors.
 */

package org.springframework.kafka.listener.adapter;

import org.apache.kafka.clients.consumer.ConsumerRecord;

import org.springframework.util.Assert;

/**
 * An abstract message listener adapter that implements record filter logic
 * via a {@link RecordFilterStrategy}.
 *
 * @param <K> the key type.
 * @param <V> the value type.
 * @param <T> the delegate type.
 *
 * @author Gary Russell
 *
 */
public abstract class AbstractFilteringMessageListener<K, V, T>
		extends AbstractDelegatingMessageListenerAdapter<T> {

	private final RecordFilterStrategy<K, V> recordFilterStrategy;

	protected AbstractFilteringMessageListener(T delegate, RecordFilterStrategy<K, V> recordFilterStrategy) {
		super(delegate);
		Assert.notNull(recordFilterStrategy, "'recordFilterStrategy' cannot be null");
		this.recordFilterStrategy = recordFilterStrategy;
	}

	protected RecordFilterStrategy<K, V> getRecordFilterStrategy() {
		return this.recordFilterStrategy;
	}

	protected boolean filter(ConsumerRecord<K, V> consumerRecord) {
		return this.recordFilterStrategy.filter(consumerRecord);
	}

}
