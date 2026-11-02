/*
 * Copyright 2016-present the original author or authors.
 */

package org.springframework.kafka.listener.adapter;

import java.util.List;

import org.apache.kafka.clients.consumer.ConsumerRecord;

import org.springframework.kafka.listener.BatchMessageListener;

/**
 * Implementations of this interface can signal that a record about
 * to be delivered to a message listener should be discarded instead
 * of being delivered.
 *
 * @param <K> the key type.
 * @param <V> the value type.
 *
 * @author Gary Russell
 * @author Sanghyeok An
 */
public interface RecordFilterStrategy<K, V> {

	/**
	 * Return true if the record should be discarded.
	 * @param consumerRecord the record.
	 * @return true to discard.
	 */
	boolean filter(ConsumerRecord<K, V> consumerRecord);

	/**
	 * Filter an entire batch of records; to filter all records, return an empty list, not
	 * null.
	 * @param records the records.
	 * @return the filtered records.
	 * @since 2.8
	 */
	default List<ConsumerRecord<K, V>> filterBatch(List<ConsumerRecord<K, V>> records) {
		records.removeIf(this::filter);
		return records;
	}

	/**
	 * Determine whether {@link FilteringBatchMessageListenerAdapter} should invoke
	 * the {@link BatchMessageListener} when all {@link ConsumerRecord}s in a batch have been filtered out
	 * resulting in empty list. By default, do invoke the {@link BatchMessageListener} (return false).
	 * @return true for {@link FilteringBatchMessageListenerAdapter} to not invoke {@link BatchMessageListener}
	 * when all {@link ConsumerRecord} in a batch filtered out
	 * @since 3.3
	 */
	default boolean ignoreEmptyBatch() {
		return false;
	}

}
