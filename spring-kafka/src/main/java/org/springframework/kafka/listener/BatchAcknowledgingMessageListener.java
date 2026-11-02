/*
 * Copyright 2015-present the original author or authors.
 */

package org.springframework.kafka.listener;

import java.util.List;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.jspecify.annotations.Nullable;

import org.springframework.kafka.support.Acknowledgment;

/**
 * Listener for handling a batch of incoming Kafka messages, propagating an acknowledgment
 * handle that recipients can invoke when the message has been processed. The list is
 * created from the consumer records object returned by a poll.
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
public interface BatchAcknowledgingMessageListener<K, V> extends BatchMessageListener<K, V> {

	/**
	 * Invoked with data from kafka. Containers should never call this since it they
	 * will detect that we are an acknowledging listener.
	 * @param data the data to be processed.
	 */
	@Override
	default void onMessage(List<ConsumerRecord<K, V>> data) {
		throw new UnsupportedOperationException("Container should never call this");
	}

	@Override
	void onMessage(List<ConsumerRecord<K, V>> data, @Nullable Acknowledgment acknowledgment);

}
