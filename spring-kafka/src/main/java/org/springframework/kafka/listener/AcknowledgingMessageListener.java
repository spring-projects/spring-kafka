/*
 * Copyright 2015-present the original author or authors.
 */

package org.springframework.kafka.listener;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.jspecify.annotations.Nullable;

import org.springframework.kafka.support.Acknowledgment;

/**
 * Listener for handling incoming Kafka messages, propagating an acknowledgment handle that recipients
 * can invoke when the message has been processed.
 *
 * @param <K> the key type.
 * @param <V> the value type.
 *
 * @author Marius Bogoevici
 * @author Gary Russell
 */
@FunctionalInterface
public interface AcknowledgingMessageListener<K, V> extends MessageListener<K, V> {

	/**
	 * Invoked with data from kafka. Containers should never call this since it they
	 * will detect that we are an acknowledging listener.
	 * @param data the data to be processed.
	 */
	@Override
	default void onMessage(ConsumerRecord<K, V> data) {
		throw new UnsupportedOperationException("Container should never call this");
	}

	/**
	 * Invoked with data from kafka.
	 * @param data the data to be processed.
	 * @param acknowledgment the acknowledgment.
	 */
	@Override
	void onMessage(ConsumerRecord<K, V> data, @Nullable Acknowledgment acknowledgment);

}
