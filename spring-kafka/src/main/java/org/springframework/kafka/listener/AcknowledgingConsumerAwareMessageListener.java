/*
 * Copyright 2017-present the original author or authors.
 */

package org.springframework.kafka.listener;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.jspecify.annotations.Nullable;

import org.springframework.kafka.support.Acknowledgment;

/**
 * Listener for handling incoming Kafka messages, propagating an acknowledgment handle that recipients
 * can invoke when the message has been processed. Access to the {@link Consumer} is provided.
 *
 * @param <K> the key type.
 * @param <V> the value type.
 *
 * @author Gary Russell
 *
 * @since 2.0
 */
@FunctionalInterface
public interface AcknowledgingConsumerAwareMessageListener<K, V> extends MessageListener<K, V> {

	/**
	 * Invoked with data from kafka. Containers should never call this since they
	 * will detect that we are a consumer aware acknowledging listener.
	 * @param data the data to be processed.
	 */
	@Override
	default void onMessage(ConsumerRecord<K, V> data) {
		throw new UnsupportedOperationException("Container should never call this");
	}

	@Override
	void onMessage(ConsumerRecord<K, V> data, @Nullable Acknowledgment acknowledgment, @Nullable Consumer<?, ?> consumer);

}
