/*
 * Copyright 2015-present the original author or authors.
 */

package org.springframework.kafka.listener;

import java.util.List;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.jspecify.annotations.Nullable;

import org.springframework.kafka.support.Acknowledgment;

/**
 * Listener for handling a batch of incoming Kafka messages; the list
 * is created from the consumer records object returned by a poll.
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
public interface BatchMessageListener<K, V> extends GenericMessageListener<List<ConsumerRecord<K, V>>> {

	/**
	 * Listener receives the original {@link ConsumerRecords} object instead of a
	 * list of {@link ConsumerRecord}.
	 * @param records the records.
	 * @param acknowledgment the acknowledgment (null if not manual acks)
	 * @param consumer the consumer.
	 * @since 2.2
	 */
	default void onMessage(ConsumerRecords<K, V> records, @Nullable Acknowledgment acknowledgment,
			Consumer<K, V> consumer) {
		throw new UnsupportedOperationException("This batch listener doesn't support ConsumerRecords");
	}

	/**
	 * Return true if this listener wishes to receive the original {@link ConsumerRecords}
	 * object instead of a list of {@link ConsumerRecord}.
	 * @return true for consumer records.
	 * @since 2.2
	 */
	default boolean wantsPollResult() {
		return false;
	}

}
