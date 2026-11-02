/*
 * Copyright 2015-present the original author or authors.
 */

package org.springframework.kafka.support;

import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.jspecify.annotations.Nullable;

/**
 * Listener for handling outbound Kafka messages. Exactly one of its methods will be invoked, depending on whether
 * the write has been acknowledged or not.
 *
 * Its main goal is to provide a stateless singleton delegate for {@link org.apache.kafka.clients.producer.Callback}s,
 * which, in all but the most trivial cases, requires creating a separate instance per message.
 *
 * @param <K> the key type.
 * @param <V> the value type.
 *
 * @author Marius Bogoevici
 * @author Gary Russell
 * @author Endika Gutiérrez
 *
 * @see org.apache.kafka.clients.producer.Callback
 */
public interface ProducerListener<K, V> {

	/**
	 * Invoked after the successful send of a message (that is, after it has been acknowledged by the broker).
	 * @param producerRecord the actual sent record
	 * @param recordMetadata the result of the successful send operation
	 */
	default void onSuccess(ProducerRecord<K, V> producerRecord, RecordMetadata recordMetadata) {
	}

	/**
	 * Invoked after an attempt to send a message has failed.
	 * @param producerRecord the failed record
	 * @param recordMetadata The metadata for the record that was sent (i.e. the partition
	 * and offset). If an error occurred, metadata will contain only valid topic and maybe
	 * the partition. If the partition is not provided in the ProducerRecord and an error
	 * occurs before partition is assigned, then the partition will be set to
	 * RecordMetadata.UNKNOWN_PARTITION.
	 * @param exception the exception thrown
	 * @since 2.6.2
	 */
	default void onError(ProducerRecord<K, V> producerRecord, @Nullable RecordMetadata recordMetadata,
			Exception exception) {
	}

}
