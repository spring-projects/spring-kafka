/*
 * Copyright 2016-present the original author or authors.
 */

package org.springframework.kafka.support.converter;

import java.lang.reflect.Type;
import java.util.List;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.jspecify.annotations.NonNull;
import org.jspecify.annotations.Nullable;

import org.springframework.kafka.support.Acknowledgment;
import org.springframework.messaging.Message;

/**
 * A Kafka-specific {@link Message} converter strategy.
 *
 * @author Gary Russell
 * @since 1.1
 */
public interface BatchMessageConverter extends MessageConverter {

	/**
	 * Convert a list of {@link ConsumerRecord} to a {@link Message}.
	 * @param records the records.
	 * @param acknowledgment the acknowledgment.
	 * @param consumer the consumer.
	 * @param payloadType the required payload type.
	 * @return the message.
	 */
	@NonNull
	Message<?> toMessage(List<ConsumerRecord<?, ?>> records, @Nullable Acknowledgment acknowledgment,
			@Nullable Consumer<?, ?> consumer, Type payloadType);

	/**
	 * Convert a message to a producer record.
	 * @param message the message.
	 * @param defaultTopic the default topic to use if no header found.
	 * @return the producer records.
	 */
	List<ProducerRecord<?, ?>> fromMessage(Message<?> message, String defaultTopic);

	/**
	 * Return the record converter used by this batch converter, if configured,
	 * or null.
	 * @return the converter or null.
	 * @since 2.1.5
	 */
	@Nullable
	default RecordMessageConverter getRecordMessageConverter() {
		return null;
	}

}
