/*
 * Copyright 2016-present the original author or authors.
 */

package org.springframework.kafka.support.converter;

import java.lang.reflect.Type;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.jspecify.annotations.NonNull;
import org.jspecify.annotations.Nullable;

import org.springframework.messaging.Message;

/**
 * A Kafka-specific {@link Message} converter strategy.
 *
 * @author Gary Russell
 * @since 1.1
 */
public interface RecordMessageConverter extends MessageConverter {

	/**
	 * Convert a {@link ConsumerRecord} to a {@link Message}.
	 * @param record the record.
	 * @param acknowledgment the acknowledgment (can be Acknowledgment or ShareAcknowledgment).
	 * @param consumer the consumer (can be Consumer or ShareConsumer).
	 * @param payloadType the required payload type.
	 * @return the message.
	 */
	@NonNull
	Message<?> toMessage(ConsumerRecord<?, ?> record, @Nullable Object acknowledgment, @Nullable Object consumer,
			@Nullable Type payloadType);

	/**
	 * Convert a message to a producer record.
	 * @param message the message.
	 * @param defaultTopic the default topic to use if no header found.
	 * @return the producer record.
	 */
	ProducerRecord<?, ?> fromMessage(Message<?> message, @Nullable String defaultTopic);

}
