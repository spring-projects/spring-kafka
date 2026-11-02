/*
 * Copyright 2016-present the original author or authors.
 */

package org.springframework.kafka.streams;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.streams.errors.ErrorHandlerContext;

/**
 * Interface for resolving the dead-letter topic destination of a failed record handled by
 * either {@link RecoveringDeserializationExceptionHandler},
 * {@link RecoveringProcessingExceptionHandler}, or {@link RecoveringProductionExceptionHandler}
 * through the native Kafka Streams DLQ.
 *
 * @author Loïc Greffier
 * @since 4.1
 */
@FunctionalInterface
public interface KafkaStreamsDeadLetterDestinationResolver {
	/**
	 * Resolves the topic-partition to which a dead-letter record should be sent.
	 * Use a negative partition number to let the partitioner determine the partition.
	 *
	 * @param errorHandlerContext the error handler context
	 * @param record the input record of the failed processor
	 * @param exception the exception
	 * @return the topic-partition to which the failed record should be sent
	 */
	TopicPartition resolve(ErrorHandlerContext errorHandlerContext, ConsumerRecord<?, ?> record, Exception exception);
}
