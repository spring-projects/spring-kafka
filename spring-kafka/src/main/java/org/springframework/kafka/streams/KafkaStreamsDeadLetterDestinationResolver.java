/*
 * Copyright 2016-present the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
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
