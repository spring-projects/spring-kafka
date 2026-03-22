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

package org.springframework.kafka.listener;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.streams.errors.ErrorHandlerContext;

/**
 * An interface to resolve the destination for a record that failed and is being sent to DLT through the native Kafka Streams DLQ.
 *
 * @author Loïc Greffier
 * @since 4.1
 */
@FunctionalInterface
public interface NativeDeadLetterDestinationResolver {
	TopicPartition apply(ErrorHandlerContext errorHandlerContext, ConsumerRecord<?, ?> record, Exception exception);
}
