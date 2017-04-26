/*
 * Copyright 2016-2017 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.springframework.kafka.support.converter;

import java.lang.reflect.Type;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;

import org.springframework.kafka.support.Acknowledgment;
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
	 * @param acknowledgment the acknowledgment.
	 * @param consumer the consumer
	 * @param payloadType the required payload type.
	 * @return the message.
	 */
	Message<?> toMessage(ConsumerRecord<?, ?> record, Acknowledgment acknowledgment, Consumer<?, ?> consumer,
			Type payloadType);

	/**
	 * Convert a message to a producer record.
	 * @param message the message.
	 * @param defaultTopic the default topic to use if no header found.
	 * @return the producer record.
	 */
	ProducerRecord<?, ?> fromMessage(Message<?> message, String defaultTopic);

}
