/*
 * Copyright 2016-2016 the original author or authors.
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
import java.util.List;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;

import org.springframework.kafka.support.Acknowledgment;
import org.springframework.lang.Nullable;
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
	Message<?> toMessage(List<ConsumerRecord<?, ?>> records, Acknowledgment acknowledgment, Consumer<?, ?> consumer,
			Type payloadType);

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
