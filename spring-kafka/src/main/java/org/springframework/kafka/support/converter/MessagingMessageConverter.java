/*
 * Copyright 2016 the original author or authors.
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

import java.util.Map;

import org.apache.kafka.clients.consumer.ConsumerRecord;

import org.springframework.kafka.support.Acknowledgment;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.Message;
import org.springframework.messaging.MessageHeaders;
import org.springframework.messaging.support.MessageBuilder;

/**
 * The Messaging specific {@link MessageConverter} implementation.
 * <p>
 * Populates {@link KafkaHeaders} based on the {@link ConsumerRecord} onto the returned message.
 *
 * @param <K> the key type.
 * @param <V> the value type.
 *
 * @author Marius Bogoevici
 * @author Gary Russell
 */
public class MessagingMessageConverter<K, V> implements MessageConverter<K, V> {

	private boolean generateMessageId = false;

	private boolean generateTimestamp = false;

	/**
	 * Generate {@link Message} {@code ids} for produced messages. If set to {@code false},
	 * will try to use a default value. By default set to {@code false}.
	 * @param generateMessageId true if a message id should be generated
	 */
	public void setGenerateMessageId(boolean generateMessageId) {
		this.generateMessageId = generateMessageId;
	}

	/**
	 * Generate {@code timestamp} for produced messages. If set to {@code false}, -1 is
	 * used instead. By default set to {@code false}.
	 * @param generateTimestamp true if a timestamp should be generated
	 */
	public void setGenerateTimestamp(boolean generateTimestamp) {
		this.generateTimestamp = generateTimestamp;
	}

	@Override
	public Message<?> toMessage(ConsumerRecord<K, V> record, Acknowledgment acknowledgment) {
		KafkaMessageHeaders kafkaMessageHeaders = new KafkaMessageHeaders(this.generateMessageId, this.generateTimestamp);

		Map<String, Object> rawHeaders = kafkaMessageHeaders.getRawHeaders();
		rawHeaders.put(KafkaHeaders.RECEIVED_MESSAGE_KEY, record.key());
		rawHeaders.put(KafkaHeaders.RECEIVED_TOPIC, record.topic());
		rawHeaders.put(KafkaHeaders.RECEIVED_PARTITION_ID, record.partition());
		rawHeaders.put(KafkaHeaders.OFFSET, record.offset());

		if (acknowledgment != null) {
			rawHeaders.put(KafkaHeaders.ACKNOWLEDGMENT, acknowledgment);
		}

		return MessageBuilder.createMessage(extractAndConvertValue(record), kafkaMessageHeaders);
	}

	/**
	 * Subclasses can convert the value; by default, it's returned as provided by Kafka.
	 * @param record the record.
	 * @return the value.
	 */
	protected V extractAndConvertValue(ConsumerRecord<K, V> record) {
		return record.value();
	}

	@SuppressWarnings("serial")
	private static class KafkaMessageHeaders extends MessageHeaders {

		KafkaMessageHeaders(boolean generateId, boolean generateTimestamp) {
			super(null, generateId ? null : ID_VALUE_NONE, generateTimestamp ? null : -1L);
		}

		@Override
		public Map<String, Object> getRawHeaders() { //NOSONAR - not useless, widening to public
			return super.getRawHeaders();
		}

	}

}
