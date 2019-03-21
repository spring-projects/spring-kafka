/*
 * Copyright 2016 the original author or authors.
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

package org.springframework.kafka.support.converter;

import java.lang.reflect.Type;
import java.util.Map;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;

import org.springframework.kafka.support.Acknowledgment;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.kafka.support.KafkaNull;
import org.springframework.messaging.Message;
import org.springframework.messaging.MessageHeaders;
import org.springframework.messaging.support.MessageBuilder;

/**
 * A Messaging {@link MessageConverter} implementation for a message listener that
 * receives individual messages.
 * <p>
 * Populates {@link KafkaHeaders} based on the {@link ConsumerRecord} onto the returned
 * message.
 *
 * @author Marius Bogoevici
 * @author Gary Russell
 * @author Dariusz Szablinski
 */
public class MessagingMessageConverter implements RecordMessageConverter {

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
	public Message<?> toMessage(ConsumerRecord<?, ?> record, Acknowledgment acknowledgment, Type type) {
		KafkaMessageHeaders kafkaMessageHeaders = new KafkaMessageHeaders(this.generateMessageId,
				this.generateTimestamp);

		Map<String, Object> rawHeaders = kafkaMessageHeaders.getRawHeaders();
		rawHeaders.put(KafkaHeaders.RECEIVED_MESSAGE_KEY, record.key());
		rawHeaders.put(KafkaHeaders.RECEIVED_TOPIC, record.topic());
		rawHeaders.put(KafkaHeaders.RECEIVED_PARTITION_ID, record.partition());
		rawHeaders.put(KafkaHeaders.OFFSET, record.offset());

		if (acknowledgment != null) {
			rawHeaders.put(KafkaHeaders.ACKNOWLEDGMENT, acknowledgment);
		}

		return MessageBuilder.createMessage(extractAndConvertValue(record, type), kafkaMessageHeaders);
	}

	@SuppressWarnings({ "unchecked", "rawtypes" })
	@Override
	public ProducerRecord<?, ?> fromMessage(Message<?> message, String defaultTopic) {
		MessageHeaders headers = message.getHeaders();
		String topic = headers.get(KafkaHeaders.TOPIC, String.class);
		Integer partition = headers.get(KafkaHeaders.PARTITION_ID, Integer.class);
		Object key = headers.get(KafkaHeaders.MESSAGE_KEY);
		Object payload = convertPayload(message);
		return new ProducerRecord(topic == null ? defaultTopic : topic, partition, key, payload);
	}

	/**
	 * Subclasses can convert the payload; by default, it's sent unchanged to Kafka.
	 * @param message the message.
	 * @return the payload.
	 */
	protected Object convertPayload(Message<?> message) {
		Object payload = message.getPayload();
		if (payload instanceof KafkaNull) {
			return null;
		}
		else {
			return payload;
		}
	}

	/**
	 * Subclasses can convert the value; by default, it's returned as provided by Kafka.
	 * @param record the record.
	 * @param type the required type.
	 * @return the value.
	 */
	protected Object extractAndConvertValue(ConsumerRecord<?, ?> record, Type type) {
		return record.value() == null ? KafkaNull.INSTANCE : record.value();
	}

}
