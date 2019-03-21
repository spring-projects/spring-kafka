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
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;

import org.springframework.kafka.support.Acknowledgment;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.kafka.support.KafkaNull;
import org.springframework.messaging.Message;
import org.springframework.messaging.support.MessageBuilder;

/**
 * A Messaging {@link MessageConverter} implementation used with a batch
 * message listener; the consumer record values are extracted into a collection in
 * the message payload.
 * <p>
 * Populates {@link KafkaHeaders} based on the {@link ConsumerRecord} onto the returned message.
 * Each header is a collection where the position in the collection matches the payload
 * position.
 *
 * @author Marius Bogoevici
 * @author Gary Russell
 * @author Dariusz Szablinski
 * @since 1.1
 */
public class BatchMessagingMessageConverter implements BatchMessageConverter {

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
	public Message<?> toMessage(List<ConsumerRecord<?, ?>> records, Acknowledgment acknowledgment,
			Type type) {
		KafkaMessageHeaders kafkaMessageHeaders = new KafkaMessageHeaders(this.generateMessageId,
				this.generateTimestamp);

		Map<String, Object> rawHeaders = kafkaMessageHeaders.getRawHeaders();
		List<Object> payloads = new ArrayList<>();
		List<Object> keys = new ArrayList<>();
		List<String> topics = new ArrayList<>();
		List<Integer> partitions = new ArrayList<>();
		List<Long> offsets = new ArrayList<>();
		rawHeaders.put(KafkaHeaders.RECEIVED_MESSAGE_KEY, keys);
		rawHeaders.put(KafkaHeaders.RECEIVED_TOPIC, topics);
		rawHeaders.put(KafkaHeaders.RECEIVED_PARTITION_ID, partitions);
		rawHeaders.put(KafkaHeaders.OFFSET, offsets);

		if (acknowledgment != null) {
			rawHeaders.put(KafkaHeaders.ACKNOWLEDGMENT, acknowledgment);
		}

		for (ConsumerRecord<?, ?> record : records) {
			payloads.add(extractAndConvertValue(record, type));
			keys.add(record.key());
			topics.add(record.topic());
			partitions.add(record.partition());
			offsets.add(record.offset());
		}
		return MessageBuilder.createMessage(payloads, kafkaMessageHeaders);
	}

	@Override
	public List<ProducerRecord<?, ?>> fromMessage(Message<?> message, String defaultTopic) {
		throw new UnsupportedOperationException();
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
