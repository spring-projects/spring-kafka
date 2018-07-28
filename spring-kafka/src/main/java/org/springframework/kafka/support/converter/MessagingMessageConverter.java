/*
 * Copyright 2016-2018 the original author or authors.
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
import java.nio.charset.StandardCharsets;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.header.internals.RecordHeaders;

import org.springframework.kafka.support.Acknowledgment;
import org.springframework.kafka.support.DefaultKafkaHeaderMapper;
import org.springframework.kafka.support.JacksonPresent;
import org.springframework.kafka.support.KafkaHeaderMapper;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.kafka.support.KafkaNull;
import org.springframework.kafka.support.SimpleKafkaHeaderMapper;
import org.springframework.messaging.Message;
import org.springframework.messaging.MessageHeaders;
import org.springframework.messaging.support.MessageBuilder;
import org.springframework.util.Assert;

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
 * @author Biju Kunjummen
 */
public class MessagingMessageConverter implements RecordMessageConverter {

	protected final Log logger = LogFactory.getLog(getClass()); // NOSONAR

	private boolean generateMessageId = false;

	private boolean generateTimestamp = false;

	private KafkaHeaderMapper headerMapper;

	public MessagingMessageConverter() {
		if (JacksonPresent.isJackson2Present()) {
			this.headerMapper = new DefaultKafkaHeaderMapper();
		}
		else {
			this.headerMapper = new SimpleKafkaHeaderMapper();
		}
	}

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

	/**
	 * Set the header mapper to map headers.
	 * @param headerMapper the mapper.
	 * @since 1.3
	 */
	public void setHeaderMapper(KafkaHeaderMapper headerMapper) {
		this.headerMapper = headerMapper;
	}

	@Override
	public Message<?> toMessage(ConsumerRecord<?, ?> record, Acknowledgment acknowledgment, Consumer<?, ?> consumer,
			Type type) {
		KafkaMessageHeaders kafkaMessageHeaders = new KafkaMessageHeaders(this.generateMessageId,
				this.generateTimestamp);

		Map<String, Object> rawHeaders = kafkaMessageHeaders.getRawHeaders();
		if (this.headerMapper != null) {
			this.headerMapper.toHeaders(record.headers(), rawHeaders);
		}
		else {
			if (this.logger.isDebugEnabled()) {
				this.logger.debug(
						"No header mapper is available; Jackson is required for the default mapper; "
						+ "headers (if present) are not mapped but provided raw in "
						+ KafkaHeaders.NATIVE_HEADERS);
			}
			rawHeaders.put(KafkaHeaders.NATIVE_HEADERS, record.headers());
		}
		rawHeaders.put(KafkaHeaders.RECEIVED_MESSAGE_KEY, record.key());
		rawHeaders.put(KafkaHeaders.RECEIVED_TOPIC, record.topic());
		rawHeaders.put(KafkaHeaders.RECEIVED_PARTITION_ID, record.partition());
		rawHeaders.put(KafkaHeaders.OFFSET, record.offset());
		rawHeaders.put(KafkaHeaders.TIMESTAMP_TYPE, record.timestampType().name());
		rawHeaders.put(KafkaHeaders.RECEIVED_TIMESTAMP, record.timestamp());

		if (acknowledgment != null) {
			rawHeaders.put(KafkaHeaders.ACKNOWLEDGMENT, acknowledgment);
		}
		if (consumer != null) {
			rawHeaders.put(KafkaHeaders.CONSUMER, consumer);
		}

		return MessageBuilder.createMessage(extractAndConvertValue(record, type), kafkaMessageHeaders);
	}

	@SuppressWarnings({ "unchecked", "rawtypes" })
	@Override
	public ProducerRecord<?, ?> fromMessage(Message<?> message, String defaultTopic) {
		MessageHeaders headers = message.getHeaders();
		Object topicHeader = headers.get(KafkaHeaders.TOPIC);
		String topic = null;
		if (topicHeader instanceof byte[]) {
			topic = new String(((byte[]) topicHeader), StandardCharsets.UTF_8);
		}
		else if (topicHeader instanceof String) {
			topic = (String) topicHeader;
		}
		else if (topicHeader == null) {
			Assert.state(defaultTopic != null, "With no topic header, a defaultTopic is required");
		}
		else {
			throw new IllegalStateException(KafkaHeaders.TOPIC + " must be a String or byte[], not "
					+ topicHeader.getClass());
		}
		Integer partition = headers.get(KafkaHeaders.PARTITION_ID, Integer.class);
		Object key = headers.get(KafkaHeaders.MESSAGE_KEY);
		Object payload = convertPayload(message);
		Long timestamp = headers.get(KafkaHeaders.TIMESTAMP, Long.class);
		Headers recordHeaders = initialRecordHeaders(message);
		if (this.headerMapper != null) {
			this.headerMapper.fromHeaders(headers, recordHeaders);
		}
		return new ProducerRecord(topic == null ? defaultTopic : topic, partition, timestamp, key, payload,
				recordHeaders);
	}

	/**
	 * Subclasses can populate additional headers before they are mapped.
	 * @param message the message.
	 * @return the headers
	 * @since 2.1
	 */
	protected Headers initialRecordHeaders(Message<?> message) {
		return new RecordHeaders();
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
