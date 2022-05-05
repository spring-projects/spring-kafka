/*
 * Copyright 2016-2022 the original author or authors.
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

import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.nio.charset.StandardCharsets;
import java.util.Map;

import org.apache.commons.logging.LogFactory;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.header.internals.RecordHeaders;

import org.springframework.core.log.LogAccessor;
import org.springframework.kafka.support.AbstractKafkaHeaderMapper;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.kafka.support.DefaultKafkaHeaderMapper;
import org.springframework.kafka.support.JacksonPresent;
import org.springframework.kafka.support.KafkaHeaderMapper;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.kafka.support.KafkaNull;
import org.springframework.kafka.support.SimpleKafkaHeaderMapper;
import org.springframework.messaging.Message;
import org.springframework.messaging.MessageHeaders;
import org.springframework.messaging.converter.SmartMessageConverter;
import org.springframework.messaging.support.GenericMessage;
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

	protected final LogAccessor logger = new LogAccessor(LogFactory.getLog(getClass())); // NOSONAR

	private boolean generateMessageId = false;

	private boolean generateTimestamp = false;

	private KafkaHeaderMapper headerMapper;

	private boolean rawRecordHeader;

	private SmartMessageConverter messagingConverter;

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

	/**
	 * Set to true to add the raw {@link ConsumerRecord} as a header
	 * {@link KafkaHeaders#RAW_DATA}.
	 * @param rawRecordHeader true to add the header.
	 * @since 2.7
	 */
	public void setRawRecordHeader(boolean rawRecordHeader) {
		this.rawRecordHeader = rawRecordHeader;
	}


	protected org.springframework.messaging.converter.MessageConverter getMessagingConverter() {
		return this.messagingConverter;
	}

	/**
	 * Set a spring-messaging {@link SmartMessageConverter} to convert the record value to
	 * the desired type. This will also cause the {@link MessageHeaders#CONTENT_TYPE} to
	 * be converted to String when mapped inbound.
	 * <p>
	 * IMPORTANT: This converter's {@link #fromMessage(Message, String)} method is called
	 * for outbound conversion to a {@link ProducerRecord} with the message payload in the
	 * {@link ProducerRecord#value()} property.
	 * {@link #toMessage(ConsumerRecord, Acknowledgment, Consumer, Type)} is called for
	 * inbound conversion from {@link ConsumerRecord} with the payload being the
	 * {@link ConsumerRecord#value()} property.
	 * <p>
	 * The {@link SmartMessageConverter#toMessage(Object, MessageHeaders)} method is
	 * called to create a new outbound {@link Message} from the {@link Message} passed to
	 * {@link #fromMessage(Message, String)}. Similarly, in
	 * {@link #toMessage(ConsumerRecord, Acknowledgment, Consumer, Type)}, after this
	 * converter has created a new {@link Message} from the {@link ConsumerRecord} the
	 * {@link SmartMessageConverter#fromMessage(Message, Class)} method is called and then
	 * the final inbound message is created with the newly converted payload.
	 * <p>
	 * In either case, if the {@link SmartMessageConverter} returns {@code null}, the
	 * original message is used.
	 * @param messagingConverter the converter.
	 * @since 2.7.1
	 */
	public void setMessagingConverter(SmartMessageConverter messagingConverter) {
		this.messagingConverter = messagingConverter;
		if (messagingConverter != null && this.headerMapper instanceof AbstractKafkaHeaderMapper) {
			((AbstractKafkaHeaderMapper) this.headerMapper).addRawMappedHeader(MessageHeaders.CONTENT_TYPE, true);
		}
	}

	@Override
	public Message<?> toMessage(ConsumerRecord<?, ?> record, Acknowledgment acknowledgment, Consumer<?, ?> consumer,
			Type type) {

		KafkaMessageHeaders kafkaMessageHeaders = new KafkaMessageHeaders(this.generateMessageId,
				this.generateTimestamp);

		Map<String, Object> rawHeaders = kafkaMessageHeaders.getRawHeaders();
		if (record.headers() != null) {
			mapOrAddHeaders(record, rawHeaders);
		}
		String ttName = record.timestampType() != null ? record.timestampType().name() : null;
		commonHeaders(acknowledgment, consumer, rawHeaders, record.key(), record.topic(), record.partition(),
				record.offset(), ttName, record.timestamp());
		if (this.rawRecordHeader) {
			rawHeaders.put(KafkaHeaders.RAW_DATA, record);
		}
		Message<?> message = MessageBuilder.createMessage(extractAndConvertValue(record, type), kafkaMessageHeaders);
		if (this.messagingConverter != null && !message.getPayload().equals(KafkaNull.INSTANCE)) {
			Class<?> clazz = type instanceof Class ? (Class<?>) type : type instanceof ParameterizedType
					? (Class<?>) ((ParameterizedType) type).getRawType() : Object.class;
			Object payload = this.messagingConverter.fromMessage(message, clazz, type);
			if (payload != null) {
				message = new GenericMessage<>(payload, message.getHeaders());
			}
		}
		return message;
	}

	private void mapOrAddHeaders(ConsumerRecord<?, ?> record, Map<String, Object> rawHeaders) {
		if (this.headerMapper != null) {
			this.headerMapper.toHeaders(record.headers(), rawHeaders);
		}
		else {
			this.logger.debug(() ->
					"No header mapper is available; Jackson is required for the default mapper; "
					+ "headers (if present) are not mapped but provided raw in "
					+ KafkaHeaders.NATIVE_HEADERS);
			rawHeaders.put(KafkaHeaders.NATIVE_HEADERS, record.headers());
			Header contentType = record.headers().lastHeader(MessageHeaders.CONTENT_TYPE);
			if (contentType != null) {
				rawHeaders.put(MessageHeaders.CONTENT_TYPE,
						new String(contentType.value(), StandardCharsets.UTF_8));
			}
		}
	}

	@SuppressWarnings({ "unchecked", "rawtypes" })
	@Override
	public ProducerRecord<?, ?> fromMessage(Message<?> messageArg, String defaultTopic) {
		Message<?> message = messageArg;
		if (this.messagingConverter != null) {
			Message<?> converted = this.messagingConverter.toMessage(message.getPayload(), message.getHeaders());
			if (converted != null) {
				message = converted;
			}
		}
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
		Integer partition = headers.get(KafkaHeaders.PARTITION, Integer.class);
		Object key = headers.get(KafkaHeaders.KEY);
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
	 * Subclasses can convert the value; by default, it's returned as provided by Kafka
	 * unless there is a {@link SmartMessageConverter} that can convert it.
	 * @param record the record.
	 * @param type the required type.
	 * @return the value.
	 */
	protected Object extractAndConvertValue(ConsumerRecord<?, ?> record, Type type) {
		return record.value() == null ? KafkaNull.INSTANCE : record.value();
	}

}
