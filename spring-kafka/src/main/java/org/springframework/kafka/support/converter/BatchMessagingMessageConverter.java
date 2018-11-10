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

import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.Headers;

import org.springframework.kafka.support.Acknowledgment;
import org.springframework.kafka.support.DefaultKafkaHeaderMapper;
import org.springframework.kafka.support.JacksonPresent;
import org.springframework.kafka.support.KafkaHeaderMapper;
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
 * <p>
 * If a {@link RecordMessageConverter} is provided, and the batch type is a {@link ParameterizedType}
 * with a single generic type parameter, each record will be passed to the converter, thus supporting
 * a method signature {@code List<Foo> foos}.
 *
 * @author Marius Bogoevici
 * @author Gary Russell
 * @author Dariusz Szablinski
 * @author Biju Kunjummen
 * @since 1.1
 */
public class BatchMessagingMessageConverter implements BatchMessageConverter {

	protected final Log logger = LogFactory.getLog(getClass()); // NOSONAR

	private final RecordMessageConverter recordConverter;

	private boolean generateMessageId = false;

	private boolean generateTimestamp = false;

	private KafkaHeaderMapper headerMapper;

	/**
	 * Create an instance that does not convert the record values.
	 */
	public BatchMessagingMessageConverter() {
		this(null);
	}

	/**
	 * Create an instance that converts record values using the supplied
	 * converter.
	 * @param recordConverter the converter.
	 * @since 1.3.2
	 */
	public BatchMessagingMessageConverter(RecordMessageConverter recordConverter) {
		this.recordConverter = recordConverter;
		if (JacksonPresent.isJackson2Present()) {
			this.headerMapper = new DefaultKafkaHeaderMapper();
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
	public RecordMessageConverter getRecordMessageConverter() {
		return this.recordConverter;
	}

	@Override
	public Message<?> toMessage(List<ConsumerRecord<?, ?>> records, Acknowledgment acknowledgment,
			Consumer<?, ?> consumer, Type type) {
		KafkaMessageHeaders kafkaMessageHeaders = new KafkaMessageHeaders(this.generateMessageId,
				this.generateTimestamp);

		Map<String, Object> rawHeaders = kafkaMessageHeaders.getRawHeaders();
		List<Object> payloads = new ArrayList<>();
		List<Object> keys = new ArrayList<>();
		List<String> topics = new ArrayList<>();
		List<Integer> partitions = new ArrayList<>();
		List<Long> offsets = new ArrayList<>();
		List<String> timestampTypes = new ArrayList<>();
		List<Long> timestamps = new ArrayList<>();
		List<Map<String, Object>> convertedHeaders = new ArrayList<>();
		List<Headers> natives = new ArrayList<>();
		rawHeaders.put(KafkaHeaders.RECEIVED_MESSAGE_KEY, keys);
		rawHeaders.put(KafkaHeaders.RECEIVED_TOPIC, topics);
		rawHeaders.put(KafkaHeaders.RECEIVED_PARTITION_ID, partitions);
		rawHeaders.put(KafkaHeaders.OFFSET, offsets);
		rawHeaders.put(KafkaHeaders.TIMESTAMP_TYPE, timestampTypes);
		rawHeaders.put(KafkaHeaders.RECEIVED_TIMESTAMP, timestamps);
		if (this.headerMapper != null) {
			rawHeaders.put(KafkaHeaders.BATCH_CONVERTED_HEADERS, convertedHeaders);
		}
		else {
			rawHeaders.put(KafkaHeaders.NATIVE_HEADERS, natives);
		}

		if (acknowledgment != null) {
			rawHeaders.put(KafkaHeaders.ACKNOWLEDGMENT, acknowledgment);
		}
		if (consumer != null) {
			rawHeaders.put(KafkaHeaders.CONSUMER, consumer);
		}

		boolean logged = false;
		for (ConsumerRecord<?, ?> record : records) {
			payloads.add(this.recordConverter == null || !containerType(type)
					? extractAndConvertValue(record, type)
					: convert(record, type));
			keys.add(record.key());
			topics.add(record.topic());
			partitions.add(record.partition());
			offsets.add(record.offset());
			timestampTypes.add(record.timestampType().name());
			timestamps.add(record.timestamp());
			if (this.headerMapper != null) {
				Map<String, Object> converted = new HashMap<>();
				this.headerMapper.toHeaders(record.headers(), converted);
				convertedHeaders.add(converted);
			}
			else {
				if (this.logger.isDebugEnabled() && !logged) {
					this.logger.debug(
						"No header mapper is available; Jackson is required for the default mapper; "
						+ "headers (if present) are not mapped but provided raw in "
						+ KafkaHeaders.NATIVE_HEADERS);
					logged = true;
				}
				natives.add(record.headers());
			}
		}
		return MessageBuilder.createMessage(payloads, kafkaMessageHeaders);
	}

	@Override
	public List<ProducerRecord<?, ?>> fromMessage(Message<?> message, String defaultTopic) {
		throw new UnsupportedOperationException();
	}

	/**
	 * Subclasses can convert the value; by default, it's returned as provided by Kafka
	 * unless a {@link RecordMessageConverter} has been provided.
	 * @param record the record.
	 * @param type the required type.
	 * @return the value.
	 */
	protected Object extractAndConvertValue(ConsumerRecord<?, ?> record, Type type) {
		return record.value() == null ? KafkaNull.INSTANCE : record.value();
	}


	/**
	 * Convert the record value.
	 * @param record the record.
	 * @param type the type - must be a {@link ParameterizedType} with a single generic
	 * type parameter.
	 * @return the converted payload.
	 */
	protected Object convert(ConsumerRecord<?, ?> record, Type type) {
		return this.recordConverter
			.toMessage(record, null, null, ((ParameterizedType) type).getActualTypeArguments()[0]).getPayload();
	}

	/**
	 * Return true if the type is a parameterized type with a single generic type
	 * parameter.
	 * @param type the type.
	 * @return true if the conditions are met.
	 */
	private boolean containerType(Type type) {
		return type instanceof ParameterizedType
				&& ((ParameterizedType) type).getActualTypeArguments().length == 1;
	}

}
