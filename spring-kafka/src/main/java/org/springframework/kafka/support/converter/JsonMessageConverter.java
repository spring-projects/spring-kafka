/*
 * Copyright 2019-2021 the original author or authors.
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

import java.io.IOException;
import java.lang.reflect.Type;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.common.utils.Bytes;

import org.springframework.kafka.support.JacksonUtils;
import org.springframework.kafka.support.KafkaNull;
import org.springframework.kafka.support.mapping.DefaultJackson2JavaTypeMapper;
import org.springframework.kafka.support.mapping.Jackson2JavaTypeMapper;
import org.springframework.kafka.support.mapping.Jackson2JavaTypeMapper.TypePrecedence;
import org.springframework.messaging.Message;
import org.springframework.util.Assert;

import com.fasterxml.jackson.databind.JavaType;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.type.TypeFactory;

/**
 * Base class for JSON message converters; on the consumer side, it can
 * handle {@code byte[]}, {@link Bytes} and {@link String} record values.
 * On the producer side, select a subclass that matches the corresponding
 * Kafka Serializer.
 *
 * @author Gary Russell
 * @since 2.3
 *
 */
public class JsonMessageConverter extends MessagingMessageConverter {

	private static final JavaType OBJECT = TypeFactory.defaultInstance().constructType(Object.class);

	private final ObjectMapper objectMapper;

	private Jackson2JavaTypeMapper typeMapper = new DefaultJackson2JavaTypeMapper();

	public JsonMessageConverter() {
		this(JacksonUtils.enhancedObjectMapper());
	}

	public JsonMessageConverter(ObjectMapper objectMapper) {
		Assert.notNull(objectMapper, "'objectMapper' must not be null.");
		this.objectMapper = objectMapper;
	}

	public Jackson2JavaTypeMapper getTypeMapper() {
		return this.typeMapper;
	}

	/**
	 * Set a customized type mapper.
	 * @param typeMapper the type mapper.
	 */
	public void setTypeMapper(Jackson2JavaTypeMapper typeMapper) {
		Assert.notNull(typeMapper, "'typeMapper' cannot be null");
		this.typeMapper = typeMapper;
	}

	/**
	 * Return the object mapper.
	 * @return the mapper.
	 */
	protected ObjectMapper getObjectMapper() {
		return this.objectMapper;
	}

	@Override
	protected Headers initialRecordHeaders(Message<?> message) {
		RecordHeaders headers = new RecordHeaders();
		this.typeMapper.fromClass(message.getPayload().getClass(), headers);
		return headers;
	}

	@Override
	protected Object convertPayload(Message<?> message) {
		throw new UnsupportedOperationException("Select a subclass that creates a ProducerRecord value "
				+ "corresponding to the configured Kafka Serializer");
	}

	@Override
	protected Object extractAndConvertValue(ConsumerRecord<?, ?> record, Type type) {
		Object value = record.value();
		if (record.value() == null) {
			return KafkaNull.INSTANCE;
		}

		JavaType javaType = determineJavaType(record, type);
		if (value instanceof Bytes) {
			value = ((Bytes) value).get();
		}
		if (value instanceof String) {
			try {
				return this.objectMapper.readValue((String) value, javaType);
			}
			catch (IOException e) {
				throw new ConversionException("Failed to convert from JSON", record, e);
			}
		}
		else if (value instanceof byte[]) {
			try {
				return this.objectMapper.readValue((byte[]) value, javaType);
			}
			catch (IOException e) {
				throw new ConversionException("Failed to convert from JSON", record, e);
			}
		}
		else {
			throw new IllegalStateException("Only String, Bytes, or byte[] supported");
		}
	}

	private JavaType determineJavaType(ConsumerRecord<?, ?> record, Type type) {
		JavaType javaType = this.typeMapper.getTypePrecedence().equals(TypePrecedence.INFERRED) && type != null
				? TypeFactory.defaultInstance().constructType(type)
				: this.typeMapper.toJavaType(record.headers());
		if (javaType == null) { // no headers
			if (type != null) {
				javaType = TypeFactory.defaultInstance().constructType(type);
			}
			else {
				javaType = OBJECT;
			}
		}
		return javaType;
	}

}
