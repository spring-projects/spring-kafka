/*
 * Copyright 2019-present the original author or authors.
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

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.common.utils.Bytes;
import org.jspecify.annotations.Nullable;
import tools.jackson.databind.JavaType;
import tools.jackson.databind.json.JsonMapper;
import tools.jackson.databind.type.TypeFactory;

import org.springframework.kafka.support.JacksonMapperUtils;
import org.springframework.kafka.support.KafkaNull;
import org.springframework.kafka.support.mapping.DefaultJacksonJavaTypeMapper;
import org.springframework.kafka.support.mapping.JacksonJavaTypeMapper;
import org.springframework.messaging.Message;
import org.springframework.util.Assert;

/**
 * Base class for JSON message converters that use Jackson 3; on the consumer side, it can
 * handle {@code byte[]}, {@link Bytes} and {@link String} record values.
 * On the producer side, select a subclass that matches the corresponding
 * Kafka Serializer.
 *
 * @author Gary Russell
 * @author Soby Chacko
 *
 * @since 4.0
 */
public class JacksonJsonMessageConverter extends MessagingMessageConverter {

	private final JsonMapper jsonMapper;

	private JacksonJavaTypeMapper typeMapper = new DefaultJacksonJavaTypeMapper();

	private final TypeFactory typeFactory;

	public JacksonJsonMessageConverter() {
		this(JacksonMapperUtils.enhancedJsonMapper());
	}

	public JacksonJsonMessageConverter(JsonMapper jsonMapper) {
		Assert.notNull(jsonMapper, "'jsonMapper' must not be null.");
		this.jsonMapper = jsonMapper;
		this.typeFactory = jsonMapper.getTypeFactory();
	}

	public JacksonJavaTypeMapper getTypeMapper() {
		return this.typeMapper;
	}

	/**
	 * Set a customized type mapper.
	 * @param typeMapper the type mapper.
	 */
	public void setTypeMapper(JacksonJavaTypeMapper typeMapper) {
		Assert.notNull(typeMapper, "'typeMapper' cannot be null");
		this.typeMapper = typeMapper;
	}

	/**
	 * Return the object mapper.
	 * @return the mapper.
	 */
	protected JsonMapper getJsonMapper() {
		return this.jsonMapper;
	}

	@Override
	protected Headers initialRecordHeaders(Message<?> message) {
		RecordHeaders headers = new RecordHeaders();
		this.typeMapper.fromClass(message.getPayload().getClass(), headers);
		return headers;
	}

	@Override
	protected @Nullable Object convertPayload(Message<?> message) {
		throw new UnsupportedOperationException("Select a subclass that creates a ProducerRecord value "
				+ "corresponding to the configured Kafka Serializer");
	}

	@Override
	protected Object extractAndConvertValue(ConsumerRecord<?, ?> record, @Nullable Type type) {
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
				return this.jsonMapper.readValue((String) value, javaType);
			}
			catch (Exception e) {
				throw new ConversionException("Failed to convert from JSON", record, e);
			}
		}
		else if (value instanceof byte[]) {
			try {
				return this.jsonMapper.readValue((byte[]) value, javaType);
			}
			catch (Exception e) {
				throw new ConversionException("Failed to convert from JSON", record, e);
			}
		}
		else {
			throw new IllegalStateException("Only String, Bytes, or byte[] supported");
		}
	}

	private JavaType determineJavaType(ConsumerRecord<?, ?> record, @Nullable Type type) {
		JavaType javaType = this.typeMapper.getTypePrecedence().equals(JacksonJavaTypeMapper.TypePrecedence.INFERRED) && type != null
				? this.typeFactory.constructType(type)
				: this.typeMapper.toJavaType(record.headers());
		if (javaType == null) { // no headers
			if (type != null) {
				javaType = this.typeFactory.constructType(type);
			}
			else {
				javaType = this.typeFactory.constructType(Object.class);
			}
		}
		return javaType;
	}

}
