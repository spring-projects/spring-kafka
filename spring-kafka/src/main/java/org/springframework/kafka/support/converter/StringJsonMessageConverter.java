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

import java.io.IOException;
import java.lang.reflect.Type;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.common.utils.Bytes;

import org.springframework.kafka.support.KafkaNull;
import org.springframework.kafka.support.converter.Jackson2JavaTypeMapper.TypePrecedence;
import org.springframework.messaging.Message;
import org.springframework.util.Assert;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JavaType;
import com.fasterxml.jackson.databind.MapperFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.type.TypeFactory;

/**
 * JSON Message converter - String on output, String, Bytes, or byte[] on input. Used in
 * conjunction with Kafka
 * {@code StringSerializer/StringDeserializer or BytesDeserializer}. Consider using the
 * BytesJsonMessageConverter instead.
 *
 * @author Gary Russell
 * @author Artem Bilan
 * @author Dariusz Szablinski
 */
public class StringJsonMessageConverter extends MessagingMessageConverter {

	private final ObjectMapper objectMapper;

	private Jackson2JavaTypeMapper typeMapper = new DefaultJackson2JavaTypeMapper();

	public StringJsonMessageConverter() {
		this(new ObjectMapper());
		this.objectMapper.configure(MapperFeature.DEFAULT_VIEW_INCLUSION, false);
		this.objectMapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
	}

	public StringJsonMessageConverter(ObjectMapper objectMapper) {
		Assert.notNull(objectMapper, "'objectMapper' must not be null.");
		this.objectMapper = objectMapper;
	}

	public Jackson2JavaTypeMapper getTypeMapper() {
		return this.typeMapper;
	}

	/**
	 * Set a customized type mapper.
	 * @param typeMapper the type mapper.
	 * @since 2.1
	 */
	public void setTypeMapper(Jackson2JavaTypeMapper typeMapper) {
		Assert.notNull(typeMapper, "'typeMapper' cannot be null");
		this.typeMapper = typeMapper;
	}

	/**
	 * Return the object mapper.
	 * @return the mapper.
	 * @since 2.1.7
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
		try {
			return this.objectMapper.writeValueAsString(message.getPayload());
		}
		catch (JsonProcessingException e) {
			throw new ConversionException("Failed to convert to JSON", e);
		}
	}


	@Override
	protected Object extractAndConvertValue(ConsumerRecord<?, ?> record, Type type) {
		Object value = record.value();
		if (record.value() == null) {
			return KafkaNull.INSTANCE;
		}

		JavaType javaType = this.typeMapper.getTypePrecedence().equals(TypePrecedence.INFERRED)
			? TypeFactory.defaultInstance().constructType(type)
			: this.typeMapper.toJavaType(record.headers());
		if (javaType == null) { // no headers
			javaType = TypeFactory.defaultInstance().constructType(type);
		}
		if (value instanceof Bytes) {
			value = ((Bytes) value).get();
		}
		if (value instanceof String) {
			try {
				return this.objectMapper.readValue((String) value, javaType);
			}
			catch (IOException e) {
				throw new ConversionException("Failed to convert from JSON", e);
			}
		}
		else if (value instanceof byte[]) {
			try {
				return this.objectMapper.readValue((byte[]) value, javaType);
			}
			catch (IOException e) {
				throw new ConversionException("Failed to convert from JSON", e);
			}
		}
		else {
			throw new IllegalStateException("Only String or byte[] supported");
		}
	}

}
