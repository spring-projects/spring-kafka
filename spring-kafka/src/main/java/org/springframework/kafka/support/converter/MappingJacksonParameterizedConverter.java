/*
 * Copyright 2021 the original author or authors.
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

import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.utils.Bytes;

import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.kafka.support.converter.Jackson2JavaTypeMapper.TypePrecedence;
import org.springframework.lang.Nullable;
import org.springframework.messaging.Message;
import org.springframework.messaging.converter.MappingJackson2MessageConverter;
import org.springframework.util.Assert;
import org.springframework.util.MimeType;

import com.fasterxml.jackson.databind.JavaType;
import com.fasterxml.jackson.databind.type.TypeFactory;

/**
 * Subclass of {@link MappingJackson2MessageConverter} that can handle parameterized
 * (generic) types.
 *
 * @author Gary Russell
 * @since 2.7.1
 *
 */
public class MappingJacksonParameterizedConverter extends MappingJackson2MessageConverter {

	private static final JavaType OBJECT = TypeFactory.defaultInstance().constructType(Object.class);

	private Jackson2JavaTypeMapper typeMapper = new DefaultJackson2JavaTypeMapper();

	/**
	 * Construct a {@code MappingJacksonParameterizedConverter} supporting
	 * the {@code application/json} MIME type with {@code UTF-8} character set.
	 */
	public MappingJacksonParameterizedConverter() {
	}

	/**
	 * Construct a {@code MappingJacksonParameterizedConverter} supporting
	 * one or more custom MIME types.
	 * @param supportedMimeTypes the supported MIME types
	 */
	public MappingJacksonParameterizedConverter(MimeType... supportedMimeTypes) {
		super(supportedMimeTypes);
	}

	/**
	 * Return the type mapper.
	 * @return the mapper.
	 */
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

	@Override
	@Nullable
	protected Object convertFromInternal(Message<?> message, Class<?> targetClass, @Nullable Object conversionHint) {
		JavaType javaType = determineJavaType(message, conversionHint);
		Object value = message.getPayload();
		if (value instanceof Bytes) {
			value = ((Bytes) value).get();
		}
		if (value instanceof String) {
			try {
				return getObjectMapper().readValue((String) value, javaType);
			}
			catch (IOException e) {
				throw new ConversionException("Failed to convert from JSON", message, e);
			}
		}
		else if (value instanceof byte[]) {
			try {
				return getObjectMapper().readValue((byte[]) value, javaType);
			}
			catch (IOException e) {
				throw new ConversionException("Failed to convert from JSON", message, e);
			}
		}
		else {
			throw new IllegalStateException("Only String, Bytes, or byte[] supported");
		}
	}

	private JavaType determineJavaType(Message<?> message, @Nullable Object hint) {
		JavaType javaType = null;
		Type type = null;
		if (hint instanceof Type) {
			type = (Type) hint;
			Headers nativeHeaders = message.getHeaders().get(KafkaHeaders.NATIVE_HEADERS, Headers.class);
			if (nativeHeaders != null) {
				javaType = this.typeMapper.getTypePrecedence().equals(TypePrecedence.INFERRED)
						? TypeFactory.defaultInstance().constructType(type)
						: this.typeMapper.toJavaType(nativeHeaders);
			}
		}
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
