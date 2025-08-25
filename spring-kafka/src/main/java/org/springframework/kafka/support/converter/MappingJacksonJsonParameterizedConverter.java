/*
 * Copyright 2021-present the original author or authors.
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

import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.utils.Bytes;
import org.jspecify.annotations.Nullable;
import tools.jackson.databind.JavaType;
import tools.jackson.databind.type.TypeFactory;

import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.kafka.support.mapping.DefaultJacksonJavaTypeMapper;
import org.springframework.kafka.support.mapping.JacksonJavaTypeMapper;
import org.springframework.messaging.Message;
import org.springframework.messaging.converter.JacksonJsonMessageConverter;
import org.springframework.util.Assert;
import org.springframework.util.MimeType;

/**
 * Subclass of {@link JacksonJsonMessageConverter} that can handle parameterized
 * (generic) types. Based on Jackson 3.
 *
 * @author Soby Chacko
 * @author Artem Bilan
 *
 * @since 4.0
 */
public class MappingJacksonJsonParameterizedConverter extends JacksonJsonMessageConverter {

	private static final JavaType OBJECT = TypeFactory.createDefaultInstance().constructType(Object.class);

	private JacksonJavaTypeMapper typeMapper = new DefaultJacksonJavaTypeMapper();

	/**
	 * Construct a {@code MappingJacksonParameterizedConverter} supporting
	 * the {@code application/json} MIME type with {@code UTF-8} character set.
	 */
	public MappingJacksonJsonParameterizedConverter() {
	}

	/**
	 * Construct a {@code MappingJacksonParameterizedConverter} supporting
	 * one or more custom MIME types.
	 * @param supportedMimeTypes the supported MIME types
	 */
	public MappingJacksonJsonParameterizedConverter(MimeType... supportedMimeTypes) {
		super(supportedMimeTypes);
	}

	/**
	 * Return the type mapper.
	 * @return the mapper.
	 */
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

	@Override
	@Nullable
	protected Object convertFromInternal(Message<?> message, Class<?> targetClass, @Nullable Object conversionHint) {
		JavaType javaType = determineJavaType(message, conversionHint);
		Object value = message.getPayload();
		if (value instanceof Bytes bytesValue) {
			value = bytesValue.get();
		}

		if (value instanceof String stringValue) {
			try {
				return getJsonMapper().readValue(stringValue, javaType);
			}
			catch (Exception e) {
				throw new ConversionException("Failed to convert from JSON", message, e);
			}
		}
		else if (value instanceof byte[] byteArrayValue) {
			try {
				return getJsonMapper().readValue(byteArrayValue, javaType);
			}
			catch (Exception e) {
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
				javaType = this.typeMapper.getTypePrecedence().equals(JacksonJavaTypeMapper.TypePrecedence.INFERRED)
						? TypeFactory.createDefaultInstance().constructType(type)
						: this.typeMapper.toJavaType(nativeHeaders);
			}
		}
		if (javaType == null) { // no headers
			if (type != null) {
				javaType = TypeFactory.createDefaultInstance().constructType(type);
			}
			else {
				javaType = OBJECT;
			}
		}
		return javaType;
	}

}
