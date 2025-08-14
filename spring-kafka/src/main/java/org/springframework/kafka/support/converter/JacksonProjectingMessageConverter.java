/*
 * Copyright 2018-present the original author or authors.
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

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.lang.reflect.Type;
import java.nio.charset.StandardCharsets;

import com.jayway.jsonpath.Configuration;
import com.jayway.jsonpath.TypeRef;
import com.jayway.jsonpath.spi.mapper.MappingException;
import com.jayway.jsonpath.spi.mapper.MappingProvider;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.utils.Bytes;
import org.jspecify.annotations.Nullable;
import tools.jackson.databind.JavaType;
import tools.jackson.databind.json.JsonMapper;

import org.springframework.core.ResolvableType;
import org.springframework.data.projection.MethodInterceptorFactory;
import org.springframework.data.projection.ProjectionFactory;
import org.springframework.data.projection.SpelAwareProxyProjectionFactory;
import org.springframework.data.web.JsonProjectingMethodInterceptorFactory;
import org.springframework.kafka.support.JacksonMapperUtils;
import org.springframework.kafka.support.KafkaNull;
import org.springframework.messaging.Message;
import org.springframework.util.Assert;

/**
 * A {@link MessageConverter} implementation based on Jackson3 uses a Spring Data
 * {@link ProjectionFactory} to bind incoming messages to projection interfaces.
 *
 * @author Oliver Gierke
 * @author Artem Bilan
 * @author Gary Russell
 * @author Soby Chacko
 *
 * @since 4.0
 */
public class JacksonProjectingMessageConverter extends MessagingMessageConverter {

	private final ProjectionFactory projectionFactory;

	private final MessagingMessageConverter delegate;

	/**
	 * Create a new {@link JacksonProjectingMessageConverter} using a
	 * {@link JacksonMapperUtils#enhancedJsonMapper()} by default.
	 */
	public JacksonProjectingMessageConverter() {
		this(JacksonMapperUtils.enhancedJsonMapper());
	}

	/**
	 * Create a new {@link JacksonProjectingMessageConverter} using the given {@link JsonMapper}.
	 * @param mapper must not be {@literal null}.
	 */
	public JacksonProjectingMessageConverter(JsonMapper mapper) {
		this(mapper, new StringJacksonJsonMessageConverter());
	}

	/**
	 * Create a new {@link JacksonProjectingMessageConverter} using the given {@link JsonMapper}.
	 * @param delegate the delegate converter for outbound and non-interfaces.
	 */
	public JacksonProjectingMessageConverter(MessagingMessageConverter delegate) {
		this(JacksonMapperUtils.enhancedJsonMapper(), delegate);
	}

	/**
	 * Create a new {@link JacksonProjectingMessageConverter} using the given {@link JsonMapper}.
	 * @param mapper must not be {@literal null}.
	 * @param delegate the delegate converter for outbound and non-interfaces.
	 */
	public JacksonProjectingMessageConverter(JsonMapper mapper, MessagingMessageConverter delegate) {
		Assert.notNull(mapper, "JsonMapper must not be null");
		Assert.notNull(delegate, "'delegate' cannot be null");

		MappingProvider provider = new Jackson3MappingProvider(mapper);
		MethodInterceptorFactory interceptorFactory = new JsonProjectingMethodInterceptorFactory(provider);

		SpelAwareProxyProjectionFactory factory = new SpelAwareProxyProjectionFactory();
		factory.registerMethodInvokerFactory(interceptorFactory);

		this.projectionFactory = factory;
		this.delegate = delegate;
	}

	@Override
	protected @Nullable Object convertPayload(Message<?> message) {
		return this.delegate.convertPayload(message);
	}

	@Override
	protected Object extractAndConvertValue(ConsumerRecord<?, ?> record, @Nullable Type type) {
		Object value = record.value();

		if (value == null) {
			return KafkaNull.INSTANCE;
		}

		Class<?> rawType = ResolvableType.forType(type).resolve(Object.class);

		if (!rawType.isInterface()) {
			return this.delegate.extractAndConvertValue(record, type);
		}

		InputStream inputStream = new ByteArrayInputStream(getAsByteArray(value));

		// The inputStream is closed underneath by the JsonMapper#_readTreeAndClose()
		return this.projectionFactory.createProjection(rawType, inputStream);
	}

	/**
	 * Return the given source value as byte array.
	 * @param source must not be {@literal null}.
	 * @return the source instance as byte array.
	 */
	private static byte[] getAsByteArray(Object source) {
		Assert.notNull(source, "Source must not be null");

		if (source instanceof String) {
			return ((String) source).getBytes(StandardCharsets.UTF_8);
		}

		if (source instanceof byte[]) {
			return (byte[]) source;
		}

		if (source instanceof Bytes) {
			return ((Bytes) source).get();
		}

		throw new ConversionException(String.format(
				"Unsupported payload type '%s'. Expected 'String', 'Bytes', or 'byte[]'",
				source.getClass()), null);
	}

	/**
	 * A {@link MappingProvider} implementation for Jackson 3.
	 * Until respective implementation is there in json-path library.
	 * @param jsonMapper Jackson 3 {@link JsonMapper}.
	 */
	private record Jackson3MappingProvider(JsonMapper jsonMapper) implements MappingProvider {

		@Override
		public <T> @Nullable T map(@Nullable Object source, Class<T> targetType, Configuration configuration) {
			if (source == null) {
				return null;
			}
			try {
				return this.jsonMapper.convertValue(source, targetType);
			}
			catch (Exception ex) {
				throw new MappingException(ex);
			}
		}

		@Override
		public <T> @Nullable T map(@Nullable Object source, final TypeRef<T> targetType, Configuration configuration) {
			if (source == null) {
				return null;
			}
			JavaType type = this.jsonMapper.constructType(targetType.getType());

			try {
				return this.jsonMapper.convertValue(source, type);
			}
			catch (Exception ex) {
				throw new MappingException(ex);
			}
		}

	}

}
