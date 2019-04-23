/*
 * Copyright 2018-2019 the original author or authors.
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

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.utils.Bytes;

import org.springframework.core.ResolvableType;
import org.springframework.data.projection.MethodInterceptorFactory;
import org.springframework.data.projection.ProjectionFactory;
import org.springframework.data.projection.SpelAwareProxyProjectionFactory;
import org.springframework.data.web.JsonProjectingMethodInterceptorFactory;
import org.springframework.kafka.support.JacksonUtils;
import org.springframework.kafka.support.KafkaNull;
import org.springframework.messaging.Message;
import org.springframework.util.Assert;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.jayway.jsonpath.spi.mapper.JacksonMappingProvider;

/**
 * A {@link MessageConverter} implementation that uses a Spring Data
 * {@link ProjectionFactory} to bind incoming messages to projection interfaces.
 *
 * @author Oliver Gierke
 * @author Artem Bilan
 * @author Gary Russell
 *
 * @since 2.1.1
 */
public class ProjectingMessageConverter extends MessagingMessageConverter {

	private final ProjectionFactory projectionFactory;

	private final MessagingMessageConverter delegate;

	/**
	 * Create a new {@link ProjectingMessageConverter} using a
	 * {@link JacksonUtils#enhancedObjectMapper()} by default.
	 * @since 2.3
	 */
	public ProjectingMessageConverter() {
		this(JacksonUtils.enhancedObjectMapper());
	}

	/**
	 * Create a new {@link ProjectingMessageConverter} using the given {@link ObjectMapper}.
	 * @param mapper must not be {@literal null}.
	 */
	public ProjectingMessageConverter(ObjectMapper mapper) {
		this(mapper, new StringJsonMessageConverter());
	}

	/**
	 * Create a new {@link ProjectingMessageConverter} using the given {@link ObjectMapper}.
	 * @param delegate the delegate converter for outbound and non-interfaces.
	 * @since 2.3
	 */
	public ProjectingMessageConverter(MessagingMessageConverter delegate) {
		this(JacksonUtils.enhancedObjectMapper(), delegate);
	}

	/**
	 * Create a new {@link ProjectingMessageConverter} using the given {@link ObjectMapper}.
	 * @param mapper must not be {@literal null}.
	 * @param delegate the delegate converter for outbound and non-interfaces.
	 * @since 2.3
	 */
	public ProjectingMessageConverter(ObjectMapper mapper, MessagingMessageConverter delegate) {
		Assert.notNull(mapper, "ObjectMapper must not be null");
		Assert.notNull(delegate, "'delegate' cannot be null");

		JacksonMappingProvider provider = new JacksonMappingProvider(mapper);
		MethodInterceptorFactory interceptorFactory = new JsonProjectingMethodInterceptorFactory(provider);

		SpelAwareProxyProjectionFactory factory = new SpelAwareProxyProjectionFactory();
		factory.registerMethodInvokerFactory(interceptorFactory);

		this.projectionFactory = factory;
		this.delegate = delegate;
	}

	@Override
	protected Object convertPayload(Message<?> message) {
		return this.delegate.convertPayload(message);
	}

	@Override
	protected Object extractAndConvertValue(ConsumerRecord<?, ?> record, Type type) {
		Object value = record.value();

		if (value == null) {
			return KafkaNull.INSTANCE;
		}

		Class<?> rawType = ResolvableType.forType(type).resolve(Object.class);

		if (!rawType.isInterface()) {
			return this.delegate.extractAndConvertValue(record, type);
		}

		InputStream inputStream = new ByteArrayInputStream(getAsByteArray(value));

		// The inputStream is closed underneath by the ObjectMapper#_readTreeAndClose()
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

}
