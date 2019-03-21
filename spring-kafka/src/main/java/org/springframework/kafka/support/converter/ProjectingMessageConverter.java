/*
 * Copyright 2018 the original author or authors.
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

import org.springframework.core.ResolvableType;
import org.springframework.data.projection.MethodInterceptorFactory;
import org.springframework.data.projection.ProjectionFactory;
import org.springframework.data.projection.SpelAwareProxyProjectionFactory;
import org.springframework.data.web.JsonProjectingMethodInterceptorFactory;
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
 *
 * @since 2.1.1
 */
public class ProjectingMessageConverter extends MessagingMessageConverter {

	private final ProjectionFactory projectionFactory;

	private final MessagingMessageConverter delegate;

	/**
	 * Creates a new {@link ProjectingMessageConverter} using the given {@link ObjectMapper}.
	 * @param mapper must not be {@literal null}.
	 */
	public ProjectingMessageConverter(ObjectMapper mapper) {
		Assert.notNull(mapper, "ObjectMapper must not be null");

		JacksonMappingProvider provider = new JacksonMappingProvider(mapper);
		MethodInterceptorFactory interceptorFactory = new JsonProjectingMethodInterceptorFactory(provider);

		SpelAwareProxyProjectionFactory factory = new SpelAwareProxyProjectionFactory();
		factory.registerMethodInvokerFactory(interceptorFactory);

		this.projectionFactory = factory;
		this.delegate = new StringJsonMessageConverter(mapper);
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
			return String.class.cast(source).getBytes(StandardCharsets.UTF_8);
		}

		if (source instanceof byte[]) {
			return byte[].class.cast(source);
		}

		throw new ConversionException(String.format("Unsupported payload type '%s'. Expected 'String' or 'byte[]'",
				source.getClass()), null);
	}

}
