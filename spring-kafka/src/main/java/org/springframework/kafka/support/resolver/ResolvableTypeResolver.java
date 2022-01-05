/*
 * Copyright 2014-2021 the original author or authors.
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

package org.springframework.kafka.support.resolver;

import org.springframework.core.MethodParameter;
import org.springframework.kafka.annotation.ResolvableType;
import org.springframework.messaging.Message;
import org.springframework.messaging.MessageHeaders;
import org.springframework.messaging.converter.MessageConverter;
import org.springframework.messaging.handler.annotation.support.PayloadMethodArgumentResolver;
import org.springframework.messaging.support.MessageBuilder;

/**
 * A resolver to extract and convert the payload of a message using a MessageConverter.
 * This HandlerMethodArgumentResolver only work with ResolvableType annotation now.
 *
 * @author Scruel Tao
 */
public class ResolvableTypeResolver extends PayloadMethodArgumentResolver {
	/**
	 * Create a new {@code ResolvableTypeResolver} with the given
	 * {@link MessageConverter}.
	 *
	 * @param messageConverter the MessageConverter to use (required)
	 */
	public ResolvableTypeResolver(MessageConverter messageConverter) {
		super(messageConverter);
	}

	@Override
	public boolean supportsParameter(MethodParameter parameter) {
		return parameter.hasParameterAnnotation(ResolvableType.class);
	}

	@Override
	public Object resolveArgument(MethodParameter parameter, Message<?> message) throws Exception {
		ResolvableType ann = parameter.getParameterAnnotation(ResolvableType.class);
		if (null == ann) {
			throw new IllegalStateException("Annotation parsing failed.");
		}
		// If not present, parse content type from annotation
		Object type = message.getHeaders().get(MessageHeaders.CONTENT_TYPE);
		if (ann.force() || type == null) {
			message = MessageBuilder.fromMessage(message)
					.setHeader(MessageHeaders.CONTENT_TYPE, ann.type())
					.build();
		}
		return super.resolveArgument(parameter, message);
	}

}
