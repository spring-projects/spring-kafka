/*
 * Copyright 2021-present the original author or authors.
 */

package org.springframework.kafka.listener.adapter;

import java.util.List;

import org.jspecify.annotations.Nullable;

import org.springframework.core.MethodParameter;
import org.springframework.kafka.support.KafkaNull;
import org.springframework.messaging.Message;
import org.springframework.messaging.converter.MessageConverter;
import org.springframework.messaging.handler.annotation.support.PayloadMethodArgumentResolver;
import org.springframework.validation.Validator;

/**
 * {@link PayloadMethodArgumentResolver} that can properly decode {@link KafkaNull}
 * payloads, returning {@code null}. When using a custom
 * {@link org.springframework.messaging.handler.annotation.support.MessageHandlerMethodFactory},
 * add this resolver if you need to handle tombstone records with null values.
 *
 * @author Gary Russell
 * @author Wang Zhiyang
 * @since 2.7.4
 *
 */
public class KafkaNullAwarePayloadArgumentResolver extends PayloadMethodArgumentResolver {

	KafkaNullAwarePayloadArgumentResolver(MessageConverter messageConverter, @Nullable Validator validator) {
		super(messageConverter, validator);
	}

	@Override
	public @Nullable Object resolveArgument(MethodParameter parameter, Message<?> message) throws Exception { // NOSONAR
		Object resolved = super.resolveArgument(parameter, message);
		/*
		 * Replace KafkaNull list elements with null.
		 */
		if (resolved instanceof List<?> list) {
			for (int i = 0; i < list.size(); i++) {
				if (list.get(i) instanceof KafkaNull) {
					list.set(i, null);
				}
			}
		}
		return resolved;
	}

	@Override
	protected boolean isEmptyPayload(@Nullable Object payload) {
		return payload == null || payload instanceof KafkaNull;
	}

}
