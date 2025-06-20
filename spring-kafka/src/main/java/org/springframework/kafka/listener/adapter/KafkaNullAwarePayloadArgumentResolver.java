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
