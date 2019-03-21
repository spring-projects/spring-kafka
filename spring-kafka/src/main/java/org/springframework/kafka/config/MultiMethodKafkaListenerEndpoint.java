/*
 * Copyright 2016 the original author or authors.
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

package org.springframework.kafka.config;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;

import org.springframework.kafka.listener.adapter.DelegatingInvocableHandler;
import org.springframework.kafka.listener.adapter.HandlerAdapter;
import org.springframework.kafka.listener.adapter.MessagingMessageListenerAdapter;
import org.springframework.messaging.handler.invocation.InvocableHandlerMethod;

/**
 * The {@link MethodKafkaListenerEndpoint} extension for several POJO methods
 * based on the {@link org.springframework.kafka.annotation.KafkaHandler}.
 *
 * @param <K> the key type.
 * @param <V> the value type.
 *
 * @author Gary Russell
 *
 * @see org.springframework.kafka.annotation.KafkaHandler
 * @see DelegatingInvocableHandler
 */
public class MultiMethodKafkaListenerEndpoint<K, V> extends MethodKafkaListenerEndpoint<K, V> {

	private final List<Method> methods;

	public MultiMethodKafkaListenerEndpoint(List<Method> methods, Object bean) {
		this.methods = methods;
		setBean(bean);
	}

	@Override
	protected HandlerAdapter configureListenerAdapter(MessagingMessageListenerAdapter<K, V> messageListener) {
		List<InvocableHandlerMethod> invocableHandlerMethods = new ArrayList<InvocableHandlerMethod>();
		for (Method method : this.methods) {
			invocableHandlerMethods.add(getMessageHandlerMethodFactory()
					.createInvocableHandlerMethod(getBean(), method));
		}
		DelegatingInvocableHandler delegatingHandler =
				new DelegatingInvocableHandler(invocableHandlerMethods, getBean());
		return new HandlerAdapter(delegatingHandler);
	}

}
