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

package org.springframework.kafka.config;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;

import org.springframework.kafka.listener.adapter.DelegatingInvocableHandler;
import org.springframework.kafka.listener.adapter.HandlerAdapter;
import org.springframework.kafka.listener.adapter.MessagingMessageListenerAdapter;
import org.springframework.lang.Nullable;
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

	private final Method defaultMethod;

	/**
	 * Construct an instance for the provided methods and bean with no default method.
	 * @param methods the methods.
	 * @param bean the bean.
	 */
	public MultiMethodKafkaListenerEndpoint(List<Method> methods, Object bean) {
		this(methods, null, bean);
	}

	/**
	 * Construct an instance for the provided methods, default method and bean.
	 * @param methods the methods.
	 * @param defaultMethod the default method.
	 * @param bean the bean.
	 * @since 2.1.3
	 */
	public MultiMethodKafkaListenerEndpoint(List<Method> methods, @Nullable Method defaultMethod, Object bean) {
		this.methods = methods;
		this.defaultMethod = defaultMethod;
		setBean(bean);
	}

	@Override
	protected HandlerAdapter configureListenerAdapter(MessagingMessageListenerAdapter<K, V> messageListener) {
		List<InvocableHandlerMethod> invocableHandlerMethods = new ArrayList<InvocableHandlerMethod>();
		InvocableHandlerMethod defaultHandler = null;
		for (Method method : this.methods) {
			InvocableHandlerMethod handler = getMessageHandlerMethodFactory()
					.createInvocableHandlerMethod(getBean(), method);
			invocableHandlerMethods.add(handler);
			if (method.equals(this.defaultMethod)) {
				defaultHandler = handler;
			}
		}
		DelegatingInvocableHandler delegatingHandler = new DelegatingInvocableHandler(invocableHandlerMethods,
				defaultHandler, getBean(), getResolver(), getBeanExpressionContext(), getBeanFactory());
		return new HandlerAdapter(delegatingHandler);
	}

}
