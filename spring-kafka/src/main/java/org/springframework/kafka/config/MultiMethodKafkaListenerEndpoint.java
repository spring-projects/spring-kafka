/*
 * Copyright 2016-present the original author or authors.
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

import org.jspecify.annotations.Nullable;

import org.springframework.kafka.listener.adapter.DelegatingInvocableHandler;
import org.springframework.kafka.listener.adapter.HandlerAdapter;
import org.springframework.kafka.listener.adapter.MessagingMessageListenerAdapter;
import org.springframework.messaging.handler.annotation.support.MessageHandlerMethodFactory;
import org.springframework.messaging.handler.invocation.InvocableHandlerMethod;
import org.springframework.validation.Validator;

/**
 * The {@link MethodKafkaListenerEndpoint} extension for several POJO methods
 * based on the {@link org.springframework.kafka.annotation.KafkaHandler}.
 *
 * @param <K> the key type.
 * @param <V> the value type.
 *
 * @author Gary Russell
 * @author Wang Zhiyang
 *
 * @see org.springframework.kafka.annotation.KafkaHandler
 * @see DelegatingInvocableHandler
 */
public class MultiMethodKafkaListenerEndpoint<K, V> extends MethodKafkaListenerEndpoint<K, V> {

	private List<Method> methods;

	private @Nullable Method defaultMethod;

	private @Nullable Validator validator;

	/**
	 * Construct an instance for the provided methods, default method and bean.
	 * @param methods the methods.
	 * @param defaultMethod the default method.
	 * @param bean the bean.
	 * @since 2.1.3
	 */
	@SuppressWarnings("this-escape")
	public MultiMethodKafkaListenerEndpoint(List<Method> methods, @Nullable Method defaultMethod, Object bean) {
		this.methods = methods;
		this.defaultMethod = defaultMethod;
		setBean(bean);
	}

	/**
	 * Get a method list.
	 * @return the method list.
	 * @since 3.2
	 */
	public List<Method> getMethods() {
		return this.methods;
	}

	/**
	 * Set a method list.
	 * @param methods the methods.
	 * @since 3.2
	 */
	public void setMethods(List<Method> methods) {
		this.methods = methods;
	}

	/**
	 * Get a default method.
	 * @return the default method.
	 * @since 3.2
	 */
	public @Nullable Method getDefaultMethod() {
		return this.defaultMethod;
	}

	/**
	 * Set a default method.
	 * @param defaultMethod the default method.
	 * @since 3.2
	 */
	public void setDefaultMethod(@Nullable Method defaultMethod) {
		this.defaultMethod = defaultMethod;
	}

	/**
	 * Set a payload validator.
	 * @param validator the validator.
	 * @since 2.5.11
	 */
	public void setValidator(Validator validator) {
		this.validator = validator;
	}

	@Override
	protected HandlerAdapter configureListenerAdapter(MessagingMessageListenerAdapter<K, V> messageListener) {
		List<InvocableHandlerMethod> invocableHandlerMethods = new ArrayList<>();
		InvocableHandlerMethod defaultHandler = null;
		for (Method method : this.methods) {
			MessageHandlerMethodFactory messageHandlerMethodFactory = getMessageHandlerMethodFactory();
			if (messageHandlerMethodFactory != null) {
				InvocableHandlerMethod handler = messageHandlerMethodFactory
						.createInvocableHandlerMethod(getBean(), method);
				invocableHandlerMethods.add(handler);
				if (method.equals(this.defaultMethod)) {
					defaultHandler = handler;
				}
			}
		}
		DelegatingInvocableHandler delegatingHandler = new DelegatingInvocableHandler(invocableHandlerMethods,
				defaultHandler, getBean(), getResolver(), getBeanExpressionContext(), getBeanFactory(), this.validator);
		return new HandlerAdapter(delegatingHandler);
	}

}
