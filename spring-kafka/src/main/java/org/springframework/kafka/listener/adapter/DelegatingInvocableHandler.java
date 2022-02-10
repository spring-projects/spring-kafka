/*
 * Copyright 2016-2022 the original author or authors.
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

import java.lang.annotation.Annotation;
import java.lang.reflect.Method;
import java.lang.reflect.Parameter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import org.springframework.beans.factory.BeanFactory;
import org.springframework.beans.factory.config.BeanExpressionContext;
import org.springframework.beans.factory.config.BeanExpressionResolver;
import org.springframework.beans.factory.config.ConfigurableListableBeanFactory;
import org.springframework.core.MethodParameter;
import org.springframework.core.annotation.AnnotationUtils;
import org.springframework.expression.Expression;
import org.springframework.expression.spel.standard.SpelExpressionParser;
import org.springframework.kafka.KafkaException;
import org.springframework.kafka.support.KafkaUtils;
import org.springframework.lang.Nullable;
import org.springframework.messaging.Message;
import org.springframework.messaging.MessageHeaders;
import org.springframework.messaging.converter.MessageConverter;
import org.springframework.messaging.handler.HandlerMethod;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.SendTo;
import org.springframework.messaging.handler.annotation.support.PayloadMethodArgumentResolver;
import org.springframework.messaging.handler.invocation.InvocableHandlerMethod;
import org.springframework.util.Assert;
import org.springframework.validation.Validator;


/**
 * Delegates to an {@link InvocableHandlerMethod} based on the message payload type.
 * Matches a single, non-annotated parameter or one that is annotated with
 * {@link org.springframework.messaging.handler.annotation.Payload}. Matches must be
 * unambiguous.
 *
 * @author Gary Russell
 *
 */
public class DelegatingInvocableHandler {

	private static final SpelExpressionParser PARSER = new SpelExpressionParser();

	private final List<InvocableHandlerMethod> handlers;

	private final ConcurrentMap<Class<?>, InvocableHandlerMethod> cachedHandlers = new ConcurrentHashMap<>();

	private final ConcurrentMap<InvocableHandlerMethod, MethodParameter> payloadMethodParameters =
			new ConcurrentHashMap<>();

	private final InvocableHandlerMethod defaultHandler;

	private final Map<InvocableHandlerMethod, Expression> handlerSendTo = new ConcurrentHashMap<>();

	private final Map<InvocableHandlerMethod, Boolean> handlerReturnsMessage = new ConcurrentHashMap<>();

	private final Object bean;

	private final BeanExpressionResolver resolver;

	private final BeanExpressionContext beanExpressionContext;

	private final ConfigurableListableBeanFactory beanFactory;

	private final PayloadValidator validator;

	/**
	 * Construct an instance with the supplied handlers for the bean.
	 * @param handlers the handlers.
	 * @param defaultHandler the default handler.
	 * @param bean the bean.
	 * @param beanExpressionResolver the resolver.
	 * @param beanExpressionContext the context.
	 * @param beanFactory the bean factory.
	 * @param validator the validator.
	 * @since 2.5.11
	 */
	public DelegatingInvocableHandler(List<InvocableHandlerMethod> handlers,
			@Nullable InvocableHandlerMethod defaultHandler, Object bean,
			@Nullable BeanExpressionResolver beanExpressionResolver,
			@Nullable BeanExpressionContext beanExpressionContext,
			@Nullable BeanFactory beanFactory, @Nullable Validator validator) {

		this.handlers = new ArrayList<>();
		for (InvocableHandlerMethod handler : handlers) {
			this.handlers.add(wrapIfNecessary(handler));
		}
		this.defaultHandler = wrapIfNecessary(defaultHandler);
		this.bean = bean;
		this.resolver = beanExpressionResolver;
		this.beanExpressionContext = beanExpressionContext;
		this.beanFactory = beanFactory instanceof ConfigurableListableBeanFactory
				? (ConfigurableListableBeanFactory) beanFactory
				: null;
		this.validator = validator == null ? null : new PayloadValidator(validator);
	}

	@Nullable
	private InvocableHandlerMethod wrapIfNecessary(@Nullable InvocableHandlerMethod handler) {
		if (handler == null) {
			return null;
		}
		Parameter[] parameters = handler.getMethod().getParameters();
		for (Parameter parameter : parameters) {
			if (parameter.getType().equals(ConsumerRecordMetadata.class)) {
				return new DelegatingInvocableHandler.MetadataAwareInvocableHandlerMethod(handler);
			}
		}
		return handler;
	}

	/**
	 * Return the bean for this handler.
	 * @return the bean.
	 */
	public Object getBean() {
		return this.bean;
	}

	/**
	 * Invoke the method with the given message.
	 * @param message the message.
	 * @param providedArgs additional arguments.
	 * @return the result of the invocation.
	 * @throws Exception raised if no suitable argument resolver can be found,
	 * or the method raised an exception.
	 */
	public Object invoke(Message<?> message, Object... providedArgs) throws Exception { //NOSONAR
		Class<? extends Object> payloadClass = message.getPayload().getClass();
		InvocableHandlerMethod handler = getHandlerForPayload(payloadClass);
		if (this.validator != null && this.defaultHandler != null) {
			MethodParameter parameter = this.payloadMethodParameters.get(handler);
			if (parameter != null) {
				this.validator.validate(message, parameter, message.getPayload());
			}
		}
		Object result;
		if (handler instanceof MetadataAwareInvocableHandlerMethod) {
			Object[] args = new Object[providedArgs.length + 1];
			args[0] = AdapterUtils.buildConsumerRecordMetadataFromArray(providedArgs);
			System.arraycopy(providedArgs, 0, args, 1, providedArgs.length);
			result = handler.invoke(message, args);
		}
		else {
			result = handler.invoke(message, providedArgs);
		}
		Expression replyTo = this.handlerSendTo.get(handler);
		return new InvocationResult(result, replyTo, this.handlerReturnsMessage.get(handler));
	}

	/**
	 * Determine the {@link InvocableHandlerMethod} for the provided type.
	 * @param payloadClass the payload class.
	 * @return the handler.
	 */
	protected InvocableHandlerMethod getHandlerForPayload(Class<? extends Object> payloadClass) {
		InvocableHandlerMethod handler = this.cachedHandlers.get(payloadClass);
		if (handler == null) {
			handler = findHandlerForPayload(payloadClass);
			if (handler == null) {
				throw new KafkaException("No method found for " + payloadClass);
			}
			this.cachedHandlers.putIfAbsent(payloadClass, handler); //NOSONAR
			setupReplyTo(handler);
		}
		return handler;
	}

	private void setupReplyTo(InvocableHandlerMethod handler) {
		String replyTo = null;
		Method method = handler.getMethod();
		SendTo ann = null;
		if (method != null) {
			ann = AnnotationUtils.getAnnotation(method, SendTo.class);
			replyTo = extractSendTo(method.toString(), ann);
		}
		if (ann == null) {
			Class<?> beanType = handler.getBeanType();
			ann = AnnotationUtils.getAnnotation(beanType, SendTo.class);
			replyTo = extractSendTo(beanType.getSimpleName(), ann);
		}
		if (ann != null && replyTo == null) {
			replyTo = AdapterUtils.getDefaultReplyTopicExpression();
		}
		if (replyTo != null) {
			this.handlerSendTo.put(handler, PARSER.parseExpression(replyTo, AdapterUtils.PARSER_CONTEXT));
		}
		this.handlerReturnsMessage.put(handler, KafkaUtils.returnTypeMessageOrCollectionOf(method));
	}

	@Nullable
	private String extractSendTo(String element, @Nullable SendTo ann) {
		String replyTo = null;
		if (ann != null) {
			String[] destinations = ann.value();
			if (destinations.length > 1) {
				throw new IllegalStateException("Invalid @" + SendTo.class.getSimpleName() + " annotation on '"
						+ element + "' one destination must be set (got " + Arrays.toString(destinations) + ")");
			}
			replyTo = destinations.length == 1 ? destinations[0] : null;
			if (replyTo != null && this.beanFactory != null) {
				replyTo = this.beanFactory.resolveEmbeddedValue(replyTo);
				if (replyTo != null) {
					replyTo = resolve(replyTo);
				}
			}
		}
		return replyTo;
	}

	private String resolve(String value) {
		if (this.resolver != null && this.beanExpressionContext != null) {
			Object newValue = this.resolver.evaluate(value, this.beanExpressionContext);
			Assert.isInstanceOf(String.class, newValue, "Invalid @SendTo expression");
			return (String) newValue;
		}
		else {
			return value;
		}
	}

	@Nullable
	protected InvocableHandlerMethod findHandlerForPayload(Class<? extends Object> payloadClass) {
		InvocableHandlerMethod result = null;
		for (InvocableHandlerMethod handler : this.handlers) {
			if (matchHandlerMethod(payloadClass, handler)) {
				if (result != null) {
					boolean resultIsDefault = result.equals(this.defaultHandler);
					if (!handler.equals(this.defaultHandler) && !resultIsDefault) {
						throw new KafkaException("Ambiguous methods for payload type: " + payloadClass + ": " +
								result.getMethod().getName() + " and " + handler.getMethod().getName());
					}
					if (!resultIsDefault) {
						continue; // otherwise replace the result with the actual match
					}
				}
				result = handler;
			}
		}
		return result != null ? result : this.defaultHandler;
	}

	protected boolean matchHandlerMethod(Class<? extends Object> payloadClass, InvocableHandlerMethod handler) {
		Method method = handler.getMethod();
		Annotation[][] parameterAnnotations = method.getParameterAnnotations();
		// Single param; no annotation or not @Header
		if (parameterAnnotations.length == 1) {
			MethodParameter methodParameter = new MethodParameter(method, 0);
			if ((methodParameter.getParameterAnnotations().length == 0
					|| !methodParameter.hasParameterAnnotation(Header.class))
					&& methodParameter.getParameterType().isAssignableFrom(payloadClass)) {
				if (this.validator != null) {
					this.payloadMethodParameters.put(handler, methodParameter);
				}
				return true;
			}
		}

		MethodParameter foundCandidate = findCandidate(payloadClass, method, parameterAnnotations);
		if (foundCandidate != null && this.validator != null) {
			this.payloadMethodParameters.put(handler, foundCandidate);
		}
		return foundCandidate != null;
	}

	private MethodParameter findCandidate(Class<? extends Object> payloadClass, Method method,
			Annotation[][] parameterAnnotations) {
		MethodParameter foundCandidate = null;
		for (int i = 0; i < parameterAnnotations.length; i++) {
			MethodParameter methodParameter = new MethodParameter(method, i);
			if ((methodParameter.getParameterAnnotations().length == 0
					|| !methodParameter.hasParameterAnnotation(Header.class))
					&& methodParameter.getParameterType().isAssignableFrom(payloadClass)) {
				if (foundCandidate != null) {
					throw new KafkaException("Ambiguous payload parameter for " + method.toGenericString());
				}
				foundCandidate = methodParameter;
			}
		}
		return foundCandidate;
	}

	/**
	 * Return a string representation of the method that will be invoked for this payload.
	 * @param payload the payload.
	 * @return the method name.
	 */
	public String getMethodNameFor(Object payload) {
		InvocableHandlerMethod handlerForPayload = getHandlerForPayload(payload.getClass());
		return handlerForPayload == null ? "no match" : handlerForPayload.getMethod().toGenericString(); //NOSONAR
	}

	public boolean hasDefaultHandler() {
		return this.defaultHandler != null;
	}

	/**
	 * A handler method that is aware of {@link ConsumerRecordMetadata}.
	 *
	 * @since 2.5
	 */
	private static final class MetadataAwareInvocableHandlerMethod extends InvocableHandlerMethod {

		MetadataAwareInvocableHandlerMethod(HandlerMethod handlerMethod) {
			super(handlerMethod);
		}

	}

	private static final class PayloadValidator extends PayloadMethodArgumentResolver {

		PayloadValidator(Validator validator) {
			super(new MessageConverter() { // Required but never used

				@Override
				@Nullable
				public Message<?> toMessage(Object payload, @Nullable
				MessageHeaders headers) {
					return null;
				}

				@Override
				@Nullable
				public Object fromMessage(Message<?> message, Class<?> targetClass) {
					return null;
				}

			}, validator);
		}

		@Override
		public void validate(Message<?> message, MethodParameter parameter, Object target) { // NOSONAR - public
			super.validate(message, parameter, target);
		}

	}

}
