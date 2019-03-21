/*
 * Copyright 2002-2016 the original author or authors.
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

import java.lang.reflect.Method;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.lang.reflect.WildcardType;

import org.apache.kafka.clients.consumer.ConsumerRecord;

import org.springframework.core.MethodParameter;
import org.springframework.kafka.listener.ListenerExecutionFailedException;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.kafka.support.converter.MessageConverter;
import org.springframework.kafka.support.converter.MessagingMessageConverter;
import org.springframework.messaging.Message;
import org.springframework.messaging.MessagingException;
import org.springframework.messaging.converter.MessageConversionException;
import org.springframework.messaging.handler.annotation.Payload;


/**
 * A {@link org.springframework.kafka.listener.MessageListener MessageListener}
 * adapter that invokes a configurable {@link HandlerAdapter}.
 *
 * <p>Wraps the incoming Kafka Message to Spring's {@link Message} abstraction.
 *
 * <p>The original {@link ConsumerRecord} and
 * the {@link Acknowledgment} are provided as additional arguments so that these can
 * be injected as method arguments if necessary.
 *
 * @param <K> the key type.
 * @param <V> the value type.
 *
 * @author Stephane Nicoll
 * @author Gary Russell
 * @author Artem Bilan
 */
public class MessagingMessageListenerAdapter<K, V> extends AbstractAdaptableMessageListener<K, V> {

	private final Type inferredType;

	private HandlerAdapter handlerMethod;

	private MessageConverter messageConverter = new MessagingMessageConverter();


	public MessagingMessageListenerAdapter(Method method) {
		this.inferredType = determineInferredType(method);
	}

	/**
	 * Set the {@link HandlerAdapter} to use to invoke the method
	 * processing an incoming {@link ConsumerRecord}.
	 * @param handlerMethod {@link HandlerAdapter} instance.
	 */
	public void setHandlerMethod(HandlerAdapter handlerMethod) {
		this.handlerMethod = handlerMethod;
	}

	/**
	 * Set the MessageConverter.
	 * @param messageConverter the converter.
	 */
	public void setMessageConverter(MessageConverter messageConverter) {
		this.messageConverter = messageConverter;
	}

	/**
	 * Return the {@link MessagingMessageConverter} for this listener,
	 * being able to convert {@link org.springframework.messaging.Message}.
	 * @return the {@link MessagingMessageConverter} for this listener,
	 * being able to convert {@link org.springframework.messaging.Message}.
	 */
	protected final MessageConverter getMessageConverter() {
		return this.messageConverter;
	}


	@Override
	public void onMessage(ConsumerRecord<K, V> record, Acknowledgment acknowledgment) {
		Message<?> message = toMessagingMessage(record, acknowledgment);
		if (logger.isDebugEnabled()) {
			logger.debug("Processing [" + message + "]");
		}
		invokeHandler(record, acknowledgment, message);
	}

	protected Message<?> toMessagingMessage(ConsumerRecord<K, V> record, Acknowledgment acknowledgment) {
		return getMessageConverter().toMessage(record, acknowledgment, this.inferredType);
	}

	/**
	 * Invoke the handler, wrapping any exception to a {@link ListenerExecutionFailedException}
	 * with a dedicated error message.
	 * @param record the record to process during invocation.
	 * @param acknowledgment the acknowledgment to use if any.
	 * @param message the message to process.
	 * @return the result of invocation.
	 */
	private Object invokeHandler(ConsumerRecord<K, V> record, Acknowledgment acknowledgment, Message<?> message) {
		try {
			return this.handlerMethod.invoke(message, record, acknowledgment);
		}
		catch (org.springframework.messaging.converter.MessageConversionException ex) {
			throw new ListenerExecutionFailedException(createMessagingErrorMessage("Listener method could not " +
					"be invoked with the incoming message", message.getPayload()),
					new MessageConversionException("Cannot handle message", ex));
		}
		catch (MessagingException ex) {
			throw new ListenerExecutionFailedException(createMessagingErrorMessage("Listener method could not " +
					"be invoked with the incoming message", message.getPayload()), ex);
		}
		catch (Exception ex) {
			throw new ListenerExecutionFailedException("Listener method '" +
					this.handlerMethod.getMethodAsString(message.getPayload()) + "' threw exception", ex);
		}
	}

	private String createMessagingErrorMessage(String description, Object payload) {
		return description + "\n"
				+ "Endpoint handler details:\n"
				+ "Method [" + this.handlerMethod.getMethodAsString(payload) + "]\n"
				+ "Bean [" + this.handlerMethod.getBean() + "]";
	}

	private Type determineInferredType(Method method) {
		if (method == null) {
			return null;
		}

		Type genericParameterType = null;

		for (int i = 0; i < method.getParameterTypes().length; i++) {
			MethodParameter methodParameter = new MethodParameter(method, i);
			/*
			 * We're looking for a single non-annotated parameter, or one annotated with @Payload.
			 * We ignore parameters with type Message because they are not involved with conversion.
			 */
			if (eligibleParameter(methodParameter)
					&& (methodParameter.getParameterAnnotations().length == 0
					|| methodParameter.hasParameterAnnotation(Payload.class))) {
				if (genericParameterType == null) {
					genericParameterType = methodParameter.getGenericParameterType();
					if (genericParameterType instanceof ParameterizedType) {
						ParameterizedType parameterizedType = (ParameterizedType) genericParameterType;
						if (parameterizedType.getRawType().equals(Message.class)) {
							genericParameterType = ((ParameterizedType) genericParameterType)
								.getActualTypeArguments()[0];
						}
					}
				}
				else {
					if (this.logger.isDebugEnabled()) {
						this.logger.debug("Ambiguous parameters for target payload for method " + method
										+ "; no inferred type available");
					}
					return null;
				}
			}
		}

		return genericParameterType;
	}

	/*
	 * Don't consider parameter types that are available after conversion.
	 * Acknowledgment, ConsumerRecord and Message<?>.
	 */
	private boolean eligibleParameter(MethodParameter methodParameter) {
		Type parameterType = methodParameter.getGenericParameterType();
		if (parameterType.equals(Acknowledgment.class) || parameterType.equals(ConsumerRecord.class)) {
			return false;
		}
		if (parameterType instanceof ParameterizedType) {
			ParameterizedType parameterizedType = (ParameterizedType) parameterType;
			if (parameterizedType.getRawType().equals(Message.class)) {
				return !(parameterizedType.getActualTypeArguments()[0] instanceof WildcardType);
			}
		}
		return !parameterType.equals(Message.class); // could be Message without a generic type
	}

}
