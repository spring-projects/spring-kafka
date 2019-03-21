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

package org.springframework.kafka.listener.adapter;

import java.lang.reflect.Method;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.lang.reflect.WildcardType;
import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;

import org.springframework.core.MethodParameter;
import org.springframework.kafka.listener.ConsumerSeekAware;
import org.springframework.kafka.listener.ListenerExecutionFailedException;
import org.springframework.kafka.listener.MessageListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.kafka.support.converter.MessagingMessageConverter;
import org.springframework.kafka.support.converter.RecordMessageConverter;
import org.springframework.messaging.Message;
import org.springframework.messaging.MessagingException;
import org.springframework.messaging.converter.MessageConversionException;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.util.Assert;

/**
 * An abstract {@link MessageListener} adapter providing the necessary infrastructure
 * to extract the payload of a {@link org.springframework.messaging.Message}.
 *
 * @param <K> the key type.
 * @param <V> the value type.
 *
 * @author Stephane Nicoll
 * @author Gary Russell
 * @author Artem Bilan
 *
 */
public abstract class MessagingMessageListenerAdapter<K, V> implements ConsumerSeekAware {

	private final Object bean;

	protected final Log logger = LogFactory.getLog(getClass()); //NOSONAR

	private final Type inferredType;

	private HandlerAdapter handlerMethod;

	private boolean isConsumerRecordList;

	private boolean isMessageList;

	private RecordMessageConverter messageConverter = new MessagingMessageConverter();

	private Type fallbackType = Object.class;


	public MessagingMessageListenerAdapter(Object bean, Method method) {
		this.bean = bean;
		this.inferredType = determineInferredType(method);
	}

	/**
	 * Set the MessageConverter.
	 * @param messageConverter the converter.
	 */
	public void setMessageConverter(RecordMessageConverter messageConverter) {
		this.messageConverter = messageConverter;
	}

	/**
	 * Return the {@link MessagingMessageConverter} for this listener,
	 * being able to convert {@link org.springframework.messaging.Message}.
	 * @return the {@link MessagingMessageConverter} for this listener,
	 * being able to convert {@link org.springframework.messaging.Message}.
	 */
	protected final RecordMessageConverter getMessageConverter() {
		return this.messageConverter;
	}

	/**
	 * Returns the inferred type for conversion or, if null, the
	 * {@link #setFallbackType(Class) fallbackType}.
	 * @return the type.
	 */
	protected Type getType() {
		return this.inferredType == null ? this.fallbackType : this.inferredType;
	}

	/**
	 * Set a fallback type to use when using a type-aware message converter and this
	 * adapter cannot determine the inferred type from the method. An example of a
	 * type-aware message converter is the {@code StringJsonMessageConverter}. Defaults to
	 * {@link Object}.
	 * @param fallbackType the type.
	 */
	public void setFallbackType(Class<?> fallbackType) {
		this.fallbackType = fallbackType;
	}

	/**
	 * Set the {@link HandlerAdapter} to use to invoke the method
	 * processing an incoming {@link ConsumerRecord}.
	 * @param handlerMethod {@link HandlerAdapter} instance.
	 */
	public void setHandlerMethod(HandlerAdapter handlerMethod) {
		this.handlerMethod = handlerMethod;
	}

	protected boolean isConsumerRecordList() {
		return this.isConsumerRecordList;
	}

	protected boolean isMessageList() {
		return this.isMessageList;
	}

	@Override
	public void registerSeekCallback(ConsumerSeekCallback callback) {
		if (this.bean instanceof ConsumerSeekAware) {
			((ConsumerSeekAware) this.bean).registerSeekCallback(callback);
		}
	}

	@Override
	public void onPartitionsAssigned(Map<TopicPartition, Long> assignments, ConsumerSeekCallback callback) {
		if (this.bean instanceof ConsumerSeekAware) {
			((ConsumerSeekAware) this.bean).onPartitionsAssigned(assignments, callback);
		}
	}

	@Override
	public void onIdleContainer(Map<TopicPartition, Long> assignments, ConsumerSeekCallback callback) {
		if (this.bean instanceof ConsumerSeekAware) {
			((ConsumerSeekAware) this.bean).onIdleContainer(assignments, callback);
		}
	}

	protected Message<?> toMessagingMessage(ConsumerRecord<K, V> record, Acknowledgment acknowledgment) {
		return getMessageConverter().toMessage(record, acknowledgment, getType());
	}

	/**
	 * Invoke the handler, wrapping any exception to a {@link ListenerExecutionFailedException}
	 * with a dedicated error message.
	 * @param data the data to process during invocation.
	 * @param acknowledgment the acknowledgment to use if any.
	 * @param message the message to process.
	 * @return the result of invocation.
	 */
	protected final Object invokeHandler(Object data, Acknowledgment acknowledgment, Message<?> message) {
		try {
			if (data instanceof List && !this.isConsumerRecordList) {
				return this.handlerMethod.invoke(message, acknowledgment);
			}
			else {
				return this.handlerMethod.invoke(message, data, acknowledgment);
			}
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

	/**
	 * Subclasses can override this method to use a different mechanism to determine
	 * the target type of the payload conversion.
	 * @param method the method.
	 * @return the type.
	 */
	protected Type determineInferredType(Method method) {
		if (method == null) {
			return null;
		}

		Type genericParameterType = null;
		boolean hasAck = false;

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
						else if (parameterizedType.getRawType().equals(List.class)
								&& parameterizedType.getActualTypeArguments().length == 1) {
							Type paramType = parameterizedType.getActualTypeArguments()[0];
							this.isConsumerRecordList =	paramType.equals(ConsumerRecord.class)
									|| (paramType instanceof ParameterizedType
										&& ((ParameterizedType) paramType).getRawType().equals(ConsumerRecord.class));
							this.isMessageList = paramType.equals(Message.class)
									|| (paramType instanceof ParameterizedType
											&& ((ParameterizedType) paramType).getRawType().equals(Message.class));
						}
					}
				}
				else {
					if (this.logger.isDebugEnabled()) {
						this.logger.debug("Ambiguous parameters for target payload for method " + method
										+ "; no inferred type available");
					}
					break;
				}
			}
			else if (methodParameter.getGenericParameterType().equals(Acknowledgment.class)) {
				hasAck = true;
			}
		}
		Assert.state(!this.isConsumerRecordList || method.getParameterTypes().length == 1
					|| (method.getGenericParameterTypes().length == 2 && hasAck),
				"A parameter of type 'List<ConsumerRecord>' must be the only parameter "
				+ "(except for an optional 'Acknowledgment')");
		Assert.state(!this.isMessageList || method.getParameterTypes().length == 1
				|| (method.getGenericParameterTypes().length == 2 && hasAck),
			"A parameter of type 'List<Message<?>>' must be the only parameter "
			+ "(except for an optional 'Acknowledgment')");

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
