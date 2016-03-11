/*
 * Copyright 2002-2016 the original author or authors.
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

package org.springframework.kafka.listener.adapter;

import org.apache.kafka.clients.consumer.ConsumerRecord;

import org.springframework.kafka.listener.ListenerExecutionFailedException;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.kafka.support.converter.MessageConverter;
import org.springframework.kafka.support.converter.MessagingMessageConverter;
import org.springframework.messaging.Message;
import org.springframework.messaging.MessagingException;
import org.springframework.messaging.converter.MessageConversionException;

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

	private HandlerAdapter handlerMethod;

	private MessageConverter<K, V> messageConverter = new MessagingMessageConverter<>();

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
	public void setMessageConverter(MessageConverter<K, V> messageConverter) {
		this.messageConverter = messageConverter;
	}

	/**
	 * Return the {@link MessagingMessageConverter} for this listener,
	 * being able to convert {@link org.springframework.messaging.Message}.
	 * @return the {@link MessagingMessageConverter} for this listener,
	 * being able to convert {@link org.springframework.messaging.Message}.
	 */
	protected final MessageConverter<K, V> getMessageConverter() {
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
		return getMessageConverter().toMessage(record, acknowledgment);
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

}
