/*
 * Copyright 2002-2017 the original author or authors.
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

import java.lang.reflect.Method;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import org.springframework.kafka.listener.AcknowledgingConsumerAwareMessageListener;
import org.springframework.kafka.listener.KafkaListenerErrorHandler;
import org.springframework.kafka.listener.ListenerExecutionFailedException;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.messaging.Message;


/**
 * A {@link org.springframework.kafka.listener.MessageListener MessageListener}
 * adapter that invokes a configurable {@link HandlerAdapter}; used when the factory is
 * configured for the listener to receive individual messages.
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
 * @author Venil Noronha
 */
public class RecordMessagingMessageListenerAdapter<K, V> extends MessagingMessageListenerAdapter<K, V>
		implements AcknowledgingConsumerAwareMessageListener<K, V> {

	private KafkaListenerErrorHandler errorHandler;

	public RecordMessagingMessageListenerAdapter(Object bean, Method method) {
		this(bean, method, null);
	}

	public RecordMessagingMessageListenerAdapter(Object bean, Method method, KafkaListenerErrorHandler errorHandler) {
		super(bean, method);
		this.errorHandler = errorHandler;
	}

	/**
	 * Kafka {@link AcknowledgingConsumerAwareMessageListener} entry point.
	 * <p> Delegate the message to the target listener method,
	 * with appropriate conversion of the message argument.
	 * @param record the incoming Kafka {@link ConsumerRecord}.
	 * @param acknowledgment the acknowledgment.
	 * @param consumer the consumer.
	 */
	@Override
	public void onMessage(ConsumerRecord<K, V> record, Acknowledgment acknowledgment, Consumer<?, ?> consumer) {
		Message<?> message = toMessagingMessage(record, acknowledgment, consumer);
		if (logger.isDebugEnabled()) {
			logger.debug("Processing [" + message + "]");
		}
		try {
			Object result = invokeHandler(record, acknowledgment, message, consumer);
			if (result != null) {
				handleResult(result, record, message);
			}
		}
		catch (ListenerExecutionFailedException e) { // NOSONAR ex flow control
			if (this.errorHandler != null) {
				try {
					Object result = this.errorHandler.handleError(message, e, consumer);
					if (result != null) {
						handleResult(result, record, message);
					}
				}
				catch (Exception ex) {
					throw new ListenerExecutionFailedException(createMessagingErrorMessage(// NOSONAR stack trace loss
							"Listener error handler threw an exception for the incoming message",
							message.getPayload()), ex);
				}
			}
			else {
				throw e;
			}
		}
	}

}
