/*
 * Copyright 2002-present the original author or authors.
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

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.jspecify.annotations.Nullable;

import org.springframework.kafka.listener.AcknowledgingConsumerAwareMessageListener;
import org.springframework.kafka.listener.KafkaListenerErrorHandler;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.kafka.support.converter.JacksonProjectingMessageConverter;
import org.springframework.kafka.support.converter.ProjectingMessageConverter;
import org.springframework.kafka.support.converter.RecordMessageConverter;
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

	public RecordMessagingMessageListenerAdapter(@Nullable Object bean, @Nullable Method method) {
		this(bean, method, null);
	}

	public RecordMessagingMessageListenerAdapter(@Nullable Object bean, @Nullable Method method,
			@Nullable KafkaListenerErrorHandler errorHandler) {

		super(bean, method, errorHandler);
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
	@SuppressWarnings("removal")
	public void onMessage(ConsumerRecord<K, V> record, @Nullable Acknowledgment acknowledgment,
			@Nullable Consumer<?, ?> consumer) {

		Message<?> message;
		if (isConversionNeeded()) {
			message = toMessagingMessage(record, acknowledgment, consumer);
		}
		else {
			message = NULL_MESSAGE;
		}
		if (logger.isDebugEnabled()) {
			RecordMessageConverter messageConverter = getMessageConverter();
			if (!(messageConverter instanceof JacksonProjectingMessageConverter
					|| messageConverter instanceof ProjectingMessageConverter)) {
				this.logger.debug("Processing [" + message + "]");
			}
		}
		invoke(record, acknowledgment, consumer, message);
	}

}
