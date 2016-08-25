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

import java.lang.reflect.Method;
import java.util.List;

import org.apache.kafka.clients.consumer.ConsumerRecord;

import org.springframework.kafka.listener.BatchAcknowledgingMessageListener;
import org.springframework.kafka.listener.BatchMessageListener;
import org.springframework.kafka.listener.MessageListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.kafka.support.KafkaNull;
import org.springframework.kafka.support.converter.BatchMessageConverter;
import org.springframework.kafka.support.converter.BatchMessagingMessageConverter;
import org.springframework.messaging.Message;
import org.springframework.messaging.support.GenericMessage;


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
public class BatchMessagingMessageListenerAdapter<K, V> extends MessagingMessageListenerAdapter<K, V>
		implements BatchMessageListener<K, V>, BatchAcknowledgingMessageListener<K, V> {

	private static final Message<KafkaNull> NULL_MESSAGE = new GenericMessage<>(KafkaNull.INSTANCE);

	@SuppressWarnings("rawtypes")
	private BatchMessageConverter messageConverter = new BatchMessagingMessageConverter();


	public BatchMessagingMessageListenerAdapter(Method method) {
		super(method);
	}

	/**
	 * Set the BatchMessageConverter.
	 * @param messageConverter the converter.
	 */
	@SuppressWarnings("rawtypes")
	public void setMessageConverter(BatchMessageConverter messageConverter) {
		this.messageConverter = messageConverter;
	}

	/**
	 * Return the {@link BatchMessagingMessageConverter} for this listener,
	 * being able to convert {@link org.springframework.messaging.Message}.
	 * @return the {@link BatchMessagingMessageConverter} for this listener,
	 * being able to convert {@link org.springframework.messaging.Message}.
	 */
	@SuppressWarnings("rawtypes")
	protected final BatchMessageConverter getMessageConverter() {
		return this.messageConverter;
	}

	/**
	 * Kafka {@link MessageListener} entry point.
	 * <p> Delegate the message to the target listener method,
	 * with appropriate conversion of the message argument.
	 * @param records the incoming Kafka {@link ConsumerRecord}s.
	 * @see #handleListenerException
	 */
	@Override
	public void onMessage(List<ConsumerRecord<K, V>> records) {
		onMessage(records, null);
	}

	@Override
	public void onMessage(List<ConsumerRecord<K, V>> records, Acknowledgment acknowledgment) {
		Message<?> message;
		if (!isConsumerRecordList()) {
			message = toMessagingMessage(records, acknowledgment);
		}
		else {
			message = NULL_MESSAGE;
		}
		if (logger.isDebugEnabled()) {
			logger.debug("Processing [" + message + "]");
		}
		invokeHandler(records, acknowledgment, message);
	}

	@SuppressWarnings("unchecked")
	protected Message<?> toMessagingMessage(List<ConsumerRecord<K, V>> records, Acknowledgment acknowledgment) {
		return getMessageConverter().toMessage(records, acknowledgment, this.inferredType);
	}

}
