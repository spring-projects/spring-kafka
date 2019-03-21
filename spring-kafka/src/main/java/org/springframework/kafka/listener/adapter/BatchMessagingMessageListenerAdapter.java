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
import java.util.ArrayList;
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
import org.springframework.messaging.support.MessageBuilder;


/**
 * A {@link org.springframework.kafka.listener.MessageListener MessageListener}
 * adapter that invokes a configurable {@link HandlerAdapter}; used when the factory is
 * configured for the listener to receive batches of messages.
 *
 * <p>Wraps the incoming Kafka Message to Spring's {@link Message} abstraction.
 *
 * <p>The original {@code List<ConsumerRecord>} and
 * the {@link Acknowledgment} are provided as additional arguments so that these can
 * be injected as method arguments if necessary.
 *
 * @param <K> the key type.
 * @param <V> the value type.
 *
 * @author Stephane Nicoll
 * @author Gary Russell
 * @author Artem Bilan
 * @since 1.1
 */
public class BatchMessagingMessageListenerAdapter<K, V> extends MessagingMessageListenerAdapter<K, V>
		implements BatchMessageListener<K, V>, BatchAcknowledgingMessageListener<K, V> {

	private static final Message<KafkaNull> NULL_MESSAGE = new GenericMessage<>(KafkaNull.INSTANCE);

	private BatchMessageConverter messageConverter = new BatchMessagingMessageConverter();


	public BatchMessagingMessageListenerAdapter(Object bean, Method method) {
		super(bean, method);
	}

	/**
	 * Set the BatchMessageConverter.
	 * @param messageConverter the converter.
	 */
	public void setBatchMessageConverter(BatchMessageConverter messageConverter) {
		this.messageConverter = messageConverter;
	}

	/**
	 * Return the {@link BatchMessagingMessageConverter} for this listener,
	 * being able to convert {@link org.springframework.messaging.Message}.
	 * @return the {@link BatchMessagingMessageConverter} for this listener,
	 * being able to convert {@link org.springframework.messaging.Message}.
	 */
	protected final BatchMessageConverter getBatchMessageConverter() {
		return this.messageConverter;
	}

	/**
	 * Kafka {@link MessageListener} entry point.
	 * <p> Delegate the message to the target listener method,
	 * with appropriate conversion of the message argument.
	 * @param records the incoming Kafka {@link ConsumerRecord}s.
	 */
	@Override
	public void onMessage(List<ConsumerRecord<K, V>> records) {
		onMessage(records, null);
	}

	@Override
	public void onMessage(List<ConsumerRecord<K, V>> records, Acknowledgment acknowledgment) {
		Message<?> message;
		if (!isConsumerRecordList()) {
			if (isMessageList()) {
				List<Message<?>> messages = new ArrayList<>(records.size());
				for (ConsumerRecord<K, V> record : records) {
					messages.add(toMessagingMessage(record, acknowledgment));
				}
				message = MessageBuilder.withPayload(messages).build();
			}
			else {
				message = toMessagingMessage(records, acknowledgment);
			}
		}
		else {
			message = NULL_MESSAGE;
		}
		if (logger.isDebugEnabled()) {
			logger.debug("Processing [" + message + "]");
		}
		invokeHandler(records, acknowledgment, message);
	}

	@SuppressWarnings({ "unchecked", "rawtypes" })
	protected Message<?> toMessagingMessage(List records, Acknowledgment acknowledgment) {
		return getBatchMessageConverter().toMessage(records, acknowledgment, getType());
	}

}
