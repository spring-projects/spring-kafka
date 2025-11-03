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

package org.springframework.kafka.listener.adapter;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.jspecify.annotations.Nullable;

import org.springframework.kafka.listener.BatchAcknowledgingConsumerAwareMessageListener;
import org.springframework.kafka.listener.KafkaListenerErrorHandler;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.kafka.support.converter.BatchMessageConverter;
import org.springframework.kafka.support.converter.BatchMessagingMessageConverter;
import org.springframework.kafka.support.converter.RecordMessageConverter;
import org.springframework.messaging.Message;
import org.springframework.messaging.converter.SmartMessageConverter;
import org.springframework.messaging.support.MessageBuilder;
import org.springframework.util.Assert;

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
 * @author Venil Noronha
 * @author Wang ZhiYang
 * @author Sanghyeok An
 * @author George Mahfoud
 * @since 1.1
 */
public class BatchMessagingMessageListenerAdapter<K, V> extends MessagingMessageListenerAdapter<K, V>
		implements BatchAcknowledgingConsumerAwareMessageListener<K, V> {

	private BatchMessageConverter batchMessageConverter = new BatchMessagingMessageConverter();

	private @Nullable BatchToRecordAdapter<K, V> batchToRecordAdapter;

	/**
	 * Create an instance with the provided parameters.
	 * @param bean the listener bean.
	 * @param method the listener method.
	 */
	public BatchMessagingMessageListenerAdapter(@Nullable Object bean, @Nullable Method method) {
		this(bean, method, null);
	}

	/**
	 * Create an instance with the provided parameters.
	 * @param bean the listener bean.
	 * @param method the listener method.
	 * @param errorHandler the error handler.
	 */
	public BatchMessagingMessageListenerAdapter(@Nullable Object bean, @Nullable Method method,
			@Nullable KafkaListenerErrorHandler errorHandler) {

		super(bean, method, errorHandler);
	}

	/**
	 * Set the BatchMessageConverter.
	 * @param messageConverter the converter.
	 */
	public void setBatchMessageConverter(BatchMessageConverter messageConverter) {
		Assert.notNull(messageConverter, "'messageConverter' cannot be null");
		this.batchMessageConverter = messageConverter;
		RecordMessageConverter recordMessageConverter = messageConverter.getRecordMessageConverter();
		if (recordMessageConverter != null) {
			setMessageConverter(recordMessageConverter);
		}
	}

	/**
	 * Set a {@link BatchToRecordAdapter}.
	 * @param batchToRecordAdapter the adapter.
	 * @since 2.4.2
	 */
	public void setBatchToRecordAdapter(BatchToRecordAdapter<K, V> batchToRecordAdapter) {
		this.batchToRecordAdapter = batchToRecordAdapter;
	}

	/**
	 * Set the {@link SmartMessageConverter} to use with the batch message converter.
	 * <p>
	 * When a {@code SmartMessageConverter} is configured via
	 * {@code @KafkaListener(contentTypeConverter = "...")}, this method ensures it is
	 * properly propagated to the batch converter's record converter for message conversion
	 * in batch listeners.
	 * <p>
	 * This method cannot be called after {@link #setBatchMessageConverter(BatchMessageConverter)}
	 * as it would cause a mutation of the internal {@code batchMessageConverter}. Instead, the
	 * {@link SmartMessageConverter} has to be provided on the external {@link BatchMessageConverter}.
	 * Since {@link BatchMessagingMessageConverter} now
	 * always has a default {@link org.springframework.kafka.support.converter.MessagingMessageConverter},
	 * users can configure the converter via the annotation without needing to set it on the factory.
	 * @param messageConverter the converter to set
	 */
	@Override
	public void setMessagingConverter(SmartMessageConverter messageConverter) {
		super.setMessagingConverter(messageConverter);
		if (this.batchMessageConverter instanceof BatchMessagingMessageConverter batchConverter) {
			batchConverter.setMessagingConverter(messageConverter);
		}
	}

	/**
	 * Return the {@link BatchMessagingMessageConverter} for this listener,
	 * being able to convert {@link org.springframework.messaging.Message}.
	 * @return the {@link BatchMessagingMessageConverter} for this listener,
	 * being able to convert {@link org.springframework.messaging.Message}.
	 */
	protected final BatchMessageConverter getBatchMessageConverter() {
		return this.batchMessageConverter;
	}

	@Override
	public boolean wantsPollResult() {
		return isConsumerRecords();
	}

	@Override
	public void onMessage(ConsumerRecords<K, V> records, @Nullable Acknowledgment acknowledgment,
			Consumer<K, V> consumer) {
		invoke(records, acknowledgment, consumer, NULL_MESSAGE);
	}

	/**
	 * Kafka {@link org.springframework.kafka.listener.MessageListener} entry point.
	 * <p>
	 * Delegate the message to the target listener method, with appropriate conversion of
	 * the message argument.
	 * @param records the incoming list of Kafka {@link ConsumerRecord}.
	 * @param acknowledgment the acknowledgment.
	 * @param consumer the consumer.
	 */
	@Override
	public void onMessage(List<ConsumerRecord<K, V>> records, @Nullable Acknowledgment acknowledgment,
			@Nullable Consumer<?, ?> consumer) {

		Message<?> message;
		if (!isConsumerRecordList()) {
			if (isMessageList() || this.batchToRecordAdapter != null) {
				List<Message<?>> messages = new ArrayList<>(records.size());
				for (ConsumerRecord<K, V> cRecord : records) {
					messages.add(toMessagingMessage(cRecord, acknowledgment, consumer));
				}
				if (this.batchToRecordAdapter == null) {
					message = MessageBuilder.withPayload(messages).build();
				}
				else {
					logger.debug(() -> "Processing " + messages);
					this.batchToRecordAdapter.adapt(messages, records, acknowledgment, consumer, this::invoke);
					return;
				}
			}
			else {
				message = toMessagingMessage(records, acknowledgment, consumer);
			}
		}
		else {
			message = NULL_MESSAGE; // optimization since we won't need any conversion to invoke
		}
		logger.debug(() -> "Processing [" + message + "]");
		invoke(records, acknowledgment, consumer, message);
	}

	@SuppressWarnings({ "unchecked", "rawtypes" })
	protected Message<?> toMessagingMessage(List records, @Nullable Acknowledgment acknowledgment,
			@Nullable Consumer<?, ?> consumer) {

		return getBatchMessageConverter().toMessage(records, acknowledgment, consumer, getType());
	}

}
