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

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.jspecify.annotations.Nullable;

import org.springframework.kafka.listener.KafkaListenerErrorHandler;
import org.springframework.kafka.listener.MessageListener;

/**
 * A {@link MessageListener} adapter for the share consumer model that invokes a configurable
 * {@link HandlerAdapter}. This adapter passes null for acknowledgment and consumer parameters
 * to the super implementation.
 *
 * @param <K> the key type.
 * @param <V> the value type.
 *
 * @author Soby Chacko
 * @since 4.0
 */
public class ShareRecordMessagingMessageListenerAdapter<K, V> extends RecordMessagingMessageListenerAdapter<K, V>
		implements MessageListener<K, V> {

	public ShareRecordMessagingMessageListenerAdapter(@Nullable Object bean, @Nullable Method method) {
		super(bean, method);
	}

	public ShareRecordMessagingMessageListenerAdapter(@Nullable Object bean, @Nullable Method method,
			@Nullable KafkaListenerErrorHandler errorHandler) {
		super(bean, method, errorHandler);
	}

	/**
	 * Kafka {@link MessageListener} entry point.
	 * <p> Delegate the message to the target listener method,
	 * with appropriate conversion of the message argument.
	 * <p> This implementation passes null for both acknowledgment and consumer parameters
	 * to the super implementation.
	 * @param record the incoming Kafka {@link ConsumerRecord}.
	 */
	@Override
	public void onMessage(ConsumerRecord<K, V> record) {
		onMessage(record, null, null);
	}

}
