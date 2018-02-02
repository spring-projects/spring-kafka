/*
 * Copyright 2016-2018 the original author or authors.
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

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import org.springframework.kafka.listener.AcknowledgingConsumerAwareMessageListener;
import org.springframework.kafka.listener.MessageListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.retry.RecoveryCallback;
import org.springframework.retry.RetryState;
import org.springframework.retry.support.DefaultRetryState;
import org.springframework.retry.support.RetryTemplate;
import org.springframework.util.Assert;

/**
 * A retrying message listener adapter for {@link MessageListener}s.
 *
 * @param <K> the key type.
 * @param <V> the value type.
 *
 * @author Gary Russell
 *
 */
public class RetryingMessageListenerAdapter<K, V>
		extends AbstractRetryingMessageListenerAdapter<K, V, MessageListener<K, V>>
		implements AcknowledgingConsumerAwareMessageListener<K, V> {

	/**
	 * {@link org.springframework.retry.RetryContext} attribute key for an acknowledgment
	 * if the listener is capable of acknowledging.
	 */
	public static final String CONTEXT_ACKNOWLEDGMENT = "acknowledgment";

	/**
	 * {@link org.springframework.retry.RetryContext} attribute key for the consumer if
	 * the listener is consumer-aware.
	 */
	public static final String CONTEXT_CONSUMER = "consumer";

	/**
	 * {@link org.springframework.retry.RetryContext} attribute key for the record.
	 */
	public static final String CONTEXT_RECORD = "record";

	private boolean stateful;

	/**
	 * Construct an instance with the provided template and delegate. The exception will
	 * be thrown to the container after retries are exhausted.
	 * @param messageListener the delegate listener.
	 * @param retryTemplate the template.
	 */
	public RetryingMessageListenerAdapter(MessageListener<K, V> messageListener, RetryTemplate retryTemplate) {
		this(messageListener, retryTemplate, null);
	}

	/**
	 * Construct an instance with the provided template, callback and delegate.
	 * @param messageListener the delegate listener.
	 * @param retryTemplate the template.
	 * @param recoveryCallback the recovery callback; if null, the exception will be
	 * thrown to the container after retries are exhausted.
	 */
	public RetryingMessageListenerAdapter(MessageListener<K, V> messageListener, RetryTemplate retryTemplate,
			RecoveryCallback<? extends Object> recoveryCallback) {
		this(messageListener, retryTemplate, recoveryCallback, false);
	}

	/**
	 * Construct an instance with the provided template, callback and delegate. When using
	 * stateful retry, the retry context key is a concatenated String
	 * {@code topic-partition-offset}. A
	 * {@link org.springframework.kafka.listener.SeekToCurrentErrorHandler} is required in
	 * the listener container because stateful retry will throw the exception to the
	 * container for each delivery attempt.
	 * @param messageListener the delegate listener.
	 * @param retryTemplate the template.
	 * @param recoveryCallback the recovery callback; if null, the exception will be
	 * thrown to the container after retries are exhausted.
	 * @param stateful true for stateful retry.
	 * @since 2.1.3
	 */
	public RetryingMessageListenerAdapter(MessageListener<K, V> messageListener, RetryTemplate retryTemplate,
			RecoveryCallback<? extends Object> recoveryCallback, boolean stateful) {

		super(messageListener, retryTemplate, recoveryCallback);
		Assert.notNull(messageListener, "'messageListener' cannot be null");
		this.stateful = stateful;
	}

	@Override
	public void onMessage(final ConsumerRecord<K, V> record, final Acknowledgment acknowledgment,
			final Consumer<?, ?> consumer) {
		RetryState retryState = null;
		if (this.stateful) {
			retryState = new DefaultRetryState(record.topic() + "-" + record.partition() + "-" + record.offset());
		}
		getRetryTemplate().execute(context -> {
					context.setAttribute(CONTEXT_RECORD, record);
					switch (RetryingMessageListenerAdapter.this.delegateType) {
						case ACKNOWLEDGING_CONSUMER_AWARE:
							context.setAttribute(CONTEXT_ACKNOWLEDGMENT, acknowledgment);
							context.setAttribute(CONTEXT_CONSUMER, consumer);
							RetryingMessageListenerAdapter.this.delegate.onMessage(record, acknowledgment, consumer);
							break;
						case ACKNOWLEDGING:
							context.setAttribute(CONTEXT_ACKNOWLEDGMENT, acknowledgment);
							RetryingMessageListenerAdapter.this.delegate.onMessage(record, acknowledgment);
							break;
						case CONSUMER_AWARE:
							context.setAttribute(CONTEXT_CONSUMER, consumer);
							RetryingMessageListenerAdapter.this.delegate.onMessage(record, consumer);
							break;
						case SIMPLE:
							RetryingMessageListenerAdapter.this.delegate.onMessage(record);
					}
					return null;
				},
				getRecoveryCallback(), retryState);
	}

	/*
	 * Since the container uses the delegate's type to determine which method to call, we
	 * must implement them all.
	 */

	@Override
	public void onMessage(ConsumerRecord<K, V> data) {
		onMessage(data, null, null);
	}

	@Override
	public void onMessage(ConsumerRecord<K, V> data, Acknowledgment acknowledgment) {
		onMessage(data, acknowledgment, null);
	}

	@Override
	public void onMessage(ConsumerRecord<K, V> data, Consumer<?, ?> consumer) {
		onMessage(data, null, consumer);
	}

}
