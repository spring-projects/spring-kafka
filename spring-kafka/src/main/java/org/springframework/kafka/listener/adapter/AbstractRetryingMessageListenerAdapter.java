/*
 * Copyright 2016 the original author or authors.
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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import org.springframework.beans.factory.BeanNameAware;
import org.springframework.retry.RecoveryCallback;
import org.springframework.retry.RetryContext;
import org.springframework.retry.support.RetryTemplate;
import org.springframework.util.Assert;

/**
 * Base class for retrying message listener adapters.
 *
 * @param <K> the key type.
 * @param <V> the value type.
 *
 * @author Gary Russell
 *
 */
public class AbstractRetryingMessageListenerAdapter<K, V> implements BeanNameAware {

	protected final Log logger = LogFactory.getLog(this.getClass());

	private final RetryTemplate retryTemplate;

	private final RecoveryCallback<Void> recoveryCallback;

	private String beanName = "";

	/**
	 * Construct an instance with the supplied template and callback.
	 * @param retryTemplate the template.
	 * @param recoveryCallback the callback.
	 */
	public AbstractRetryingMessageListenerAdapter(RetryTemplate retryTemplate,
			RecoveryCallback<Void> recoveryCallback) {
		Assert.notNull(retryTemplate, "'retryTemplate' cannot be null");
		this.retryTemplate = retryTemplate;
		this.recoveryCallback = recoveryCallback != null ? recoveryCallback : new RecoveryCallback<Void>() {

			@Override
			public Void recover(RetryContext context) throws Exception {
				@SuppressWarnings("unchecked")
				ConsumerRecord<K, V> record = (ConsumerRecord<K, V>) context.getAttribute("record");
				Throwable lastThrowable = context.getLastThrowable();
				AbstractRetryingMessageListenerAdapter.this.logger.error(
							AbstractRetryingMessageListenerAdapter.this.beanName +
							": Listener threw an exception and retries exhausted for " + record, lastThrowable);
				return null;
			}

		};
	}

	public RetryTemplate getRetryTemplate() {
		return this.retryTemplate;
	}

	public RecoveryCallback<Void> getRecoveryCallback() {
		return this.recoveryCallback;
	}

	@Override
	public void setBeanName(String name) {
		this.beanName = name;
	}

}
