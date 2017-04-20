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

import org.springframework.retry.RecoveryCallback;
import org.springframework.retry.support.RetryTemplate;
import org.springframework.util.Assert;

/**
 * Base class for retrying message listener adapters.
 *
 * @param <K> the key type.
 * @param <V> the value type.
 * @param <T> the delegate type.
 *
 * @author Gary Russell
 *
 */
public abstract class AbstractRetryingMessageListenerAdapter<K, V, T>
		extends AbstractDelegatingMessageListenerAdapter<T> {

	private final RetryTemplate retryTemplate;

	private final RecoveryCallback<? extends Object> recoveryCallback;

	/**
	 * Construct an instance with the supplied retry template. The exception will be
	 * thrown to the container after retries are exhausted.
	 * @param delegate the delegate listener.
	 * @param retryTemplate the template.
	 */
	public AbstractRetryingMessageListenerAdapter(T delegate, RetryTemplate retryTemplate) {
		this(delegate, retryTemplate, null);
	}

	/**
	 * Construct an instance with the supplied template and callback.
	 * @param delegate the delegate listener.
	 * @param retryTemplate the template.
	 * @param recoveryCallback the recovery callback; if null, the exception will be
	 * thrown to the container after retries are exhausted.
	 */
	public AbstractRetryingMessageListenerAdapter(T delegate, RetryTemplate retryTemplate,
			RecoveryCallback<? extends Object> recoveryCallback) {
		super(delegate);
		Assert.notNull(retryTemplate, "'retryTemplate' cannot be null");
		this.retryTemplate = retryTemplate;
		this.recoveryCallback = recoveryCallback;
	}

	public RetryTemplate getRetryTemplate() {
		return this.retryTemplate;
	}

	public RecoveryCallback<? extends Object> getRecoveryCallback() {
		return this.recoveryCallback;
	}

}
