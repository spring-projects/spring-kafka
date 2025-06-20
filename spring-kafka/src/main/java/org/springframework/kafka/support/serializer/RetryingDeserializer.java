/*
 * Copyright 2019-present the original author or authors.
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

package org.springframework.kafka.support.serializer;

import java.nio.ByteBuffer;
import java.util.Map;

import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.serialization.Deserializer;
import org.jspecify.annotations.Nullable;

import org.springframework.retry.RecoveryCallback;
import org.springframework.retry.RetryOperations;
import org.springframework.util.Assert;

/**
 * A deserializer configured with a delegate and a {@link RetryOperations} to retry
 * deserialization in case of transient errors.
 *
 * @param <T> Type to be deserialized into.
 * @author Gary Russell
 * @author Wang Zhiyang
 * @author Soby Chacko
 * @since 2.3
 */
public class RetryingDeserializer<T> implements Deserializer<T> {

	private final Deserializer<T> delegate;

	private final RetryOperations retryOperations;

	@Nullable
	private RecoveryCallback<T> recoveryCallback;

	public RetryingDeserializer(Deserializer<T> delegate, RetryOperations retryOperations) {
		Assert.notNull(delegate, "the 'delegate' deserializer cannot be null");
		Assert.notNull(retryOperations, "the 'retryOperations' deserializer cannot be null");
		this.delegate = delegate;
		this.retryOperations = retryOperations;
	}

	/**
	 * Set a recovery callback to execute when the retries are exhausted.
	 * @param recoveryCallback {@link RecoveryCallback} to execute
	 * @since 3.1.2
	 */
	public void setRecoveryCallback(@Nullable RecoveryCallback<T> recoveryCallback) {
		this.recoveryCallback = recoveryCallback;
	}

	@Override
	public void configure(Map<String, ?> configs, boolean isKey) {
		this.delegate.configure(configs, isKey);
	}

	@Override
	public T deserialize(String topic, byte[] data) {
		return this.retryOperations.execute(context -> this.delegate.deserialize(topic, data), this.recoveryCallback);
	}

	@Override
	public T deserialize(String topic, Headers headers, byte[] data) {
		return this.retryOperations.execute(context -> this.delegate.deserialize(topic, headers, data), this.recoveryCallback);
	}

	@Override
	public T deserialize(String topic, Headers headers, ByteBuffer data) {
		return this.retryOperations.execute(context -> this.delegate.deserialize(topic, headers, data), this.recoveryCallback);
	}

	@Override
	public void close() {
		this.delegate.close();
	}

}
