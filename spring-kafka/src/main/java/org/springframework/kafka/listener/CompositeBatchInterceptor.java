/*
 * Copyright 2019 the original author or authors.
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

package org.springframework.kafka.listener;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;

import org.apache.kafka.clients.consumer.ConsumerRecords;

import org.springframework.util.Assert;

/**
 * A {@link BatchInterceptor} that delegates to one or more {@link BatchInterceptor}s in
 * order.
 *
 * @param <K> the key type.
 * @param <V> the value type.
 *
 * @author Gary Russell
 * @since 2.7
 *
 */
public class CompositeBatchInterceptor<K, V> implements BatchInterceptor<K, V> {

	private final Collection<BatchInterceptor<K, V>> delegates = new ArrayList<>();

	/**
	 * Construct an instance with the provided delegates.
	 * @param delegates the delegates.
	 */
	@SafeVarargs
	@SuppressWarnings("varargs")
	public CompositeBatchInterceptor(BatchInterceptor<K, V>... delegates) {
		Assert.notNull(delegates, "'delegates' cannot be null");
		Assert.noNullElements(delegates, "'delegates' cannot have null entries");
		this.delegates.addAll(Arrays.asList(delegates));
	}

	@Override
	public ConsumerRecords<K, V> intercept(ConsumerRecords<K, V> records) {
		ConsumerRecords<K, V> recordsToIntercept = records;
		for (BatchInterceptor<K, V> delegate : this.delegates) {
			recordsToIntercept = delegate.intercept(recordsToIntercept);
		}
		return recordsToIntercept;
	}

	@Override
	public void success(ConsumerRecords<K, V> records) {
		this.delegates.forEach(del -> del.success(records));
	}

	@Override
	public void failure(ConsumerRecords<K, V> records, Exception exception) {
		this.delegates.forEach(del -> del.failure(records, exception));
	}

}
