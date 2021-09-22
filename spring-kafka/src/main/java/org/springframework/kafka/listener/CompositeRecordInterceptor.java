/*
 * Copyright 2019-2021 the original author or authors.
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

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import org.springframework.lang.Nullable;
import org.springframework.util.Assert;

/**
 * A {@link RecordInterceptor} that delegates to one or more {@link RecordInterceptor}s in
 * order.
 *
 * @param <K> the key type.
 * @param <V> the value type.
 *
 * @author Artem Bilan
 * @author Gary Russell
 * @since 2.3
 *
 */
public class CompositeRecordInterceptor<K, V> implements ConsumerAwareRecordInterceptor<K, V> {

	private final Collection<RecordInterceptor<K, V>> delegates = new ArrayList<>();

	/**
	 * Construct an instance with the provided delegates.
	 * @param delegates the delegates.
	 */
	@SafeVarargs
	@SuppressWarnings("varargs")
	public CompositeRecordInterceptor(RecordInterceptor<K, V>... delegates) {
		Assert.notNull(delegates, "'delegates' cannot be null");
		Assert.noNullElements(delegates, "'delegates' cannot have null entries");
		this.delegates.addAll(Arrays.asList(delegates));
	}

	@Override
	@Nullable
	public ConsumerRecord<K, V> intercept(ConsumerRecord<K, V> record, Consumer<K, V> consumer) {
		ConsumerRecord<K, V> recordToIntercept = record;
		for (RecordInterceptor<K, V> delegate : this.delegates) {
			recordToIntercept = delegate.intercept(recordToIntercept, consumer);
			if (recordToIntercept == null) {
				break;
			}
		}
		return recordToIntercept;
	}

	@Override
	public void success(ConsumerRecord<K, V> record, Consumer<K, V> consumer) {
		this.delegates.forEach(del -> del.success(record, consumer));
	}

	@Override
	public void failure(ConsumerRecord<K, V> record, Exception exception, Consumer<K, V> consumer) {
		this.delegates.forEach(del -> del.failure(record, exception, consumer));
	}

	@Override
	public void setupThreadState(Consumer<?, ?> consumer) {
		this.delegates.forEach(del -> del.setupThreadState(consumer));
	}

	@Override
	public void clearThreadState(Consumer<?, ?> consumer) {
		this.delegates.forEach(del -> del.clearThreadState(consumer));
	}

	@Override
	public void afterRecord(ConsumerRecord<K, V> record, Consumer<K, V> consumer) {
		this.delegates.forEach(del -> del.afterRecord(record, consumer));
	}

}
