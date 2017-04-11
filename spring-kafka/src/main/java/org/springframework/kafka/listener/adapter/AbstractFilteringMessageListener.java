/*
 * Copyright 2016-2017 the original author or authors.
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

import org.apache.kafka.clients.consumer.ConsumerRecord;

import org.springframework.util.Assert;

/**
 * An abstract message listener adapter that implements record filter logic
 * via a {@link RecordFilterStrategy}.
 *
 * @param <K> the key type.
 * @param <V> the value type.
 * @param <T> the delegate type.
 *
 * @author Gary Russell
 *
 */
public abstract class AbstractFilteringMessageListener<K, V, T>
		extends AbstractDelegatingMessageListenerAdapter<T> {

	private final RecordFilterStrategy<K, V> recordFilterStrategy;

	protected AbstractFilteringMessageListener(T delegate, RecordFilterStrategy<K, V> recordFilterStrategy) {
		super(delegate);
		Assert.notNull(recordFilterStrategy, "'recordFilterStrategy' cannot be null");
		this.recordFilterStrategy = recordFilterStrategy;
	}

	protected boolean filter(ConsumerRecord<K, V> consumerRecord) {
		return this.recordFilterStrategy.filter(consumerRecord);
	}

}
