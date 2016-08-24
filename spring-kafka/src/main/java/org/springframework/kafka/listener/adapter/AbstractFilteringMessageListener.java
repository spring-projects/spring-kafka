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

import org.apache.kafka.clients.consumer.ConsumerRecord;

import org.springframework.kafka.listener.ConsumerSeekAware;
import org.springframework.util.Assert;

/**
 * An abstract message listener adapter that implements record filter logic
 * via a {@link RecordFilterStrategy}.
 *
 * @param <K> the key type.
 * @param <V> the value type.
 *
 * @author Gary Russell
 *
 */
public abstract class AbstractFilteringMessageListener<K, V> implements ConsumerSeekAware {

	private final ConsumerSeekAware seekAware;

	private final RecordFilterStrategy<K, V> recordFilterStrategy;

	protected AbstractFilteringMessageListener(Object delegate, RecordFilterStrategy<K, V> recordFilterStrategy) {
		Assert.notNull(recordFilterStrategy, "'recordFilterStrategy' cannot be null");
		this.recordFilterStrategy = recordFilterStrategy;
		if (delegate instanceof ConsumerSeekAware) {
			seekAware = (ConsumerSeekAware) delegate;
		}
		else {
			seekAware = null;
		}
	}

	@Override
	public void registerSeekCallback(ConsumerSeekCallback callback) {
		if (this.seekAware != null) {
			this.seekAware.registerSeekCallback(callback);
		}
	}

	protected boolean filter(ConsumerRecord<K, V> consumerRecord) {
		return this.recordFilterStrategy.filter(consumerRecord);
	}

}
