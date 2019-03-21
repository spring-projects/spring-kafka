/*
 * Copyright 2016 the original author or authors.
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

import org.apache.kafka.clients.consumer.ConsumerRecord;

import org.springframework.kafka.listener.MessageListener;

/**
 * A {@link MessageListener} adapter that implements filter logic
 * via a {@link RecordFilterStrategy}.
 *
 * @param <K> the key type.
 * @param <V> the value type.
 *
 * @author Gary Russell
 *
 */
public class FilteringMessageListenerAdapter<K, V> extends AbstractFilteringMessageListener<K, V>
		implements MessageListener<K, V> {

	private final MessageListener<K, V> delegate;

	/**
	 * Create an instance with the supplied strategy and delegate listener.
	 * @param delegate the delegate.
	 * @param recordFilterStrategy the filter.
	 */
	public FilteringMessageListenerAdapter(MessageListener<K, V> delegate,
			RecordFilterStrategy<K, V> recordFilterStrategy) {
		super(recordFilterStrategy);
		this.delegate = delegate;
	}

	@Override
	public void onMessage(ConsumerRecord<K, V> consumerRecord) {
		if (!filter(consumerRecord)) {
			this.delegate.onMessage(consumerRecord);
		}
	}

}
