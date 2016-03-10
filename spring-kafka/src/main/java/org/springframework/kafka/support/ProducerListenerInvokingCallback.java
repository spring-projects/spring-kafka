/*
 * Copyright 2015-2016 the original author or authors.
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

package org.springframework.kafka.support;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.RecordMetadata;

import org.springframework.util.Assert;

/**
 * Adapts the {@link org.apache.kafka.clients.producer.Callback} interface of the
 * {@link org.apache.kafka.clients.producer.Producer} to a {@link ProducerListener}.
 *
 * @author Marius Bogoevici
 * @author Gary Russell
 */
public class ProducerListenerInvokingCallback<K, V> implements Callback {

	private final String topic;

	private final Integer partition;

	private final K key;

	private final V value;

	private final ProducerListener<K, V> producerListener;

	public ProducerListenerInvokingCallback(String topic, Integer partition, K key, V value,
											ProducerListener<K, V> producerListener) {
		Assert.notNull(producerListener, "must not be null");
		this.topic = topic;
		this.partition = partition;
		this.key = key;
		this.value = value;
		this.producerListener = producerListener;
	}

	@Override
	public void onCompletion(RecordMetadata metadata, Exception exception) {
		if (exception != null) {
			this.producerListener.onError(this.topic, this.partition, this.key, this.value, exception);
		}
		else {
			this.producerListener.onSuccess(this.topic, this.partition, this.key, this.value, metadata);
		}
	}

}
