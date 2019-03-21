/*
 * Copyright 2015-2016 the original author or authors.
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

package org.springframework.kafka.support;

import org.apache.kafka.clients.producer.RecordMetadata;

/**
 * No-op implementation of {@link ProducerListener}, to be used as base class for other implementations.
 *
 * @param <K> the key type.
 * @param <V> the value type.
 *
 * @author Marius Bogoevici
 * @author Gary Russell
 * @author Artem Bilan
 */
public abstract class ProducerListenerAdapter<K, V> implements ProducerListener<K, V> {

	@Override
	public void onSuccess(String topic, Integer partition, K key, V value, RecordMetadata recordMetadata) {
	}

	@Override
	public void onError(String topic, Integer partition, K key, V value, Exception exception) {
	}

	@Override
	public boolean isInterestedInSuccess() {
		return false;
	}

}
