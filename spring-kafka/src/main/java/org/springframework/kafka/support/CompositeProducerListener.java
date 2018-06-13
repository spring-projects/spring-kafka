/*
 * Copyright 2018 the original author or authors.
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

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import org.springframework.util.Assert;

/**
 * A {@link ProducerListener} that delegates to a collection of listeners.
 *
 * @param <K> the key type.
 * @param <V> the value type.
 *
 * @author Gary Russell
 * @author Artem Bilan
 *
 * @since 2.1.6
 *
 */
public class CompositeProducerListener<K, V> implements ProducerListener<K, V> {

	private final List<ProducerListener<K, V>> delegates = new CopyOnWriteArrayList<>();

	@SafeVarargs
	@SuppressWarnings("varargs")
	public CompositeProducerListener(ProducerListener<K, V>... delegates) {
		setDelegates(delegates);
	}

	@SafeVarargs
	@SuppressWarnings("varargs")
	public final void setDelegates(ProducerListener<K, V>... delegates) {
		Assert.notNull(delegates, "'delegates' cannot be null");
		Assert.noNullElements(delegates, "'delegates' cannot contain null elements");
		this.delegates.clear();
		this.delegates.addAll(Arrays.asList(delegates));
	}

	protected List<ProducerListener<K, V>> getDelegates() {
		return this.delegates;
	}

	public void addDelegate(ProducerListener<K, V> delegate) {
		this.delegates.add(delegate);
	}

	public boolean removeDelegate(ProducerListener<K, V> delegate) {
		return this.delegates.remove(delegate);
	}

	@Override
	public void onSuccess(ProducerRecord<K, V> producerRecord, RecordMetadata recordMetadata) {
		this.delegates.forEach(d -> d.onSuccess(producerRecord, recordMetadata));
	}

	@Override
	public void onError(ProducerRecord<K, V> producerRecord, Exception exception) {
		this.delegates.forEach(d -> d.onError(producerRecord, exception));
	}

}
