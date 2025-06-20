/*
 * Copyright 2023-present the original author or authors.
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

package org.springframework.kafka.mock;

import java.util.function.BiFunction;
import java.util.function.Supplier;

import org.apache.kafka.clients.producer.MockProducer;
import org.apache.kafka.clients.producer.Producer;
import org.jspecify.annotations.Nullable;

import org.springframework.kafka.core.ProducerFactory;

/**
 * Support the use of {@link MockProducer} in tests.
 *
 * @param <K> the key type.
 * @param <V> the value type.
 *
 * @author Gary Russell
 * @since 3.0.7
 *
 */
public class MockProducerFactory<K, V> implements ProducerFactory<K, V> {

	private final BiFunction<Boolean, @Nullable String, MockProducer<K, V>> producerProvider;

	@Nullable
	private final String defaultTxId;

	private final boolean transactional;

	/**
	 * Create an instance that does not support transactional producers.
	 * @param producerProvider a {@link Supplier} for a {@link MockProducer}.
	 */
	public MockProducerFactory(Supplier<MockProducer<K, V>> producerProvider) {
		this.producerProvider = (tx, id) -> producerProvider.get();
		this.defaultTxId = null;
		this.transactional = false;
	}

	/**
	 * Create an instance that supports transactions, with the supplied producer provider {@link BiFunction}. The
	 * function has two parameters, a boolean indicating whether a transactional producer
	 * is being requested and, if true, the transaction id prefix for that producer.
	 * @param producerProvider the provider function.
	 * @param defaultTxId the default transactional id.
	 */
	public MockProducerFactory(BiFunction<Boolean, @Nullable String, MockProducer<K, V>> producerProvider,
			@Nullable String defaultTxId) {

		this.producerProvider = producerProvider;
		this.defaultTxId = defaultTxId;
		this.transactional = true;
	}

	@Override
	public boolean transactionCapable() {
		return this.transactional;
	}

	@Override
	public Producer<K, V> createProducer() {
		return createProducer(this.defaultTxId);
	}

	@Override
	public Producer<K, V> createProducer(@Nullable String txIdPrefix) {
		return txIdPrefix == null && this.defaultTxId == null
				? this.producerProvider.apply(false, null)
				: this.producerProvider.apply(true, txIdPrefix == null ? this.defaultTxId : txIdPrefix);
	}

	@Override
	public Producer<K, V> createNonTransactionalProducer() {
		return this.producerProvider.apply(false, null);
	}

}
