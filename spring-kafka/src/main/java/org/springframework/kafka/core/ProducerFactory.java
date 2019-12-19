/*
 * Copyright 2016-2019 the original author or authors.
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

package org.springframework.kafka.core;

import org.apache.kafka.clients.producer.Producer;

/**
 * The strategy to produce a {@link Producer} instance(s).
 *
 * @param <K> the key type.
 * @param <V> the value type.
 *
 * @author Gary Russell
 */
public interface ProducerFactory<K, V> {

	/**
	 * Create a producer.
	 * @return the producer.
	 */
	Producer<K, V> createProducer();

	/**
	 * Create a producer with an overridden transaction id prefix.
	 * @param txIdPrefix the transaction id prefix.
	 * @return the producer.
	 * @since 2.3
	 */
	default Producer<K, V> createProducer(@SuppressWarnings("unused") String txIdPrefix) {
		throw new UnsupportedOperationException("This factory does not support this method");
	}

	/**
	 * Return true if the factory supports transactions.
	 * @return true if transactional.
	 */
	default boolean transactionCapable() {
		return false;
	}

	/**
	 * Remove the specified producer from the cache and close it.
	 * @param transactionIdSuffix the producer's transaction id suffix.
	 * @since 1.3.8
	 */
	default void closeProducerFor(String transactionIdSuffix) {
		// NOSONAR
	}

	/**
	 * Return the producerPerConsumerPartition.
	 * @return the producerPerConsumerPartition.
	 * @since 1.3.8
	 */
	default boolean isProducerPerConsumerPartition() {
		return false;
	}

	/**
	 * If the factory implementation uses thread-bound producers, call this method to
	 * close and release this thread's producer.
	 * @since 2.3
	 */
	default void closeThreadBoundProducer() {
		// NOSONAR
	}

	/**
	 * Reset any state in the factory, if supported.
	 * @since 2.4
	 */
	default void reset() {
		// NOSONAR
	}

}
