/*
 * Copyright 2016-2020 the original author or authors.
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

import java.time.Duration;
import java.util.Map;
import java.util.function.Supplier;

import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.common.serialization.Serializer;

import org.springframework.lang.Nullable;

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
	 * The default close timeout duration as 30 seconds.
	 */
	Duration DEFAULT_PHYSICAL_CLOSE_TIMEOUT = Duration.ofSeconds(30);

	/**
	 * Create a producer which will be transactional if the factory is so configured.
	 * @return the producer.
	 * @see #transactionCapable()
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
	 * Create a non-transactional producer.
	 * @return the producer.
	 * @since 2.4.3
	 * @see #transactionCapable()
	 */
	default Producer<K, V> createNonTransactionalProducer() {
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

	/**
	 * Return an unmodifiable reference to the configuration map for this factory.
	 * Useful for cloning to make a similar factory.
	 * @return the configs.
	 * @since 2.5
	 */
	default Map<String, Object> getConfigurationProperties() {
		throw new UnsupportedOperationException("This implementation doesn't support this method");
	}

	/**
	 * Return a supplier for a value serializer.
	 * Useful for cloning to make a similar factory.
	 * @return the supplier.
	 * @since 2.5
	 */
	default Supplier<Serializer<V>> getValueSerializerSupplier() {
		return () -> null;
	}

	/**
	 * Return a supplier for a key serializer.
	 * Useful for cloning to make a similar factory.
	 * @return the supplier.
	 * @since 2.5
	 */
	default Supplier<Serializer<K>> getKeySerializerSupplier() {
		return () -> null;
	}

	/**
	 * Return true when there is a producer per thread.
	 * @return the produver per thread.
	 * @since 2.5
	 */
	default boolean isProducerPerThread() {
		return false;
	}

	/**
	 * Return the transaction id prefix.
	 * @return the prefix or null if not configured.
	 * @since 2.5
	 */
	@Nullable
	default String getTransactionIdPrefix() {
		return null;
	}

	/**
	 * Get the physical close timeout.
	 * @return the timeout.
	 * @since 2.5
	 */
	default Duration getPhysicalCloseTimeout() {
		return DEFAULT_PHYSICAL_CLOSE_TIMEOUT;
	}

	/**
	 * Called whenever a producer is added or removed.
	 *
	 * @param <K> the key type.
	 * @param <V> the value type.
	 *
	 * @since 2.5
	 *
	 */
	interface Listener<K, V> {

		/**
		 * A new producer was created.
		 * @param id the producer id (factory bean name and client.id separated by a
		 * period).
		 * @param producer the producer.
		 */
		default void producerAdded(String id, Producer<K, V> producer) {
		}

		/**
		 * An exsting producer was removed.
		 * @param id the producer id (factory bean name and client.id separated by a
		 * period).
		 * @param producer the producer.
		 */
		default void producerRemoved(String id, Producer<K, V> producer) {
		}

	}

}
