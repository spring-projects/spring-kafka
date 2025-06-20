/*
 * Copyright 2025-present the original author or authors.
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

import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.apache.kafka.clients.consumer.ShareConsumer;
import org.apache.kafka.common.serialization.Deserializer;
import org.jspecify.annotations.Nullable;

/**
 * The strategy to produce a {@link ShareConsumer} instance for Kafka queue support.
 *
 * @param <K> the key type.
 * @param <V> the value type.
 *
 * @author Soby Chacko
 * @since 4.0
 */
public interface ShareConsumerFactory<K, V> {

	/**
	 * Create a share consumer with the provided group id and client id.
	 * @param groupId the group id (maybe null).
	 * @param clientId the client id.
	 * @return the share consumer.
	 */
	ShareConsumer<K, V> createShareConsumer(@Nullable String groupId, @Nullable String clientId);

	/**
	 * Return an unmodifiable reference to the configuration map for this factory.
	 * Useful for cloning to make a similar factory.
	 * @return the configs.
	 */
	default Map<String, Object> getConfigurationProperties() {
		throw new UnsupportedOperationException("'getConfigurationProperties()' is not supported");
	}

	/**
	 * Return the configured key deserializer (if provided as an object instead
	 * of a class name in the properties).
	 * @return the deserializer.
	 */
	@Nullable
	default Deserializer<K> getKeyDeserializer() {
		return null;
	}

	/**
	 * Return the configured value deserializer (if provided as an object instead
	 * of a class name in the properties).
	 * @return the deserializer.
	 */
	@Nullable
	default Deserializer<V> getValueDeserializer() {
		return null;
	}

	/**
	 * Remove a listener.
	 * @param listener the listener.
	 * @return true if removed.
	 */
	default boolean removeListener(Listener<K, V> listener) {
		return false;
	}

	/**
	 * Add a listener at a specific index.
	 * @param index the index (list position).
	 * @param listener the listener.
	 */
	default void addListener(int index, Listener<K, V> listener) {
	}

	/**
	 * Add a listener.
	 * @param listener the listener.
	 */
	default void addListener(Listener<K, V> listener) {
	}

	/**
	 * Get the current list of listeners.
	 * @return the listeners.
	 */
	default List<Listener<K, V>> getListeners() {
		return Collections.emptyList();
	}

	/**
	 * Listener for share consumer lifecycle events.
	 *
	 * @param <K> the key type.
	 * @param <V> the value type.
	 */
	interface Listener<K, V> {

		/**
		 * A new consumer was created.
		 * @param id the consumer id (factory bean name and client.id separated by a period).
		 * @param consumer the consumer.
		 */
		default void consumerAdded(String id, ShareConsumer<K, V> consumer) {
		}

		/**
		 * An existing consumer was removed.
		 * @param id the consumer id (factory bean name and client.id separated by a period).
		 * @param consumer the consumer.
		 */
		default void consumerRemoved(@Nullable String id, ShareConsumer<K, V> consumer) {
		}

	}
}
