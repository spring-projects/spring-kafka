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

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.common.serialization.Deserializer;

import org.springframework.lang.Nullable;

/**
 * The strategy to produce a {@link Consumer} instance(s).
 *
 * @param <K> the key type.
 * @param <V> the value type.
 *
 * @author Gary Russell
 * @author Artem Bilan
 */
public interface ConsumerFactory<K, V> {

	/**
	 * Create a consumer with the group id and client id as configured in the properties.
	 * @return the consumer.
	 */
	default Consumer<K, V> createConsumer() {
		return createConsumer(null);
	}

	/**
	 * Create a consumer, appending the suffix to the {@code client.id} property,
	 * if present.
	 * @param clientIdSuffix the suffix.
	 * @return the consumer.
	 * @since 1.3
	 */
	default Consumer<K, V> createConsumer(@Nullable String clientIdSuffix) {
		return createConsumer(null, clientIdSuffix);
	}

	/**
	 * Create a consumer with an explicit group id; in addition, the
	 * client id suffix is appended to the {@code client.id} property, if both
	 * are present.
	 * @param groupId the group id.
	 * @param clientIdSuffix the suffix.
	 * @return the consumer.
	 * @since 1.3
	 */
	default Consumer<K, V> createConsumer(@Nullable String groupId, @Nullable String clientIdSuffix) {
		return createConsumer(groupId, null, clientIdSuffix);
	}

	/**
	 * Create a consumer with an explicit group id; in addition, the
	 * client id suffix is appended to the clientIdPrefix which overrides the
	 * {@code client.id} property, if present.
	 * @param groupId the group id.
	 * @param clientIdPrefix the prefix.
	 * @param clientIdSuffix the suffix.
	 * @return the consumer.
	 * @since 2.1.1
	 */
	Consumer<K, V> createConsumer(@Nullable String groupId, @Nullable String clientIdPrefix,
			@Nullable String clientIdSuffix);

	/**
	 * Create a consumer with an explicit group id; in addition, the
	 * client id suffix is appended to the clientIdPrefix which overrides the
	 * {@code client.id} property, if present. In addition, consumer properties can
	 * be overridden if the factory implementation supports it.
	 * @param groupId the group id.
	 * @param clientIdPrefix the prefix.
	 * @param clientIdSuffix the suffix.
	 * @param properties the properties to override.
	 * @return the consumer.
	 * @since 2.2.4
	 */
	default Consumer<K, V> createConsumer(@Nullable String groupId, @Nullable String clientIdPrefix,
			@Nullable String clientIdSuffix, @Nullable Properties properties) {

		return createConsumer(groupId, clientIdPrefix, clientIdSuffix);
	}

	/**
	 * Return true if consumers created by this factory use auto commit.
	 * @return true if auto commit.
	 */
	boolean isAutoCommit();

	/**
	 * Return an unmodifiable reference to the configuration map for this factory.
	 * Useful for cloning to make a similar factory.
	 * @return the configs.
	 * @since 2.0
	 */
	default Map<String, Object> getConfigurationProperties() {
		throw new UnsupportedOperationException("'getConfigurationProperties()' is not supported");
	}

	/**
	 * Return the configured key deserializer (if provided as an object instead
	 * of a class name in the properties).
	 * @return the deserializer.
	 * @since 2.0
	 */
	@Nullable
	default Deserializer<K> getKeyDeserializer() {
		return null;
	}

	/**
	 * Return the configured value deserializer (if provided as an object instead
	 * of a class name in the properties).
	 * @return the deserializer.
	 * @since 2.0
	 */
	@Nullable
	default Deserializer<V> getValueDeserializer() {
		return null;
	}

	/**
	 * Remove a listener.
	 * @param listener the listener.
	 * @return true if removed.
	 * @since 2.5.3
	 */
	default boolean removeListener(Listener<K, V> listener) {
		return false;
	}

	/**
	 * Add a listener at a specific index.
	 * @param index the index (list position).
	 * @param listener the listener.
	 * @since 2.5.3
	 */
	default void addListener(int index, Listener<K, V> listener) {

	}

	/**
	 * Add a listener.
	 * @param listener the listener.
	 * @since 2.5.3
	 */
	default void addListener(Listener<K, V> listener) {

	}

	/**
	 * Get the current list of listeners.
	 * @return the listeners.
	 * @since 2.5.3
	 */
	default List<Listener<K, V>> getListeners() {
		return Collections.emptyList();
	}

	/**
	 * Add a post processor.
	 * @param postProcessor the post processor.
	 * @since 2.5.3
	 */
	default void addPostProcessor(ConsumerPostProcessor<K, V> postProcessor) {

	}

	/**
	 * Remove a post processor.
	 * @param postProcessor the post processor.
	 * @return true if removed.
	 * @since 2.5.3
	 */
	default boolean removePostProcessor(ConsumerPostProcessor<K, V> postProcessor) {
		return false;
	}

	/**
	 * Get the current list of post processors.
	 * @return the post processor.
	 * @since 2.5.3
	 */
	default List<ConsumerPostProcessor<K, V>> getPostProcessors() {
		return Collections.emptyList();
	}

	/**
	 * Called whenever a consumer is added or removed.
	 *
	 * @param <K> the key type.
	 * @param <V> the value type.
	 *
	 * @since 2.5
	 *
	 */
	interface Listener<K, V> {

		/**
		 * A new consumer was created.
		 * @param id the consumer id (factory bean name and client.id separated by a
		 * period).
		 * @param consumer the consumer.
		 */
		default void consumerAdded(String id, Consumer<K, V> consumer) {
		}

		/**
		 * An existing consumer was removed.
		 * @param id the consumer id (factory bean name and client.id separated by a
		 * period).
		 * @param consumer the consumer.
		 */
		default void consumerRemoved(String id, Consumer<K, V> consumer) {
		}

	}

}
