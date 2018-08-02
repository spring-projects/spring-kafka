/*
 * Copyright 2016-2018 the original author or authors.
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

package org.springframework.kafka.core;

import java.util.Map;

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
	 * If a factory does not implement this method, {@link #createConsumer(String, String)}
	 * is invoked, ignoring the prefix.
	 * @param groupId the group id.
	 * @param clientIdPrefix the prefix.
	 * @param clientIdSuffix the suffix.
	 * @return the consumer.
	 * @since 2.1.1
	 */
	Consumer<K, V> createConsumer(@Nullable String groupId, @Nullable String clientIdPrefix,
			@Nullable String clientIdSuffix);

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
	default Deserializer<K> getKeyDeserializer() {
		throw new UnsupportedOperationException("'getKeyDeserializer()' is not supported");
	}

	/**
	 * Return the configured value deserializer (if provided as an object instead
	 * of a class name in the properties).
	 * @return the deserializer.
	 * @since 2.0
	 */
	default Deserializer<V> getValueDeserializer() {
		throw new UnsupportedOperationException("'getKeyDeserializer()' is not supported");
	}

}
