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

package org.springframework.kafka.core;

import java.util.Map;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.common.serialization.Deserializer;

/**
 * The strategy to produce a {@link Consumer} instance(s).
 *
 * @param <K> the key type.
 * @param <V> the value type.
 *
 * @author Gary Russell
 */
public interface ConsumerFactory<K, V> {

	/**
	 * Create a consumer with the group id and client id as configured in the properties.
	 * @return the consumer.
	 */
	Consumer<K, V> createConsumer();

	/**
	 * Create a consumer, appending the suffix to the {@code client.id} property,
	 * if present.
	 * @param clientIdSuffix the suffix.
	 * @return the consumer.
	 * @since 1.3
	 */
	Consumer<K, V> createConsumer(String clientIdSuffix);

	/**
	 * Create a consumer with an explicit group id; in addition, the
	 * client id suffix is appended to the {@code client.id} property, if both
	 * are present.
	 * @param groupId the group id.
	 * @param clientIdSuffix the suffix.
	 * @return the consumer.
	 * @since 1.3
	 */
	Consumer<K, V> createConsumer(String groupId, String clientIdSuffix);

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
