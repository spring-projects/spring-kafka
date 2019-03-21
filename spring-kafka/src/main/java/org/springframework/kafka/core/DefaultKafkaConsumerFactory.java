/*
 * Copyright 2016-2017 the original author or authors.
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
import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.Deserializer;

/**
 * The {@link ConsumerFactory} implementation to produce a new {@link Consumer} instance
 * for provided {@link Map} {@code configs} and optional {@link Deserializer} {@code keyDeserializer},
 * {@code valueDeserializer} implementations on each {@link #createConsumer()}
 * invocation.
 *
 * @param <K> the key type.
 * @param <V> the value type.
 *
 * @author Gary Russell
 * @author Murali Reddy
 */
public class DefaultKafkaConsumerFactory<K, V> implements ConsumerFactory<K, V> {

	private final Map<String, Object> configs;

	private Deserializer<K> keyDeserializer;

	private Deserializer<V> valueDeserializer;

	public DefaultKafkaConsumerFactory(Map<String, Object> configs) {
		this(configs, null, null);
	}

	public DefaultKafkaConsumerFactory(Map<String, Object> configs,
			Deserializer<K> keyDeserializer,
			Deserializer<V> valueDeserializer) {
		this.configs = new HashMap<>(configs);
		this.keyDeserializer = keyDeserializer;
		this.valueDeserializer = valueDeserializer;
	}

	public void setKeyDeserializer(Deserializer<K> keyDeserializer) {
		this.keyDeserializer = keyDeserializer;
	}

	public void setValueDeserializer(Deserializer<V> valueDeserializer) {
		this.valueDeserializer = valueDeserializer;
	}

	@Override
	public Map<String, Object> getConfigurationProperties() {
		return Collections.unmodifiableMap(this.configs);
	}

	@Override
	public Deserializer<K> getKeyDeserializer() {
		return this.keyDeserializer;
	}

	@Override
	public Deserializer<V> getValueDeserializer() {
		return this.valueDeserializer;
	}

	@Override
	public Consumer<K, V> createConsumer() {
		return createKafkaConsumer();
	}

	@Override
	public Consumer<K, V> createConsumer(String clientIdSuffix) {
		return createKafkaConsumer(null, clientIdSuffix);
	}

	@Override
	public Consumer<K, V> createConsumer(String groupId, String clientIdSuffix) {
		return createKafkaConsumer(groupId, clientIdSuffix);
	}

	protected KafkaConsumer<K, V> createKafkaConsumer() {
		return createKafkaConsumer(this.configs);
	}

	protected KafkaConsumer<K, V> createKafkaConsumer(String groupId, String clientIdSuffix) {
		boolean shouldModifyClientId = this.configs.containsKey(ConsumerConfig.CLIENT_ID_CONFIG)
				&& clientIdSuffix != null;
		if (groupId == null && !shouldModifyClientId) {
			return createKafkaConsumer();
		}
		else {
			Map<String, Object> modifiedConfigs = new HashMap<>(this.configs);
			if (groupId != null) {
				modifiedConfigs.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
			}
			if (shouldModifyClientId) {
				modifiedConfigs.put(ConsumerConfig.CLIENT_ID_CONFIG,
					modifiedConfigs.get(ConsumerConfig.CLIENT_ID_CONFIG) + clientIdSuffix);
			}
			return createKafkaConsumer(modifiedConfigs);
		}
	}

	protected KafkaConsumer<K, V> createKafkaConsumer(Map<String, Object> configs) {
		return new KafkaConsumer<K, V>(configs, this.keyDeserializer, this.valueDeserializer);
	}

	@Override
	public boolean isAutoCommit() {
		Object auto = this.configs.get(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG);
		return auto instanceof Boolean ? (Boolean) auto
				: auto instanceof String ? Boolean.valueOf((String) auto) : true;
	}

}
