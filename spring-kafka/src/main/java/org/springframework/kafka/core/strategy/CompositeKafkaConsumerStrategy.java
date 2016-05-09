/*
 * Copyright 2016 the original author or authors.
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

package org.springframework.kafka.core.strategy;

import java.util.Map;

import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.Deserializer;

/**
 * Composite implementation of Kafka Consumer strategy contract.
 * <p>
 * This {@link KafkaConsumerStrategy} implementation will produce
 * a new {@link KafkaConsumer} for provided {@link Map} {@code configs}, {@link Deserializer} {@code keyDeserializer}
 * and {@link Deserializer} {@code valueDeserializer} on {@link #createKafkaConsumer()} invocation.
 *
 * @param <K> the key type.
 * @param <V> the value type.
 *
 * @author Murali Reddy
 */
public class CompositeKafkaConsumerStrategy<K, V> implements KafkaConsumerStrategy<K, V> {

	private final Map<String, Object> configs;

	private Deserializer<K> keyDeserializer;

	private Deserializer<V> valueDeserializer;

	public CompositeKafkaConsumerStrategy(Map<String, Object> configs, Deserializer<K> keyDeserializer,
			Deserializer<V> valueDeserializer) {
		super();
		this.configs = configs;
		this.keyDeserializer = keyDeserializer;
		this.valueDeserializer = valueDeserializer;
	}

	@Override
	public KafkaConsumer<K, V> createKafkaConsumer() {
		return new KafkaConsumer<K, V>(this.configs, this.keyDeserializer, this.valueDeserializer);
	}

}
