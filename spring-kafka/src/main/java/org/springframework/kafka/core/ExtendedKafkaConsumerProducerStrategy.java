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

package org.springframework.kafka.core;

import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serializer;

/**
 * Extended implementation of Kafka Consumer and Producer strategy contract.
 * 
 *
 * @param <K> the key type.
 * @param <V> the value type.
 * 
 * @author Murali Reddy
 */
public class ExtendedKafkaConsumerProducerStrategy<K,V> implements KafkaConsumerProducerStrategy<K,V> {

	private final Map<String, Object> configs;
	private Serializer<K> keySerializer;
	private Serializer<V> valueSerializer;
	private Deserializer<K> keyDeserializer;
	private Deserializer<V> valueDeserializer;
	
	public ExtendedKafkaConsumerProducerStrategy(Map<String, Object> configs, Serializer<K> keySerializer,
			Serializer<V> valueSerializer, Deserializer<K> keyDeserializer, Deserializer<V> valueDeserializer) {
		this.configs = new HashMap<>(configs);
		this.keySerializer = keySerializer;
		this.valueSerializer = valueSerializer;
		this.keyDeserializer = keyDeserializer;
		this.valueDeserializer = valueDeserializer;
	}

	public ExtendedKafkaConsumerProducerStrategy(Map<String, Object> configs, Deserializer<K> keyDeserializer,
			Deserializer<V> valueDeserializer) {
		super();
		this.configs = configs;
		this.keyDeserializer = keyDeserializer;
		this.valueDeserializer = valueDeserializer;
	}

	public ExtendedKafkaConsumerProducerStrategy(Map<String, Object> configs, Serializer<K> keySerializer,
			Serializer<V> valueSerializer) {
		super();
		this.configs = configs;
		this.keySerializer = keySerializer;
		this.valueSerializer = valueSerializer;
	}

	@Override
	public KafkaConsumer<K, V> createKafkaConsumer() {
		return new KafkaConsumer<K,V>(this.configs,this.keyDeserializer,this.valueDeserializer);
	}

	@Override
	public KafkaProducer<K, V> createKafkaProducer() {
		return new KafkaProducer<K,V>(this.configs,this.keySerializer,this.valueSerializer);
	}

}
