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

import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.clients.producer.KafkaProducer;


/**
 * Default implementation of Kafka Producer strategy.
 * <p>
 * This {@link KafkaProducerStrategy} implementation will produce a new {@link KafkaProducer} instance(s)
 * for the provided {@link Map} {@code configs} on each {@link #createKafkaProducer()} invocations.
 *
 * @param <K> the key type.
 * @param <V> the value type.
 *
 * @author Murali Reddy
 */

public class DefaultKafkaProducerStrategy<K, V> implements KafkaProducerStrategy<K, V> {

	private final Map<String, Object> configs;

	public DefaultKafkaProducerStrategy(Map<String, Object> configs) {
		this.configs = new HashMap<>(configs);
	}

	@Override
	public KafkaProducer<K, V> createKafkaProducer() {
		return new KafkaProducer<K, V>(this.configs);
	}

}
