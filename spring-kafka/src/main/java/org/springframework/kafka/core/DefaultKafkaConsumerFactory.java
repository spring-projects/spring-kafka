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

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.KafkaConsumer;

/**
 * @author Gary Russell
 *
 */
public class DefaultKafkaConsumerFactory<K, V> implements ConsumerFactory<K, V> {

	private final Map<String, Object> configs;

	public DefaultKafkaConsumerFactory(Map<String, Object> configs) {
		this.configs = new HashMap<>(configs);
	}

	@Override
	public Consumer<K, V> createConsumer() {
		return new KafkaConsumer<>(this.configs);
	}

	@Override
	public boolean isAutoCommit() {
		Object auto = this.configs.get("enable.auto.commit");
		return auto instanceof Boolean ? (Boolean) auto
				: auto instanceof String ? Boolean.valueOf((String) auto) : false;
	}

}
