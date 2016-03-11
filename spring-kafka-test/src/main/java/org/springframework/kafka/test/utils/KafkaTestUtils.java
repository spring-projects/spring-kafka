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

package org.springframework.kafka.test.utils;

import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;

import org.springframework.kafka.test.rule.KafkaEmbedded;

/**
 * The Kafka specific testing utilities.
 *
 * @author Gary Russell
 *
 */
public final class KafkaTestUtils {

	private KafkaTestUtils() {
		// private ctor
	}

	/**
	 * Set up test properties for an {@code <Integer, String>} consumer.
	 * @param group the group id.
	 * @param autoCommit the auto commit.
	 * @param embeddedKafka a {@link KafkaEmbedded} instance.
	 * @return the properties.
	 */
	public static Map<String, Object> consumerProps(String group, String autoCommit, KafkaEmbedded embeddedKafka) {
		return consumerProps(embeddedKafka.getBrokersAsString(), group, autoCommit);
	}

	/**
	 * Set up test properties for an {@code <Integer, String>} producer.
	 * @param embeddedKafka a {@link KafkaEmbedded} instance.
	 * @return the properties.
	 */
	public static Map<String, Object> producerProps(KafkaEmbedded embeddedKafka) {
		return senderProps(embeddedKafka.getBrokersAsString());
	}

	public static Map<String, Object> consumerProps(String brokers, String group, String autoCommit) {
		Map<String, Object> props = new HashMap<>();
		props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, brokers);
		props.put(ConsumerConfig.GROUP_ID_CONFIG, group);
		props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, autoCommit);
		props.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "100");
		props.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, "15000");
		props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
				"org.apache.kafka.common.serialization.IntegerDeserializer");
		props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
				"org.apache.kafka.common.serialization.StringDeserializer");
		return props;
	}

	public static Map<String, Object> senderProps(String brokers) {
		Map<String, Object> props = new HashMap<>();
		props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokers);
		props.put(ProducerConfig.RETRIES_CONFIG, 0);
		props.put(ProducerConfig.BATCH_SIZE_CONFIG, 16384);
		props.put(ProducerConfig.LINGER_MS_CONFIG, 1);
		props.put(ProducerConfig.BUFFER_MEMORY_CONFIG, 33554432);
		props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
				"org.apache.kafka.common.serialization.IntegerSerializer");
		props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
				"org.apache.kafka.common.serialization.StringSerializer");
		return props;
	}

}
