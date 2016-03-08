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

import org.springframework.kafka.test.rule.KafkaEmbedded;

/**
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
	public static Map<String, Object> senderProps(KafkaEmbedded embeddedKafka) {
		return senderProps(embeddedKafka.getBrokersAsString());
	}

	public static Map<String, Object> consumerProps(String brokers, String group, String autoCommit) {
		Map<String, Object> props = new HashMap<>();
		props.put("bootstrap.servers", brokers);
		props.put("group.id", group);
		props.put("enable.auto.commit", autoCommit);
		props.put("auto.commit.interval.ms", "100");
		props.put("session.timeout.ms", "15000");
		props.put("key.deserializer", "org.apache.kafka.common.serialization.IntegerDeserializer");
		props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
		return props;
	}

	public static Map<String, Object> senderProps(String brokers) {
		Map<String, Object> props = new HashMap<>();
		props.put("bootstrap.servers", brokers);
		props.put("retries", 0);
		props.put("batch.size", 16384);
		props.put("linger.ms", 1);
		props.put("buffer.memory", 33554432);
		props.put("key.serializer", "org.apache.kafka.common.serialization.IntegerSerializer");
		props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		return props;
	}

}
