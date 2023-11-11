/*
 * Copyright 2023 the original author or authors.
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

package org.springframework.kafka.test;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import org.springframework.util.StringUtils;

/**
 * @author Gary Russell
 * @author Nathan Xu
 * @since 3.1
 *
 */
public class EmbeddedKafkaKraftBrokerTests {

	@BeforeEach
	void setUp() {
		System.clearProperty(EmbeddedKafkaKraftBroker.BROKER_LIST_PROPERTY);
		System.clearProperty(EmbeddedKafkaBroker.SPRING_EMBEDDED_KAFKA_BROKERS);
	}

	@Test
	void testUpDown() {
		EmbeddedKafkaKraftBroker kafka = new EmbeddedKafkaKraftBroker(1, 1, "topic1");
		kafka.afterPropertiesSet();
		Assertions.assertTrue(StringUtils.hasText(kafka.getBrokersAsString()));
		Assertions.assertEquals(kafka.getBrokersAsString(), System.getProperty(EmbeddedKafkaBroker.SPRING_EMBEDDED_KAFKA_BROKERS));
		kafka.destroy();
	}

	@Test
	void testBrokerListSystemPropertyTakePrecedence() {
		String customBrokerListPropertyName = "my.brokerList.property";
		System.setProperty(EmbeddedKafkaKraftBroker.BROKER_LIST_PROPERTY, customBrokerListPropertyName);
		try {
			EmbeddedKafkaKraftBroker kafka = new EmbeddedKafkaKraftBroker(1, 1, "topic1");
			kafka.afterPropertiesSet();
			String brokersString = kafka.getBrokersAsString();
			Assertions.assertEquals(brokersString, System.getProperty(customBrokerListPropertyName));
			Assertions.assertFalse(System.getenv().containsKey(EmbeddedKafkaBroker.SPRING_EMBEDDED_KAFKA_BROKERS));
			kafka.destroy();
		} finally {
			System.clearProperty(customBrokerListPropertyName);
		}
	}

}
