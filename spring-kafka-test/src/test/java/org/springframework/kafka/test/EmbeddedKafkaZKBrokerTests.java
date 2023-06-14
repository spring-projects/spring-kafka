/*
 * Copyright 2019-2023 the original author or authors.
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

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

/**
 * @author Gary Russell
 * @since 2.3
 *
 */
public class EmbeddedKafkaZKBrokerTests {

	@Test
	void testUpDown() {
		EmbeddedKafkaZKBroker kafka = new EmbeddedKafkaZKBroker(1);
		kafka.brokerListProperty("foo.bar");
		kafka.afterPropertiesSet();
		assertThat(kafka.getZookeeperConnectionString()).startsWith("127");
		assertThat(System.getProperty("foo.bar")).isNotNull();
		assertThat(System.getProperty(EmbeddedKafkaBroker.SPRING_EMBEDDED_KAFKA_BROKERS))
				.isEqualTo(System.getProperty("foo.bar"));
		kafka.destroy();
		assertThat(kafka.getZookeeperConnectionString()).isNull();
		assertThat(System.getProperty("foo.bar")).isNull();
		assertThat(System.getProperty(EmbeddedKafkaBroker.SPRING_EMBEDDED_KAFKA_BROKERS)).isNull();
	}

}
