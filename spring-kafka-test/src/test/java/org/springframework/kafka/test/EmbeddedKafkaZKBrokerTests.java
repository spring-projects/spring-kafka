/*
 * Copyright 2019-present the original author or authors.
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

import java.util.Map;

import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.jupiter.api.Test;

import org.springframework.kafka.test.utils.KafkaTestUtils;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * @author Gary Russell
 * @author Wouter Coekaerts
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

	@Test
	void testConsumeFromEmbeddedWithSeekToEnd() {
		EmbeddedKafkaZKBroker kafka = new EmbeddedKafkaZKBroker(1);
		kafka.afterPropertiesSet();
		kafka.addTopics("seekTestTopic");
		Map<String, Object> producerProps = KafkaTestUtils.producerProps(kafka);
		KafkaProducer<Integer, String> producer = new KafkaProducer<>(producerProps);
		producer.send(new ProducerRecord<>("seekTestTopic", 0, 1, "beforeSeekToEnd"));
		Map<String, Object> consumerProps = KafkaTestUtils.consumerProps("seekTest", "false", kafka);
		KafkaConsumer<Integer, String> consumer = new KafkaConsumer<>(consumerProps);
		kafka.consumeFromAnEmbeddedTopic(consumer, true /* seekToEnd */, "seekTestTopic");
		producer.send(new ProducerRecord<>("seekTestTopic", 0, 1, "afterSeekToEnd"));
		producer.close();
		assertThat(KafkaTestUtils.getSingleRecord(consumer, "seekTestTopic").value())
				.isEqualTo("afterSeekToEnd");
		consumer.close();
	}

}
