/*
 * Copyright 2023-present the original author or authors.
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
import org.springframework.util.StringUtils;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * @author Gary Russell
 * @author Wouter Coekaerts
 * @since 3.1
 *
 */
public class EmbeddedKafkaKraftBrokerTests {

	@Test
	void testUpDown() {
		EmbeddedKafkaKraftBroker kafka = new EmbeddedKafkaKraftBroker(1, 1, "topic1");
		kafka.afterPropertiesSet();
		assertThat(StringUtils.hasText(kafka.getBrokersAsString())).isTrue();
		kafka.destroy();
	}

	@Test
	void testConsumeFromEmbeddedWithSeekToEnd() {
		EmbeddedKafkaKraftBroker kafka = new EmbeddedKafkaKraftBroker(1, 1, "seekTestTopic");
		kafka.afterPropertiesSet();
		Map<String, Object> producerProps = KafkaTestUtils.producerProps(kafka);
		KafkaProducer<Integer, String> producer = new KafkaProducer<>(producerProps);
		producer.send(new ProducerRecord<>("seekTestTopic", 0, 1, "beforeSeekToEnd"));
		Map<String, Object> consumerProps = KafkaTestUtils.consumerProps(kafka, "seekTest", false);
		KafkaConsumer<Integer, String> consumer = new KafkaConsumer<>(consumerProps);
		kafka.consumeFromAnEmbeddedTopic(consumer, true /* seekToEnd */, "seekTestTopic");
		producer.send(new ProducerRecord<>("seekTestTopic", 0, 1, "afterSeekToEnd"));
		producer.close();
		assertThat(KafkaTestUtils.getSingleRecord(consumer, "seekTestTopic").value())
				.isEqualTo("afterSeekToEnd");
		consumer.close();
	}

}
