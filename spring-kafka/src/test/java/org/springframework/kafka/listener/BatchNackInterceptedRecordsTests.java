/*
 * Copyright 2026-present the original author or authors.
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


package org.springframework.kafka.listener;

import java.time.Duration;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.Test;

import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.ContainerProperties.AckMode;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.condition.EmbeddedKafkaCondition;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.kafka.test.utils.KafkaTestUtils;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * @author Sharang Gupta
 *
 * @since 4.2
 */
@EmbeddedKafka(topics = BatchNackInterceptedRecordsTests.TOPIC, partitions = 1)
class BatchNackInterceptedRecordsTests {

	static final String TOPIC = "batchNackInterceptedRecords";

	@Test
	void nackIndexRefersToTheRecordsReturnedByTheBatchInterceptor() {
		EmbeddedKafkaBroker broker = EmbeddedKafkaCondition.getBroker();
		KafkaTemplate<Integer, String> template =
				new KafkaTemplate<>(new DefaultKafkaProducerFactory<>(KafkaTestUtils.producerProps(broker)));
		template.send(TOPIC, 0, null, "dropped");
		template.send(TOPIC, 0, null, "processed");
		template.send(TOPIC, 0, null, "redelivered");
		template.flush();

		Map<String, Object> consumerProps = KafkaTestUtils.consumerProps(broker, "batchNackIntercepted", false);
		consumerProps.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 3);
		ContainerProperties containerProps = new ContainerProperties(TOPIC);
		containerProps.setAckMode(AckMode.MANUAL);
		List<List<String>> batches = new CopyOnWriteArrayList<>();
		AtomicBoolean nacked = new AtomicBoolean();
		containerProps.setMessageListener((BatchAcknowledgingMessageListener<Integer, String>) (records, ack) -> {
			batches.add(records.stream().map(ConsumerRecord::value).toList());
			// The listener sees [processed, redelivered]: the first is processed, the second is redelivered.
			if (records.size() == 2 && nacked.compareAndSet(false, true)) {
				ack.nack(1, Duration.ofMillis(100));
			}
			else {
				ack.acknowledge();
			}
		});
		KafkaMessageListenerContainer<Integer, String> container =
				new KafkaMessageListenerContainer<>(new DefaultKafkaConsumerFactory<>(consumerProps), containerProps);
		container.setBatchInterceptor((records, consumer) -> withoutDroppedRecord(records));
		container.start();
		try {
			await().atMost(Duration.ofSeconds(30)).untilAsserted(() ->
					assertThat(batches).containsExactly(List.of("processed", "redelivered"), List.of("redelivered")));
		}
		finally {
			container.stop();
			template.destroy();
		}
	}

	private static ConsumerRecords<Integer, String> withoutDroppedRecord(ConsumerRecords<Integer, String> records) {
		Map<TopicPartition, List<ConsumerRecord<Integer, String>>> kept = new LinkedHashMap<>();
		for (ConsumerRecord<Integer, String> record : records) {
			if (!"dropped".equals(record.value())) {
				kept.computeIfAbsent(new TopicPartition(record.topic(), record.partition()), tp -> new ArrayList<>())
						.add(record);
			}
		}
		return new ConsumerRecords<>(kept, Map.of());
	}

}
