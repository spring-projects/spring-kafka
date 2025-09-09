/*
 * Copyright 2025-present the original author or authors.
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

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.IntegerDeserializer;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.Test;

import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.listener.ContainerProperties.AckMode;
import org.springframework.kafka.listener.adapter.FilteringBatchMessageListenerAdapter;
import org.springframework.kafka.listener.adapter.FilteringMessageListenerAdapter;
import org.springframework.kafka.listener.adapter.RecordFilterStrategy;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.kafka.support.FilterAwareAcknowledgment;
import org.springframework.kafka.support.TopicPartitionOffset;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.condition.EmbeddedKafkaCondition;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.kafka.test.utils.ContainerTestUtils;
import org.springframework.kafka.test.utils.KafkaTestUtils;

/**
 * Tests for the RECORD_FILTERED acknowledgment mode.
 *
 * @author Spring Kafka Team
 * @since 3.1
 */
@EmbeddedKafka(topics = RecordFilteredAckModeTests.TOPIC)
public class RecordFilteredAckModeTests {

	static final String TOPIC = "record-filtered-test";

	@Test
	public void testRecordFilteredAckModeBasic() throws InterruptedException {
		EmbeddedKafkaBroker embeddedKafka = EmbeddedKafkaCondition.getBroker();
		Map<String, Object> consumerProps = KafkaTestUtils.consumerProps("testGroup", "false", embeddedKafka);
		consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
		ConsumerFactory<Integer, String> cf = new DefaultKafkaConsumerFactory<>(consumerProps);

		ContainerProperties containerProperties = new ContainerProperties(TOPIC);
		containerProperties.setAckMode(AckMode.RECORD_FILTERED);
		
		CountDownLatch latch = new CountDownLatch(2); // Only expect 2 non-filtered messages
		List<String> received = new ArrayList<>();
		AtomicReference<Consumer<Integer, String>> consumerRef = new AtomicReference<>();

		// Filter out messages with value "filtered"
		RecordFilterStrategy<Integer, String> filter = record -> "filtered".equals(record.value());
		
		MessageListener<Integer, String> listener = new FilteringMessageListenerAdapter<>(
				record -> {
					received.add(record.value());
					latch.countDown();
				}, filter);

		KafkaMessageListenerContainer<Integer, String> container = 
			new KafkaMessageListenerContainer<>(cf, containerProperties);
		container.setupMessageListener(listener);
		container.start();

		ContainerTestUtils.waitForAssignment(container, 1);

		// Send test messages
		Map<String, Object> producerProps = KafkaTestUtils.producerProps(embeddedKafka);
		ProducerFactory<Integer, String> pf = new DefaultKafkaProducerFactory<>(producerProps);
		KafkaTemplate<Integer, String> template = new KafkaTemplate<>(pf);

		template.send(TOPIC, 0, "message1");
		template.send(TOPIC, 0, "filtered");  // This should be filtered
		template.send(TOPIC, 0, "message2");
		template.send(TOPIC, 0, "filtered");  // This should be filtered
		template.send(TOPIC, 0, "message3");

		assertThat(latch.await(10, TimeUnit.SECONDS)).isTrue();
		
		// Verify only non-filtered messages were received
		assertThat(received).containsExactly("message1", "message2");
		
		// Give some time for potential commits
		Thread.sleep(1000);
		
		container.stop();
		pf.destroy();
	}

	@Test
	public void testRecordFilteredAckModeWithFilterAwareAcknowledgment() throws InterruptedException {
		EmbeddedKafkaBroker embeddedKafka = EmbeddedKafkaCondition.getBroker();
		Map<String, Object> consumerProps = KafkaTestUtils.consumerProps("testGroup2", "false", embeddedKafka);
		consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
		ConsumerFactory<Integer, String> cf = new DefaultKafkaConsumerFactory<>(consumerProps);

		ContainerProperties containerProperties = new ContainerProperties(TOPIC + "2");
		containerProperties.setAckMode(AckMode.RECORD_FILTERED);
		
		CountDownLatch latch = new CountDownLatch(1);
		AtomicReference<FilterAwareAcknowledgment> ackRef = new AtomicReference<>();
		AtomicInteger filteredCount = new AtomicInteger();

		AcknowledgingMessageListener<Integer, String> listener = (record, acknowledgment) -> {
			if ("filtered".equals(record.value())) {
				if (acknowledgment instanceof FilterAwareAcknowledgment faa) {
					faa.markFiltered(record);
					ackRef.set(faa);
					filteredCount.incrementAndGet();
				}
			} else {
				latch.countDown();
			}
		};

		KafkaMessageListenerContainer<Integer, String> container = 
			new KafkaMessageListenerContainer<>(cf, containerProperties);
		container.setupMessageListener(listener);
		container.start();

		ContainerTestUtils.waitForAssignment(container, 1);

		// Send test messages  
		Map<String, Object> producerProps = KafkaTestUtils.producerProps(embeddedKafka);
		ProducerFactory<Integer, String> pf = new DefaultKafkaProducerFactory<>(producerProps);
		KafkaTemplate<Integer, String> template = new KafkaTemplate<>(pf);

		template.send(TOPIC + "2", 0, "filtered");
		template.send(TOPIC + "2", 0, "message1");

		assertThat(latch.await(10, TimeUnit.SECONDS)).isTrue();
		
		// Verify filter aware acknowledgment was used
		assertThat(ackRef.get()).isNotNull();
		assertThat(filteredCount.get()).isEqualTo(1);
		
		container.stop();
		pf.destroy();
	}

	@Test
	public void testRecordFilteredAckModeAutoCommitDisabled() throws InterruptedException {
		EmbeddedKafkaBroker embeddedKafka = EmbeddedKafkaCondition.getBroker();
		Map<String, Object> consumerProps = KafkaTestUtils.consumerProps("testGroup3", "true", embeddedKafka);
		consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
		ConsumerFactory<Integer, String> cf = new DefaultKafkaConsumerFactory<>(consumerProps);

		ContainerProperties containerProperties = new ContainerProperties(TOPIC + "3");
		containerProperties.setAckMode(AckMode.RECORD_FILTERED);
		
		CountDownLatch latch = new CountDownLatch(1);
		AtomicReference<Consumer<Integer, String>> consumerRef = new AtomicReference<>();

		AcknowledgingConsumerAwareMessageListener<Integer, String> listener = (record, acknowledgment, consumer) -> {
			consumerRef.set(consumer);
			latch.countDown();
		};

		KafkaMessageListenerContainer<Integer, String> container = 
			new KafkaMessageListenerContainer<>(cf, containerProperties);
		container.setupMessageListener(listener);
		container.start();

		ContainerTestUtils.waitForAssignment(container, 1);

		// Send a test message
		Map<String, Object> producerProps = KafkaTestUtils.producerProps(embeddedKafka);
		ProducerFactory<Integer, String> pf = new DefaultKafkaProducerFactory<>(producerProps);
		KafkaTemplate<Integer, String> template = new KafkaTemplate<>(pf);

		template.send(TOPIC + "3", 0, "message1");

		assertThat(latch.await(10, TimeUnit.SECONDS)).isTrue();
		
		// Verify auto-commit was forced to false
		Consumer<Integer, String> consumer = consumerRef.get();
		assertThat(consumer).isNotNull();
		
		container.stop();
		pf.destroy();
	}

	@Test
	public void testRecordFilteredAckModeBatch() throws InterruptedException {
		EmbeddedKafkaBroker embeddedKafka = EmbeddedKafkaCondition.getBroker();
		Map<String, Object> consumerProps = KafkaTestUtils.consumerProps("testGroup4", "false", embeddedKafka);
		consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
		consumerProps.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 10);
		ConsumerFactory<Integer, String> cf = new DefaultKafkaConsumerFactory<>(consumerProps);

		ContainerProperties containerProperties = new ContainerProperties(TOPIC + "4");
		containerProperties.setAckMode(AckMode.RECORD_FILTERED);
		
		CountDownLatch latch = new CountDownLatch(1);
		List<String> received = new ArrayList<>();
		AtomicInteger filteredCount = new AtomicInteger();

		// Filter out messages with value "filtered"
		RecordFilterStrategy<Integer, String> filter = record -> "filtered".equals(record.value());
		
		BatchAcknowledgingMessageListener<Integer, String> batchListener = (records, acknowledgment) -> {
			for (ConsumerRecord<Integer, String> record : records) {
				if (filter.filter(record)) {
					if (acknowledgment instanceof FilterAwareAcknowledgment faa) {
						faa.markFiltered(record);
						filteredCount.incrementAndGet();
					}
				} else {
					received.add(record.value());
				}
			}
			latch.countDown();
		};

		BatchMessageListener<Integer, String> listener = new FilteringBatchMessageListenerAdapter<>(
				batchListener, filter);

		KafkaMessageListenerContainer<Integer, String> container = 
			new KafkaMessageListenerContainer<>(cf, containerProperties);
		container.setupMessageListener(listener);
		container.start();

		ContainerTestUtils.waitForAssignment(container, 1);

		// Send test messages
		Map<String, Object> producerProps = KafkaTestUtils.producerProps(embeddedKafka);
		ProducerFactory<Integer, String> pf = new DefaultKafkaProducerFactory<>(producerProps);
		KafkaTemplate<Integer, String> template = new KafkaTemplate<>(pf);

		template.send(TOPIC + "4", 0, "message1");
		template.send(TOPIC + "4", 0, "filtered");
		template.send(TOPIC + "4", 0, "message2");
		template.send(TOPIC + "4", 0, "filtered");
		template.send(TOPIC + "4", 0, "message3");

		assertThat(latch.await(10, TimeUnit.SECONDS)).isTrue();
		
		// Verify only non-filtered messages were received
		assertThat(received).containsExactly("message1", "message2", "message3");
		assertThat(filteredCount.get()).isEqualTo(2);
		
		container.stop();
		pf.destroy();
	}
}