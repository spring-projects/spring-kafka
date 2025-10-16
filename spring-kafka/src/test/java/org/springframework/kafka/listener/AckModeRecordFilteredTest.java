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

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.listener.adapter.FilteringMessageListenerAdapter;
import org.springframework.kafka.listener.adapter.RecordFilterStrategy;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.BDDMockito.given;
import static org.mockito.Mockito.*;

/**
 * Tests for the RECORD_FILTERED acknowledge mode.
 *
 * Related to GitHub issue #3562
 *
 * @author Chaedong Im
 * @see AckModeRecordWithFilteringTest
 */
public class AckModeRecordFilteredTest {

	@SuppressWarnings("unchecked")
	@Test
	public void testRecordFilteredModeOnlyCommitsProcessedRecords() throws InterruptedException {
		// Given: A container with RECORD_FILTERED ack mode
		ConsumerFactory<String, String> consumerFactory = mock(ConsumerFactory.class);
		Consumer<String, String> consumer = mock(Consumer.class);
		given(consumerFactory.createConsumer(any(), any(), any(), any())).willReturn(consumer);

		ContainerProperties containerProperties = new ContainerProperties("test-topic");
		containerProperties.setAckMode(ContainerProperties.AckMode.RECORD_FILTERED);
		containerProperties.setGroupId("test-group");

		RecordFilterStrategy<String, String> filterStrategy = record -> record.offset() % 2 == 0;

		List<String> processedValues = new ArrayList<>();
		CountDownLatch processedLatch = new CountDownLatch(2);

		MessageListener<String, String> listener = record -> {
			processedValues.add(record.value());
			processedLatch.countDown();
		};

		FilteringMessageListenerAdapter<String, String> filteringAdapter =
				new FilteringMessageListenerAdapter<>(listener, filterStrategy);
		containerProperties.setMessageListener(filteringAdapter);

		KafkaMessageListenerContainer<String, String> container =
				new KafkaMessageListenerContainer<>(consumerFactory, containerProperties);

		TopicPartition tp = new TopicPartition("test-topic", 0);
		List<ConsumerRecord<String, String>> records = List.of(
				new ConsumerRecord<>("test-topic", 0, 0, "key0", "value0"), // Will be filtered -> NO COMMIT
				new ConsumerRecord<>("test-topic", 0, 1, "key1", "value1"), // Will be processed -> COMMIT offset 2
				new ConsumerRecord<>("test-topic", 0, 2, "key2", "value2"), // Will be filtered -> NO COMMIT
				new ConsumerRecord<>("test-topic", 0, 3, "key3", "value3")  // Will be processed -> COMMIT offset 4
		);

		Map<TopicPartition, List<ConsumerRecord<String, String>>> recordsMap = new HashMap<>();
		recordsMap.put(tp, records);
		ConsumerRecords<String, String> consumerRecords = new ConsumerRecords<>(recordsMap);

		given(consumer.poll(any(Duration.class)))
				.willReturn(consumerRecords)
				.willReturn(new ConsumerRecords<>(Collections.emptyMap()));

		// When: Start the container and process records
		container.start();
		assertThat(processedLatch.await(5, TimeUnit.SECONDS)).isTrue();
		Thread.sleep(500);
		container.stop();

		// Then: Verify that only odd offset records were processed
		assertThat(processedValues).containsExactly("value1", "value3");

		verify(consumer, times(2)).commitSync(any(), any(Duration.class));
	}

	@SuppressWarnings("unchecked")
	@Test
	public void testRecordFilteredModeWithAllRecordsFiltered() throws InterruptedException {
		// Given: All records are filtered
		ConsumerFactory<String, String> consumerFactory = mock(ConsumerFactory.class);
		Consumer<String, String> consumer = mock(Consumer.class);
		given(consumerFactory.createConsumer(any(), any(), any(), any())).willReturn(consumer);

		ContainerProperties containerProperties = new ContainerProperties("test-topic");
		containerProperties.setAckMode(ContainerProperties.AckMode.RECORD_FILTERED);
		containerProperties.setGroupId("test-group");

		RecordFilterStrategy<String, String> filterStrategy = record -> true;

		List<String> processedValues = new ArrayList<>();
		MessageListener<String, String> listener = record -> processedValues.add(record.value());

		FilteringMessageListenerAdapter<String, String> filteringAdapter =
				new FilteringMessageListenerAdapter<>(listener, filterStrategy);
		containerProperties.setMessageListener(filteringAdapter);

		KafkaMessageListenerContainer<String, String> container =
				new KafkaMessageListenerContainer<>(consumerFactory, containerProperties);

		TopicPartition tp = new TopicPartition("test-topic", 0);
		List<ConsumerRecord<String, String>> records = List.of(
				new ConsumerRecord<>("test-topic", 0, 0, "key0", "value0"), // Filtered -> NO COMMIT
				new ConsumerRecord<>("test-topic", 0, 1, "key1", "value1"), // Filtered -> NO COMMIT
				new ConsumerRecord<>("test-topic", 0, 2, "key2", "value2")  // Filtered -> NO COMMIT
		);

		Map<TopicPartition, List<ConsumerRecord<String, String>>> recordsMap = new HashMap<>();
		recordsMap.put(tp, records);
		ConsumerRecords<String, String> consumerRecords = new ConsumerRecords<>(recordsMap);

		given(consumer.poll(any(Duration.class)))
				.willReturn(consumerRecords)
				.willReturn(new ConsumerRecords<>(Collections.emptyMap()));

		// When
		container.start();
		Thread.sleep(1000);
		container.stop();

		assertThat(processedValues).isEmpty();
		verify(consumer, never()).commitSync(any(), any(Duration.class));
	}

	@SuppressWarnings("unchecked")
	@Test
	public void testRecordFilteredModeWithMixedPartitions() throws InterruptedException {
		// Given: Mixed partitions with different filtering scenarios
		ConsumerFactory<String, String> consumerFactory = mock(ConsumerFactory.class);
		Consumer<String, String> consumer = mock(Consumer.class);
		given(consumerFactory.createConsumer(any(), any(), any(), any())).willReturn(consumer);

		ContainerProperties containerProperties = new ContainerProperties("test-topic");
		containerProperties.setAckMode(ContainerProperties.AckMode.RECORD_FILTERED);
		containerProperties.setGroupId("test-group");

		RecordFilterStrategy<String, String> filterStrategy = record ->
				record.value().contains("skip");

		List<String> processedValues = new ArrayList<>();
		CountDownLatch processedLatch = new CountDownLatch(3);

		MessageListener<String, String> listener = record -> {
			processedValues.add(record.value());
			processedLatch.countDown();
		};

		FilteringMessageListenerAdapter<String, String> filteringAdapter =
				new FilteringMessageListenerAdapter<>(listener, filterStrategy);
		containerProperties.setMessageListener(filteringAdapter);

		KafkaMessageListenerContainer<String, String> container =
				new KafkaMessageListenerContainer<>(consumerFactory, containerProperties);

		TopicPartition tp0 = new TopicPartition("test-topic", 0);
		TopicPartition tp1 = new TopicPartition("test-topic", 1);

		List<ConsumerRecord<String, String>> records = List.of(
				// Partition 0
				new ConsumerRecord<>("test-topic", 0, 0, "key0", "process1"), // Processed -> COMMIT offset 1
				new ConsumerRecord<>("test-topic", 0, 1, "key1", "skip1"),    // Filtered -> NO COMMIT
				new ConsumerRecord<>("test-topic", 0, 2, "key2", "process2"), // Processed -> COMMIT offset 3
				// Partition 1
				new ConsumerRecord<>("test-topic", 1, 0, "key3", "skip2"),    // Filtered -> NO COMMIT
				new ConsumerRecord<>("test-topic", 1, 1, "key4", "process3"), // Processed -> COMMIT offset 2
				new ConsumerRecord<>("test-topic", 1, 2, "key5", "skip3")     // Filtered -> NO COMMIT
		);

		Map<TopicPartition, List<ConsumerRecord<String, String>>> recordsMap = new HashMap<>();
		recordsMap.put(tp0, records.subList(0, 3));
		recordsMap.put(tp1, records.subList(3, 6));
		ConsumerRecords<String, String> consumerRecords = new ConsumerRecords<>(recordsMap);

		given(consumer.poll(any(Duration.class)))
				.willReturn(consumerRecords)
				.willReturn(new ConsumerRecords<>(Collections.emptyMap()));

		// When
		container.start();
		assertThat(processedLatch.await(5, TimeUnit.SECONDS)).isTrue();
		Thread.sleep(500);
		container.stop();

		assertThat(processedValues).containsExactly("process1", "process2", "process3");
		verify(consumer, times(3)).commitSync(any(), any(Duration.class));
	}

	@SuppressWarnings("unchecked")
	@Test
	public void testRecordFilteredModeEfficiencyGains() throws InterruptedException {
		ConsumerFactory<String, String> consumerFactory = mock(ConsumerFactory.class);
		Consumer<String, String> consumer = mock(Consumer.class);
		given(consumerFactory.createConsumer(any(), any(), any(), any())).willReturn(consumer);

		ContainerProperties containerProperties = new ContainerProperties("test-topic");
		containerProperties.setAckMode(ContainerProperties.AckMode.RECORD_FILTERED);
		containerProperties.setGroupId("test-group");

		RecordFilterStrategy<String, String> filterStrategy = record -> record.offset() % 10 != 0;

		List<String> processedValues = new ArrayList<>();
		CountDownLatch processedLatch = new CountDownLatch(1);

		MessageListener<String, String> listener = record -> {
			processedValues.add(record.value());
			processedLatch.countDown();
		};

		FilteringMessageListenerAdapter<String, String> filteringAdapter =
				new FilteringMessageListenerAdapter<>(listener, filterStrategy);
		containerProperties.setMessageListener(filteringAdapter);

		KafkaMessageListenerContainer<String, String> container =
				new KafkaMessageListenerContainer<>(consumerFactory, containerProperties);

		TopicPartition tp = new TopicPartition("test-topic", 0);
		List<ConsumerRecord<String, String>> records = new ArrayList<>();
		for (int i = 0; i < 10; i++) {
			records.add(new ConsumerRecord<>("test-topic", 0, i, "key" + i, "value" + i));
		}

		Map<TopicPartition, List<ConsumerRecord<String, String>>> recordsMap = new HashMap<>();
		recordsMap.put(tp, records);
		ConsumerRecords<String, String> consumerRecords = new ConsumerRecords<>(recordsMap);

		given(consumer.poll(any(Duration.class)))
				.willReturn(consumerRecords)
				.willReturn(new ConsumerRecords<>(Collections.emptyMap()));

		// When
		container.start();
		assertThat(processedLatch.await(5, TimeUnit.SECONDS)).isTrue();
		Thread.sleep(500);
		container.stop();

		assertThat(processedValues).hasSize(1);
		assertThat(processedValues.get(0)).isEqualTo("value0");
		verify(consumer, times(1)).commitSync(any(), any(Duration.class));
	}

	@SuppressWarnings("unchecked")
	@Test
	public void testRecordFilteredModeDoesNotBreakNormalProcessing() throws InterruptedException {
		ConsumerFactory<String, String> consumerFactory = mock(ConsumerFactory.class);
		Consumer<String, String> consumer = mock(Consumer.class);
		given(consumerFactory.createConsumer(any(), any(), any(), any())).willReturn(consumer);

		ContainerProperties containerProperties = new ContainerProperties("test-topic");
		containerProperties.setAckMode(ContainerProperties.AckMode.RECORD_FILTERED);
		containerProperties.setGroupId("test-group");

		RecordFilterStrategy<String, String> filterStrategy = record -> false;

		List<String> processedValues = new ArrayList<>();
		CountDownLatch processedLatch = new CountDownLatch(3);

		MessageListener<String, String> listener = record -> {
			processedValues.add(record.value());
			processedLatch.countDown();
		};

		FilteringMessageListenerAdapter<String, String> filteringAdapter =
				new FilteringMessageListenerAdapter<>(listener, filterStrategy);
		containerProperties.setMessageListener(filteringAdapter);

		KafkaMessageListenerContainer<String, String> container =
				new KafkaMessageListenerContainer<>(consumerFactory, containerProperties);

		TopicPartition tp = new TopicPartition("test-topic", 0);
		List<ConsumerRecord<String, String>> records = List.of(
				new ConsumerRecord<>("test-topic", 0, 0, "key0", "value0"),
				new ConsumerRecord<>("test-topic", 0, 1, "key1", "value1"),
				new ConsumerRecord<>("test-topic", 0, 2, "key2", "value2")
		);

		Map<TopicPartition, List<ConsumerRecord<String, String>>> recordsMap = new HashMap<>();
		recordsMap.put(tp, records);
		ConsumerRecords<String, String> consumerRecords = new ConsumerRecords<>(recordsMap);

		given(consumer.poll(any(Duration.class)))
				.willReturn(consumerRecords)
				.willReturn(new ConsumerRecords<>(Collections.emptyMap()));

		// When
		container.start();
		assertThat(processedLatch.await(5, TimeUnit.SECONDS)).isTrue();
		Thread.sleep(500);
		container.stop();

		// Then: All records processed
		assertThat(processedValues).containsExactly("value0", "value1", "value2");
		verify(consumer, times(3)).commitSync(any(), any(Duration.class));
	}
}
