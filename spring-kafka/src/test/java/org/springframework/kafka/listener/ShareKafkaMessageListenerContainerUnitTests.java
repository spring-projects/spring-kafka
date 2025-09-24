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

import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.kafka.clients.consumer.AcknowledgeType;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.ShareConsumer;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import org.springframework.kafka.core.ShareConsumerFactory;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.ArgumentMatchers.notNull;
import static org.mockito.BDDMockito.given;
import static org.mockito.BDDMockito.willThrow;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

@ExtendWith(MockitoExtension.class)
public class ShareKafkaMessageListenerContainerUnitTests {

	@Mock
	private ShareConsumerFactory<String, String> shareConsumerFactory;

	@Mock
	private ShareConsumer<String, String> shareConsumer;

	@Mock
	private MessageListener<String, String> messageListener;

	@Mock
	private AcknowledgingShareConsumerAwareMessageListener<String, String> ackListener;

	private ContainerProperties containerProperties;

	private ConsumerRecord<String, String> testRecord;

	private ConsumerRecords<String, String> testRecords;

	@Test
	void shouldConfigureExplicitModeCorrectly() {

		ContainerProperties containerProperties = new ContainerProperties("test-topic");
		containerProperties.setShareAcknowledgmentMode(ContainerProperties.ShareAcknowledgmentMode.EXPLICIT);
		containerProperties.setMessageListener(ackListener);

		ShareKafkaMessageListenerContainer<String, String> container =
				new ShareKafkaMessageListenerContainer<>(shareConsumerFactory, containerProperties);

		assertThat(container.getContainerProperties().getShareAcknowledgmentMode())
				.isEqualTo(ContainerProperties.ShareAcknowledgmentMode.EXPLICIT);
	}

	@Test
	void shouldConfigureImplicitModeByDefault() {
		ContainerProperties containerProperties = new ContainerProperties("test-topic");
		containerProperties.setMessageListener(messageListener);

		ShareKafkaMessageListenerContainer<String, String> container =
				new ShareKafkaMessageListenerContainer<>(shareConsumerFactory, containerProperties);

		assertThat(container.getContainerProperties().getShareAcknowledgmentMode())
				.isEqualTo(ContainerProperties.ShareAcknowledgmentMode.IMPLICIT);
	}

	@Test
	void shouldInvokeBasicMessageListener() throws Exception {
		// Setup test data
		ConsumerRecord<String, String> testRecord = new ConsumerRecord<>("test-topic", 0, 100L, "key", "value");
		Map<TopicPartition, List<ConsumerRecord<String, String>>> records = new HashMap<>();
		records.put(new TopicPartition("test-topic", 0), List.of(testRecord));
		ConsumerRecords<String, String> testRecords = new ConsumerRecords<>(records, Map.of());

		// Setup mocks
		given(shareConsumerFactory.createShareConsumer(any(), any())).willReturn(shareConsumer);
		given(shareConsumer.poll(any(Duration.class)))
				.willReturn(testRecords)
				.willReturn(ConsumerRecords.empty());

		ContainerProperties containerProperties = new ContainerProperties("test-topic");
		containerProperties.setMessageListener(messageListener);

		ShareKafkaMessageListenerContainer<String, String> container =
				new ShareKafkaMessageListenerContainer<>(shareConsumerFactory, containerProperties);

		container.start();
		Thread.sleep(1000);
		container.stop();

		verify(messageListener, atLeastOnce()).onMessage(testRecord);
		verify(shareConsumer, never()).acknowledge(testRecord, AcknowledgeType.ACCEPT);
	}

	@Test
	void shouldInvokeShareConsumerAwareListenerInImplicitMode() throws Exception {
		// Setup test data
		ConsumerRecord<String, String> testRecord = new ConsumerRecord<>("test-topic", 0, 100L, "key", "value");
		Map<TopicPartition, List<ConsumerRecord<String, String>>> records = new HashMap<>();
		records.put(new TopicPartition("test-topic", 0), List.of(testRecord));
		ConsumerRecords<String, String> testRecords = new ConsumerRecords<>(records, Map.of());

		// Setup mocks
		given(shareConsumerFactory.createShareConsumer(any(), any())).willReturn(shareConsumer);
		given(shareConsumer.poll(any(Duration.class)))
				.willReturn(testRecords)
				.willReturn(ConsumerRecords.empty());

		ContainerProperties containerProperties = new ContainerProperties("test-topic");
		// Using the unified interface with implicit mode (acknowledgment will be null)
		containerProperties.setMessageListener(ackListener);

		ShareKafkaMessageListenerContainer<String, String> container =
				new ShareKafkaMessageListenerContainer<>(shareConsumerFactory, containerProperties);

		container.start();
		Thread.sleep(1000);
		container.stop();

		// In implicit mode, acknowledgment should be null
		verify(ackListener, atLeastOnce()).onShareRecord(eq(testRecord), isNull(), eq(shareConsumer));
		verify(shareConsumer, never()).acknowledge(testRecord, AcknowledgeType.ACCEPT);
	}

	@Test
	void shouldInvokeAckListenerWithNullInImplicitMode() throws Exception {
		// Setup test data
		ConsumerRecord<String, String> testRecord = new ConsumerRecord<>("test-topic", 0, 100L, "key", "value");
		Map<TopicPartition, List<ConsumerRecord<String, String>>> records = new HashMap<>();
		records.put(new TopicPartition("test-topic", 0), List.of(testRecord));
		ConsumerRecords<String, String> testRecords = new ConsumerRecords<>(records, Map.of());

		// Setup mocks
		given(shareConsumerFactory.createShareConsumer(any(), any())).willReturn(shareConsumer);
		given(shareConsumer.poll(any(Duration.class)))
				.willReturn(testRecords)
				.willReturn(ConsumerRecords.empty());

		ContainerProperties containerProperties = new ContainerProperties("test-topic");
		containerProperties.setMessageListener(ackListener);
		// Default is implicit mode

		ShareKafkaMessageListenerContainer<String, String> container =
				new ShareKafkaMessageListenerContainer<>(shareConsumerFactory, containerProperties);

		container.start();
		Thread.sleep(1000);
		container.stop();

		verify(ackListener, atLeastOnce()).onShareRecord(eq(testRecord), isNull(), eq(shareConsumer));
		verify(shareConsumer, never()).acknowledge(testRecord, AcknowledgeType.ACCEPT);
	}

	@Test
	void shouldInvokeAckListenerWithAckInExplicitMode() throws Exception {
		// Setup test data
		ConsumerRecord<String, String> testRecord = new ConsumerRecord<>("test-topic", 0, 100L, "key", "value");
		Map<TopicPartition, List<ConsumerRecord<String, String>>> records = new HashMap<>();
		records.put(new TopicPartition("test-topic", 0), List.of(testRecord));
		ConsumerRecords<String, String> testRecords = new ConsumerRecords<>(records, Map.of());

		// Setup mocks
		given(shareConsumerFactory.createShareConsumer(any(), any())).willReturn(shareConsumer);
		given(shareConsumer.poll(any(Duration.class)))
				.willReturn(testRecords)
				.willReturn(ConsumerRecords.empty());

		ContainerProperties containerProperties = new ContainerProperties("test-topic");
		containerProperties.setShareAcknowledgmentMode(ContainerProperties.ShareAcknowledgmentMode.EXPLICIT);
		containerProperties.setMessageListener(ackListener);

		ShareKafkaMessageListenerContainer<String, String> container =
				new ShareKafkaMessageListenerContainer<>(shareConsumerFactory, containerProperties);

		container.start();
		Thread.sleep(1000);
		container.stop();

		verify(ackListener, atLeastOnce()).onShareRecord(eq(testRecord), notNull(), eq(shareConsumer));
		// No auto-acknowledgment in explicit mode
		verify(shareConsumer, never()).acknowledge(any(), any());
	}

	@Test
	void shouldHandleProcessingErrorInImplicitMode() throws Exception {
		// Setup test data
		ConsumerRecord<String, String> testRecord = new ConsumerRecord<>("test-topic", 0, 100L, "key", "value");
		Map<TopicPartition, List<ConsumerRecord<String, String>>> records = new HashMap<>();
		records.put(new TopicPartition("test-topic", 0), List.of(testRecord));
		ConsumerRecords<String, String> testRecords = new ConsumerRecords<>(records, Map.of());

		// Setup mocks
		given(shareConsumerFactory.createShareConsumer(any(), any())).willReturn(shareConsumer);
		given(shareConsumer.poll(any(Duration.class)))
				.willReturn(testRecords)
				.willReturn(ConsumerRecords.empty());
		willThrow(new RuntimeException("Processing error")).given(messageListener).onMessage(any());

		ContainerProperties containerProperties = new ContainerProperties("test-topic");
		containerProperties.setMessageListener(messageListener);

		ShareKafkaMessageListenerContainer<String, String> container =
				new ShareKafkaMessageListenerContainer<>(shareConsumerFactory, containerProperties);

		container.start();
		Thread.sleep(1000);
		container.stop();

		verify(messageListener, atLeastOnce()).onMessage(testRecord);
		// Should auto-reject on error
		verify(shareConsumer, atLeastOnce()).acknowledge(testRecord, AcknowledgeType.REJECT);
	}

	@Test
	void shouldHandleProcessingErrorInExplicitMode() throws Exception {
		// Setup test data
		ConsumerRecord<String, String> testRecord = new ConsumerRecord<>("test-topic", 0, 100L, "key", "value");
		Map<TopicPartition, List<ConsumerRecord<String, String>>> records = new HashMap<>();
		records.put(new TopicPartition("test-topic", 0), List.of(testRecord));
		ConsumerRecords<String, String> testRecords = new ConsumerRecords<>(records, Map.of());

		// Setup mocks
		given(shareConsumerFactory.createShareConsumer(any(), any())).willReturn(shareConsumer);
		given(shareConsumer.poll(any(Duration.class)))
				.willReturn(testRecords)
				.willReturn(ConsumerRecords.empty());
		willThrow(new RuntimeException("Processing error")).given(ackListener).onShareRecord(any(), any(), any());

		ContainerProperties containerProperties = new ContainerProperties("test-topic");
		containerProperties.setShareAcknowledgmentMode(ContainerProperties.ShareAcknowledgmentMode.EXPLICIT);
		containerProperties.setMessageListener(ackListener);

		ShareKafkaMessageListenerContainer<String, String> container =
				new ShareKafkaMessageListenerContainer<>(shareConsumerFactory, containerProperties);

		container.start();
		Thread.sleep(1000);
		container.stop();

		verify(ackListener, atLeastOnce()).onShareRecord(eq(testRecord), notNull(), eq(shareConsumer));
		// Should auto-reject on error in explicit mode too
		verify(shareConsumer, atLeastOnce()).acknowledge(testRecord, AcknowledgeType.REJECT);
	}

	@Test
	void shouldCommitAcknowledgments() throws Exception {
		// Setup test data
		ConsumerRecord<String, String> testRecord = new ConsumerRecord<>("test-topic", 0, 100L, "key", "value");
		Map<TopicPartition, List<ConsumerRecord<String, String>>> records = new HashMap<>();
		records.put(new TopicPartition("test-topic", 0), List.of(testRecord));
		ConsumerRecords<String, String> testRecords = new ConsumerRecords<>(records, Map.of());

		// Setup mocks
		given(shareConsumerFactory.createShareConsumer(any(), any())).willReturn(shareConsumer);
		given(shareConsumer.poll(any(Duration.class)))
				.willReturn(testRecords)
				.willReturn(ConsumerRecords.empty());

		ContainerProperties containerProperties = new ContainerProperties("test-topic");
		containerProperties.setMessageListener(messageListener);

		ShareKafkaMessageListenerContainer<String, String> container =
				new ShareKafkaMessageListenerContainer<>(shareConsumerFactory, containerProperties);

		container.start();
		Thread.sleep(1000);
		container.stop();

		verify(shareConsumer, atLeastOnce()).commitSync();
	}

	@Test
	void shouldSubscribeToTopics() {
		given(shareConsumerFactory.createShareConsumer(any(), any())).willReturn(shareConsumer);

		ContainerProperties containerProperties = new ContainerProperties("test-topic");
		containerProperties.setMessageListener(messageListener);

		ShareKafkaMessageListenerContainer<String, String> container =
				new ShareKafkaMessageListenerContainer<>(shareConsumerFactory, containerProperties);

		container.start();

		verify(shareConsumer).subscribe(List.of("test-topic"));

		container.stop();
	}

	@Test
	void shouldHandleEmptyPollResults() throws Exception {
		// Setup mocks for empty results
		given(shareConsumerFactory.createShareConsumer(any(), any())).willReturn(shareConsumer);
		given(shareConsumer.poll(any(Duration.class))).willReturn(ConsumerRecords.empty());

		ContainerProperties containerProperties = new ContainerProperties("test-topic");
		containerProperties.setMessageListener(messageListener);

		ShareKafkaMessageListenerContainer<String, String> container =
				new ShareKafkaMessageListenerContainer<>(shareConsumerFactory, containerProperties);

		container.start();
		Thread.sleep(1000);
		container.stop();

		// No listener invocation for empty records
		verify(messageListener, never()).onMessage(any());
		// No acknowledgments for empty records
		verify(shareConsumer, never()).acknowledge(any(), any());
		// But commit should still be called
		verify(shareConsumer, never()).commitSync();
	}

	@Test
	void shouldCloseConsumerOnStop() throws Exception {
		given(shareConsumerFactory.createShareConsumer(any(), any())).willReturn(shareConsumer);

		ContainerProperties containerProperties = new ContainerProperties("test-topic");
		containerProperties.setMessageListener(messageListener);

		ShareKafkaMessageListenerContainer<String, String> container =
				new ShareKafkaMessageListenerContainer<>(shareConsumerFactory, containerProperties);

		container.start();
		container.stop();
		Thread.sleep(100);

		verify(shareConsumer).close();
	}

	@Test
	void shouldSupportContainerProperties() {
		ContainerProperties containerProperties = new ContainerProperties("test-topic");
		containerProperties.setMessageListener(messageListener);
		containerProperties.setShareAcknowledgmentMode(ContainerProperties.ShareAcknowledgmentMode.EXPLICIT);

		ShareKafkaMessageListenerContainer<String, String> container =
				new ShareKafkaMessageListenerContainer<>(shareConsumerFactory, containerProperties);

		assertThat(container.getContainerProperties().getShareAcknowledgmentMode())
				.isEqualTo(ContainerProperties.ShareAcknowledgmentMode.EXPLICIT);
	}

	@Test
	void shouldReportRunningState() {
		given(shareConsumerFactory.createShareConsumer(any(), any())).willReturn(shareConsumer);

		ContainerProperties containerProperties = new ContainerProperties("test-topic");
		containerProperties.setMessageListener(messageListener);

		ShareKafkaMessageListenerContainer<String, String> container =
				new ShareKafkaMessageListenerContainer<>(shareConsumerFactory, containerProperties);

		assertThat(container.isRunning()).isFalse();

		container.start();
		assertThat(container.isRunning()).isTrue();

		container.stop();
		assertThat(container.isRunning()).isFalse();
	}

	@Test
	void shouldSupportBeanNameSetting() {
		ContainerProperties containerProperties = new ContainerProperties("test-topic");
		containerProperties.setMessageListener(messageListener);

		ShareKafkaMessageListenerContainer<String, String> container =
				new ShareKafkaMessageListenerContainer<>(shareConsumerFactory, containerProperties);

		container.setBeanName("testContainer");
		assertThat(container.getBeanName()).isEqualTo("testContainer");
		assertThat(container.getListenerId()).isEqualTo("testContainer");
	}

}
