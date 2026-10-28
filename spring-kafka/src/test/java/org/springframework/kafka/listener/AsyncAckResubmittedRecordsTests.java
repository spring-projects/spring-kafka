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
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.Test;

import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.listener.ContainerProperties.AckMode;
import org.springframework.kafka.listener.ContainerProperties.AssignmentCommitOption;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.kafka.support.TopicPartitionOffset;
import org.springframework.util.backoff.FixedBackOff;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyMap;
import static org.mockito.BDDMockito.given;
import static org.mockito.BDDMockito.willAnswer;
import static org.mockito.Mockito.mock;

/**
 * @author Sharang Gupta
 *
 * @since 4.2
 */
public class AsyncAckResubmittedRecordsTests {

	private static final TopicPartition TP0 = new TopicPartition("foo", 0);

	private static final TopicPartition TP1 = new TopicPartition("foo", 1);

	@Test
	void pendingAckOnAnotherPartitionSurvivesResubmittedRecords() throws Exception {
		ConsumerFactory<Integer, String> cf = mock();
		Consumer<Integer, String> consumer = mock();
		given(cf.createConsumer(any(), any(), any(), any())).willReturn(consumer);
		Map<TopicPartition, List<ConsumerRecord<Integer, String>>> records = new LinkedHashMap<>();
		records.put(TP0, List.of(new ConsumerRecord<>("foo", 0, 0L, 0, "a1")));
		records.put(TP1, List.of(new ConsumerRecord<>("foo", 1, 0L, 0, "b1"),
				new ConsumerRecord<>("foo", 1, 1L, 0, "b2")));
		AtomicBoolean first = new AtomicBoolean(true);
		given(consumer.poll(any(Duration.class))).willAnswer(i -> {
			if (first.getAndSet(false)) {
				return new ConsumerRecords<>(records, Map.of());
			}
			Thread.sleep(50);
			return ConsumerRecords.empty();
		});
		given(consumer.paused()).willReturn(Collections.emptySet());
		List<Map<TopicPartition, OffsetAndMetadata>> commits = Collections.synchronizedList(new ArrayList<>());
		CountDownLatch partition1Committed = new CountDownLatch(2);
		willAnswer(i -> {
			Map<TopicPartition, OffsetAndMetadata> commit = i.getArgument(0);
			commits.add(new LinkedHashMap<>(commit));
			if (commit.containsKey(TP1)) {
				partition1Committed.countDown();
			}
			return null;
		}).given(consumer).commitSync(anyMap(), any());

		ContainerProperties containerProps = new ContainerProperties(new TopicPartitionOffset("foo", 0),
				new TopicPartitionOffset("foo", 1));
		containerProps.setGroupId("grp");
		containerProps.setAckMode(AckMode.MANUAL_IMMEDIATE);
		containerProps.setAsyncAcks(true);
		containerProps.setAssignmentCommitOption(AssignmentCommitOption.NEVER);
		AtomicReference<Acknowledgment> pendingAck = new AtomicReference<>();
		AtomicBoolean failedOnce = new AtomicBoolean();
		containerProps.setMessageListener((AcknowledgingMessageListener<Integer, String>) (record, ack) -> {
			if ("a1".equals(record.value())) {
				pendingAck.set(ack);
				return;
			}
			if ("b1".equals(record.value()) && failedOnce.compareAndSet(false, true)) {
				throw new RuntimeException("b1 fails once");
			}
			ack.acknowledge();
		});
		KafkaMessageListenerContainer<Integer, String> container =
				new KafkaMessageListenerContainer<>(cf, containerProps);
		DefaultErrorHandler errorHandler = new DefaultErrorHandler(new FixedBackOff(0L, 1L));
		errorHandler.setSeekAfterError(false);
		container.setCommonErrorHandler(errorHandler);
		container.start();
		try {
			assertThat(partition1Committed.await(10, TimeUnit.SECONDS)).isTrue();
			assertThat(commits).containsExactly(
					Map.of(TP1, new OffsetAndMetadata(1L)),
					Map.of(TP1, new OffsetAndMetadata(2L)));
			pendingAck.get().acknowledge();
			await().atMost(Duration.ofSeconds(5))
					.untilAsserted(() -> assertThat(commits).contains(Map.of(TP0, new OffsetAndMetadata(1L))));
		}
		finally {
			container.stop();
		}
	}

}
