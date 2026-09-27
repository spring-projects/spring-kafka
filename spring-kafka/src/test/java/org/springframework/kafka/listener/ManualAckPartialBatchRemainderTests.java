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

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyMap;
import static org.mockito.BDDMockito.given;
import static org.mockito.BDDMockito.willAnswer;
import static org.mockito.Mockito.mock;

/**
 * Tests for {@link Acknowledgment#acknowledge()} after a partial batch acknowledgment.
 *
 * @author Gangeun Lee
 *
 * @since 4.2
 *
 */
public class ManualAckPartialBatchRemainderTests {

	private static final TopicPartition TP0 = new TopicPartition("foo", 0);

	private static final TopicPartition TP1 = new TopicPartition("foo", 1);

	private static final TopicPartition TP2 = new TopicPartition("foo", 2);

	@Test
	void acknowledgeAfterPartialCommitsRemainderOfBatch() throws Exception {
		List<Map<TopicPartition, OffsetAndMetadata>> commits = runBatch(ack -> {
			ack.acknowledge(1);
			ack.acknowledge();
		});
		assertThat(commits).containsExactly(
				Map.of(TP0, new OffsetAndMetadata(2L)),
				Map.of(TP1, new OffsetAndMetadata(2L), TP2, new OffsetAndMetadata(2L)));
	}

	@Test
	void acknowledgeAfterPartialOfLastRecordIsNoOp() throws Exception {
		List<Map<TopicPartition, OffsetAndMetadata>> commits = runBatch(ack -> {
			ack.acknowledge(5);
			ack.acknowledge();
		});
		assertThat(commits).containsExactly(
				Map.of(TP0, new OffsetAndMetadata(2L), TP1, new OffsetAndMetadata(2L),
						TP2, new OffsetAndMetadata(2L)));
	}

	@Test
	void repeatedAcknowledgeAfterPartialIsNoOp() throws Exception {
		List<Map<TopicPartition, OffsetAndMetadata>> commits = runBatch(ack -> {
			ack.acknowledge(1);
			for (int i = 0; i < 5; i++) {
				ack.acknowledge();
			}
		});
		assertThat(commits).containsExactly(
				Map.of(TP0, new OffsetAndMetadata(2L)),
				Map.of(TP1, new OffsetAndMetadata(2L), TP2, new OffsetAndMetadata(2L)));
	}

	private List<Map<TopicPartition, OffsetAndMetadata>> runBatch(java.util.function.Consumer<Acknowledgment> acks)
			throws Exception {

		ConsumerFactory<Integer, String> cf = mock();
		Consumer<Integer, String> consumer = mock();
		given(cf.createConsumer(any(), any(), any(), any())).willReturn(consumer);
		Map<TopicPartition, List<ConsumerRecord<Integer, String>>> records = new LinkedHashMap<>();
		records.put(TP0, List.of(new ConsumerRecord<>("foo", 0, 0L, 0, "a"), new ConsumerRecord<>("foo", 0, 1L, 0, "b")));
		records.put(TP1, List.of(new ConsumerRecord<>("foo", 1, 0L, 0, "c"), new ConsumerRecord<>("foo", 1, 1L, 0, "d")));
		records.put(TP2, List.of(new ConsumerRecord<>("foo", 2, 0L, 0, "e"), new ConsumerRecord<>("foo", 2, 1L, 0, "f")));
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
		willAnswer(i -> {
			commits.add(new LinkedHashMap<>(i.getArgument(0)));
			return null;
		}).given(consumer).commitSync(anyMap(), any());

		ContainerProperties containerProps = new ContainerProperties(new TopicPartitionOffset("foo", 0),
				new TopicPartitionOffset("foo", 1), new TopicPartitionOffset("foo", 2));
		containerProps.setGroupId("grp");
		containerProps.setAckMode(AckMode.MANUAL_IMMEDIATE);
		containerProps.setAssignmentCommitOption(AssignmentCommitOption.NEVER);
		CountDownLatch latch = new CountDownLatch(1);
		AtomicReference<Exception> failure = new AtomicReference<>();
		containerProps.setMessageListener((BatchAcknowledgingMessageListener<Integer, String>) (data, ack) -> {
			try {
				acks.accept(ack);
			}
			catch (Exception ex) {
				failure.set(ex);
			}
			finally {
				latch.countDown();
			}
		});
		KafkaMessageListenerContainer<Integer, String> container =
				new KafkaMessageListenerContainer<>(cf, containerProps);
		container.start();
		try {
			assertThat(latch.await(10, TimeUnit.SECONDS)).isTrue();
		}
		finally {
			container.stop();
		}
		assertThat(failure.get()).isNull();
		return commits;
	}

}
