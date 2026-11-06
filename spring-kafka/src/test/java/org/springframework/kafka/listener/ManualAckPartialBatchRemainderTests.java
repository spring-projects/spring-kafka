/*
 * Copyright 2026-present the original author or authors.
 */

package org.springframework.kafka.listener;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.common.record.TimestampType;
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
 * Tests for {@link Acknowledgment#acknowledge()} and {@link Acknowledgment#nack(int, Duration)}
 * after a batch acknowledgment.
 *
 * @author Gangeun Lee
 * @author Minju Kim
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
				Map.of(TP0, new OffsetAndMetadata(2L, Optional.of(8), "")),
				Map.of(TP1, new OffsetAndMetadata(2L, Optional.of(10), ""),
						TP2, new OffsetAndMetadata(2L, Optional.of(12), "")));
	}

	@Test
	void acknowledgeAfterPartialOfLastRecordIsNoOp() throws Exception {
		List<Map<TopicPartition, OffsetAndMetadata>> commits = runBatch(ack -> {
			ack.acknowledge(5);
			ack.acknowledge();
		});
		assertThat(commits).containsExactly(
				Map.of(TP0, new OffsetAndMetadata(2L, Optional.of(8), ""),
						TP1, new OffsetAndMetadata(2L, Optional.of(10), ""),
						TP2, new OffsetAndMetadata(2L, Optional.of(12), "")));
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
				Map.of(TP0, new OffsetAndMetadata(2L, Optional.of(8), "")),
				Map.of(TP1, new OffsetAndMetadata(2L, Optional.of(10), ""),
						TP2, new OffsetAndMetadata(2L, Optional.of(12), "")));
	}

	@Test
	void nackAfterPartialCommitsOnlyRecordsNotYetAcknowledged() throws Exception {
		List<Map<TopicPartition, OffsetAndMetadata>> commits = runBatch(ack -> {
			ack.acknowledge(1);
			ack.nack(3, Duration.ZERO);
		});
		assertThat(commits).containsExactly(
				Map.of(TP0, new OffsetAndMetadata(2L, Optional.of(8), "")),
				Map.of(TP1, new OffsetAndMetadata(1L, Optional.of(9), "")));
	}

	@Test
	void nackAtOrBeforePartialIsRejected() throws Exception {
		AtomicReference<Exception> nackFailure = new AtomicReference<>();
		List<Map<TopicPartition, OffsetAndMetadata>> commits = runBatch(ack -> {
			ack.acknowledge(3);
			try {
				ack.nack(1, Duration.ZERO);
			}
			catch (IllegalArgumentException ex) {
				nackFailure.set(ex);
			}
		});
		assertThat(nackFailure.get()).hasMessageContaining("previous partial commit");
		assertThat(commits).containsExactly(
				Map.of(TP0, new OffsetAndMetadata(2L, Optional.of(8), ""),
						TP1, new OffsetAndMetadata(2L, Optional.of(10), "")));
	}

	@Test
	void nackAtPartialIsRejected() throws Exception {
		AtomicReference<Exception> nackFailure = new AtomicReference<>();
		List<Map<TopicPartition, OffsetAndMetadata>> commits = runBatch(ack -> {
			ack.acknowledge(3);
			try {
				ack.nack(3, Duration.ZERO);
			}
			catch (IllegalArgumentException ex) {
				nackFailure.set(ex);
			}
		});
		assertThat(nackFailure.get()).hasMessageContaining("previous partial commit");
		assertThat(commits).containsExactly(
				Map.of(TP0, new OffsetAndMetadata(2L, Optional.of(8), ""),
						TP1, new OffsetAndMetadata(2L, Optional.of(10), "")));
	}

	@Test
	void nackRightAfterPartialCommitsNothingMore() throws Exception {
		List<Map<TopicPartition, OffsetAndMetadata>> commits = runBatch(ack -> {
			ack.acknowledge(1);
			ack.nack(2, Duration.ZERO);
		});
		assertThat(commits).containsExactly(Map.of(TP0, new OffsetAndMetadata(2L, Optional.of(8), "")));
	}

	@Test
	void nackAfterAcknowledgeIsRejected() throws Exception {
		AtomicReference<Exception> nackFailure = new AtomicReference<>();
		List<Map<TopicPartition, OffsetAndMetadata>> commits = runBatch(ack -> {
			ack.acknowledge();
			try {
				ack.nack(1, Duration.ZERO);
			}
			catch (IllegalStateException ex) {
				nackFailure.set(ex);
			}
		});
		assertThat(nackFailure.get()).hasMessageContaining("already been acknowledged");
		assertThat(commits).containsExactly(
				Map.of(TP0, new OffsetAndMetadata(2L, Optional.of(8), ""),
						TP1, new OffsetAndMetadata(2L, Optional.of(10), ""),
						TP2, new OffsetAndMetadata(2L, Optional.of(12), "")));
	}

	private List<Map<TopicPartition, OffsetAndMetadata>> runBatch(java.util.function.Consumer<Acknowledgment> acks)
			throws Exception {

		ConsumerFactory<Integer, String> cf = mock();
		Consumer<Integer, String> consumer = mock();
		given(cf.createConsumer(any(), any(), any(), any())).willReturn(consumer);
		Map<TopicPartition, List<ConsumerRecord<Integer, String>>> records = new LinkedHashMap<>();
		records.put(TP0, List.of(
				new ConsumerRecord<>("foo", 0, 0L, 0L, TimestampType.NO_TIMESTAMP_TYPE,
						0, 0, 0, "a", new RecordHeaders(), Optional.of(7)),
				new ConsumerRecord<>("foo", 0, 1L, 0L, TimestampType.NO_TIMESTAMP_TYPE,
						0, 0, 0, "b", new RecordHeaders(), Optional.of(8))));
		records.put(TP1, List.of(
				new ConsumerRecord<>("foo", 1, 0L, 0L, TimestampType.NO_TIMESTAMP_TYPE,
						0, 0, 0, "c", new RecordHeaders(), Optional.of(9)),
				new ConsumerRecord<>("foo", 1, 1L, 0L, TimestampType.NO_TIMESTAMP_TYPE,
						0, 0, 0, "d", new RecordHeaders(), Optional.of(10))));
		records.put(TP2, List.of(
				new ConsumerRecord<>("foo", 2, 0L, 0L, TimestampType.NO_TIMESTAMP_TYPE,
						0, 0, 0, "e", new RecordHeaders(), Optional.of(11)),
				new ConsumerRecord<>("foo", 2, 1L, 0L, TimestampType.NO_TIMESTAMP_TYPE,
						0, 0, 0, "f", new RecordHeaders(), Optional.of(12))));
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
