/*
 * Copyright and licensing information for this source code can be found in
 * the NOTICE and LICENSE files distributed with this project.
 */

package org.springframework.kafka.listener;

import java.time.Duration;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.IntFunction;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.Test;

import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.listener.ContainerProperties.AckMode;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.BDDMockito.given;
import static org.mockito.BDDMockito.willAnswer;
import static org.mockito.Mockito.mock;

/**
 * Tests for the partitions paused by a batch {@code nack(index, sleep)} when another
 * nack or a cooperative assignment happens during the sleep.
 *
 * @author Jiwon Lee
 * @author Soby Chacko
 */
class NackRebalanceOwnershipTests {

	private static final Duration NACK_SLEEP = Duration.ofMillis(500);

	private final TopicPartition original = new TopicPartition("nack-original", 0);

	private final TopicPartition added = new TopicPartition("nack-added", 0);

	private final Set<TopicPartition> assigned = ConcurrentHashMap.newKeySet();

	private final Set<TopicPartition> paused = ConcurrentHashMap.newKeySet();

	private final AtomicReference<ConsumerRebalanceListener> rebalanceListener = new AtomicReference<>();

	private final AtomicInteger pollCount = new AtomicInteger();

	@Test
	void secondNackDuringSleepDoesNotStrandNackPausedPartition() throws Exception {
		CountDownLatch resumed = new CountDownLatch(1);
		KafkaMessageListenerContainer<Integer, String> container = container(poll -> {
			if (poll == 0) {
				assign(this.original);
				return records(this.original, "first-failure");
			}
			if (poll == 1) {
				// a batch fetched before the pause took effect, nacked again during the sleep
				return records(this.original, "second-failure");
			}
			if (!this.paused.contains(this.original)) {
				resumed.countDown();
			}
			return ConsumerRecords.empty();
		});
		try {
			container.start();
			assertThat(resumed.await(10, TimeUnit.SECONDS))
					.as("The partition paused by both nacks must resume after the sleep")
					.isTrue();
		}
		finally {
			container.stop();
		}
	}

	@Test
	void partitionAssignedDuringNackSleepIsPausedUntilSleepEnds() throws Exception {
		AtomicReference<Set<TopicPartition>> pausedAfterAssignment = new AtomicReference<>();
		CountDownLatch resumed = new CountDownLatch(1);
		KafkaMessageListenerContainer<Integer, String> container = container(poll -> {
			if (poll == 0) {
				assign(this.original);
				return records(this.original, "first-failure");
			}
			if (poll == 1) {
				assign(this.added);
				pausedAfterAssignment.set(Set.copyOf(this.paused));
			}
			else if (this.paused.isEmpty()) {
				resumed.countDown();
			}
			return ConsumerRecords.empty();
		});
		try {
			container.start();
			assertThat(resumed.await(10, TimeUnit.SECONDS))
					.as("Both partitions must resume after the sleep")
					.isTrue();
			assertThat(pausedAfterAssignment.get())
					.as("A partition assigned during the nack sleep must be paused with the others")
					.containsExactlyInAnyOrder(this.original, this.added);
		}
		finally {
			container.stop();
		}
	}

	@SuppressWarnings("unchecked")
	private KafkaMessageListenerContainer<Integer, String> container(
			IntFunction<ConsumerRecords<Integer, String>> polls) {

		ConsumerFactory<Integer, String> consumerFactory = mock();
		Consumer<Integer, String> consumer = mock();
		given(consumerFactory.createConsumer(eq("nack-group"), eq("nack-client"), isNull(), any()))
				.willReturn(consumer);
		given(consumerFactory.getConfigurationProperties()).willReturn(new HashMap<>());
		given(consumer.assignment()).willReturn(this.assigned);
		given(consumer.paused()).willAnswer(inv -> Set.copyOf(this.paused));
		willAnswer(inv -> {
			this.paused.addAll(inv.getArgument(0));
			return null;
		}).given(consumer).pause(any(Collection.class));
		willAnswer(inv -> {
			this.paused.removeAll(inv.getArgument(0));
			return null;
		}).given(consumer).resume(any(Collection.class));
		willAnswer(inv -> {
			this.rebalanceListener.set(inv.getArgument(1));
			return null;
		}).given(consumer).subscribe(any(Collection.class), any(ConsumerRebalanceListener.class));
		given(consumer.poll(any(Duration.class))).willAnswer(inv -> {
			Thread.sleep(20);
			return polls.apply(this.pollCount.getAndIncrement());
		});

		ContainerProperties properties = new ContainerProperties(this.original.topic(), this.added.topic());
		properties.setGroupId("nack-group");
		properties.setClientId("nack-client");
		properties.setAckMode(AckMode.MANUAL_IMMEDIATE);
		properties.setPollTimeout(20);
		properties.setMessageListener((BatchAcknowledgingMessageListener<Integer, String>) (batch, ack) -> {
			if (batch.get(0).value().endsWith("failure")) {
				ack.nack(0, NACK_SLEEP);
			}
			else {
				ack.acknowledge();
			}
		});
		return new KafkaMessageListenerContainer<>(consumerFactory, properties);
	}

	private void assign(TopicPartition partition) {
		this.assigned.add(partition);
		this.rebalanceListener.get().onPartitionsAssigned(List.of(partition));
	}

	private static ConsumerRecords<Integer, String> records(TopicPartition partition, String value) {
		return new ConsumerRecords<>(
				Map.of(partition, List.of(new ConsumerRecord<>(
						partition.topic(), partition.partition(), 0L, 1, value))),
				Map.of());
	}

}
