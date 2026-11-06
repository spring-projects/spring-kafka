/*
 * Copyright 2019-present the original author or authors.
 */

package org.springframework.kafka.listener;

import java.lang.reflect.Constructor;
import java.lang.reflect.Method;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Deque;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

import io.micrometer.observation.Observation;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.consumer.OffsetAndTimestamp;
import org.apache.kafka.clients.consumer.OffsetCommitCallback;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.GroupAuthorizationException;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.common.record.TimestampType;
import org.jspecify.annotations.Nullable;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.ArgumentCaptor;

import org.springframework.kafka.annotation.KafkaListenerAnnotationBeanPostProcessor;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.KafkaResourceHolder;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.event.ConsumerFailedToStartEvent;
import org.springframework.kafka.event.ConsumerStartedEvent;
import org.springframework.kafka.event.ConsumerStartingEvent;
import org.springframework.kafka.event.ListenerContainerIdleEvent;
import org.springframework.kafka.listener.ContainerProperties.AckMode;
import org.springframework.kafka.listener.ContainerProperties.AssignmentCommitOption;
import org.springframework.kafka.listener.adapter.HandlerAdapter;
import org.springframework.kafka.listener.adapter.RecordMessagingMessageListenerAdapter;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.kafka.support.TopicPartitionOffset;
import org.springframework.kafka.test.utils.KafkaTestUtils;
import org.springframework.kafka.transaction.KafkaAwareTransactionManager;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;
import org.springframework.transaction.support.TransactionSynchronizationManager;
import org.springframework.util.CollectionUtils;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyMap;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.ArgumentMatchers.same;
import static org.mockito.BDDMockito.given;
import static org.mockito.BDDMockito.willAnswer;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.verifyNoMoreInteractions;

/**
 * @author Gary Russell
 * @author Wang Zhiyang
 * @author Soby Chacko
 * @author Minchul Son
 * @author Nikita Kibitkin
 * @author Hyun Lee
 * @author Jian Kim
 * @author Jiyeon Kim
 * @author Minju Kim
 *
 * @since 2.2.4
 *
 */
public class ConcurrentMessageListenerContainerMockTests {

	private final CompletableFuture<Void> pendingResult = new CompletableFuture<>();

	@ParameterizedTest
	@ValueSource(strings = { "lower", "equalUnknown", "equalKnown", "higherUnknown",
			"partitions", "unacked", "transformedLower" })
	@SuppressWarnings("unchecked")
	void pendingCommitsKeepEligibleSourcePairs(String scenario) throws InterruptedException {
		ConsumerFactory<Integer, String> factory = mock();
		Consumer<Integer, String> consumer = mock();
		given(factory.createConsumer(any(), any(), any(), any())).willReturn(consumer);
		TopicPartition first = new TopicPartition("topic", 0);
		TopicPartition second = new TopicPartition("topic", 1);
		List<ConsumerRecord<Integer, String>> records = switch (scenario) {
			case "lower", "transformedLower" ->
					List.of(epochRecord(first, 43, Optional.of(8)), epochRecord(first, 41, Optional.of(7)));
			case "equalUnknown" -> List.of(epochRecord(first, 43, Optional.of(8)), epochRecord(first, 43, Optional.empty()));
			case "equalKnown" -> List.of(epochRecord(first, 43, Optional.empty()), epochRecord(first, 43, Optional.of(0)));
			case "higherUnknown" -> List.of(epochRecord(first, 41, Optional.of(7)), epochRecord(first, 43, Optional.empty()));
			case "partitions" -> List.of(epochRecord(first, 41, Optional.of(7)), epochRecord(second, 99, Optional.of(2)));
			default -> List.of(epochRecord(first, 41, Optional.of(7)), epochRecord(first, 99, Optional.of(9)));
		};
		AtomicInteger poll = new AtomicInteger();
		given(consumer.poll(any(Duration.class))).willAnswer(invocation -> {
			int index = poll.getAndIncrement();
			if (index < records.size()) {
				ConsumerRecord<Integer, String> record = records.get(index);
				return new ConsumerRecords<>(
						Map.of(new TopicPartition("topic", record.partition()), List.of(record)), Map.of());
			}
			Thread.sleep(10);
			return ConsumerRecords.empty();
		});
		OffsetAndMetadataProvider provider = mock();
		OffsetAndMetadataProvider replacement = mock();
		given(provider.provide(any(), anyLong())).willAnswer(invocation ->
				new OffsetAndMetadata(scenario.equals("transformedLower") ? 1000 - invocation.getArgument(1, Long.class)
						: invocation.getArgument(1, Long.class), "pending"));
		CountDownLatch committedLatch = new CountDownLatch(1);
		AtomicReference<Map<TopicPartition, OffsetAndMetadata>> committed = new AtomicReference<>();
		willAnswer(invocation -> {
			committed.set(Map.copyOf(invocation.getArgument(0)));
			committedLatch.countDown();
			return null;
		}).given(consumer).commitSync(anyMap(), any(Duration.class));
		ContainerProperties properties = new ContainerProperties(new TopicPartitionOffset("topic", 0),
				new TopicPartitionOffset("topic", 1));
		properties.setGroupId("grp");
		properties.setAckMode(AckMode.MANUAL);
		properties.setFixTxOffsets(false);
		properties.setOffsetAndMetadataProvider(provider);
		AtomicReference<ContainerProperties> liveProperties = new AtomicReference<>();
		List<Acknowledgment> acknowledgments = new ArrayList<>();
		properties.setMessageListener((AcknowledgingMessageListener<Integer, String>) (record, acknowledgment) -> {
			acknowledgments.add(acknowledgment);
			if (scenario.equals("partitions") && acknowledgments.size() == 1) {
				liveProperties.get().setOffsetAndMetadataProvider(replacement);
			}
			if (acknowledgments.size() == records.size()) {
				acknowledgments.get(0).acknowledge();
				if (!scenario.equals("unacked")) {
					acknowledgments.get(1).acknowledge();
				}
				verifyNoInteractions(provider);
			}
		});
		KafkaMessageListenerContainer<Integer, String> container =
				new KafkaMessageListenerContainer<>(factory, properties);
		liveProperties.set(container.getContainerProperties());
		try {
			container.start();
			assertThat(committedLatch.await(10, TimeUnit.SECONDS)).isTrue();
		}
		finally {
			container.stop();
		}
		Map<TopicPartition, OffsetAndMetadata> expected = switch (scenario) {
			case "transformedLower" -> Map.of(first, new OffsetAndMetadata(956, Optional.empty(), "pending"));
			case "lower" -> Map.of(first, new OffsetAndMetadata(44, Optional.of(8), "pending"));
			case "equalUnknown", "higherUnknown" ->
					Map.of(first, new OffsetAndMetadata(44, Optional.empty(), "pending"));
			case "equalKnown" -> Map.of(first, new OffsetAndMetadata(44, Optional.of(0), "pending"));
			case "partitions" -> Map.of(first, new OffsetAndMetadata(42, Optional.of(7), "pending"),
					second, new OffsetAndMetadata(100, Optional.of(2), "pending"));
			default -> Map.of(first, new OffsetAndMetadata(42, Optional.of(7), "pending"));
		};
		assertThat(committed.get()).isEqualTo(expected);
		ArgumentCaptor<ListenerMetadata> metadata = ArgumentCaptor.forClass(ListenerMetadata.class);
		for (OffsetAndMetadata offset : expected.values()) {
			verify(provider).provide(metadata.capture(),
					eq(scenario.equals("transformedLower") ? 44L : offset.offset()));
		}
		assertThat(metadata.getAllValues()).allSatisfy(value -> {
			assertThat(value).isSameAs(metadata.getValue());
			assertThat(value.getGroupId()).isEqualTo("grp");
		});
		verifyNoMoreInteractions(provider);
		verifyNoInteractions(replacement);
		verify(consumer, times(1)).commitSync(anyMap(), any(Duration.class));
	}

	@ParameterizedTest
	@ValueSource(booleans = { false, true })
	@SuppressWarnings("unchecked")
	void asyncBatchCommitCompletesWithTheSamePair(boolean failure) throws InterruptedException {
		Consumer<Integer, String> consumer = mock();
		ConsumerFactory<Integer, String> factory = consumerFactory(consumer);
		TopicPartition partition = new TopicPartition("topic", 0);
		ConsumerRecord<Integer, String> record = epochRecord(partition, 41L, Optional.of(7));
		ConsumerRecords<Integer, String> records = new ConsumerRecords<>(Map.of(partition, List.of(
				record,
				epochRecord(partition, 42L, "second", Optional.of(8)),
				epochRecord(partition, 43L, "third", Optional.of(9)))), Map.of());
		pollOnce(consumer, records);
		OffsetCommitCallback callback = mock();
		Exception error = failure ? new IllegalStateException("commit failed") : null;
		CountDownLatch committedLatch = new CountDownLatch(1);
		AtomicReference<Map<TopicPartition, OffsetAndMetadata>> committed = new AtomicReference<>();
		willAnswer(invocation -> {
			Map<TopicPartition, OffsetAndMetadata> offsets = invocation.getArgument(0);
			committed.set(offsets);
			OffsetCommitCallback completion = invocation.getArgument(1);
			completion.onComplete(offsets, error);
			committedLatch.countDown();
			return null;
		}).given(consumer).commitAsync(anyMap(), any());
		ContainerProperties properties = new ContainerProperties(new TopicPartitionOffset("topic", 0));
		properties.setGroupId("grp");
		properties.setClientId("clientId");
		properties.setAckMode(AckMode.BATCH);
		properties.setSyncCommits(false);
		properties.setCommitCallback(callback);
		properties.setFixTxOffsets(false);
		properties.setMessageListener((MessageListener<Integer, String>) data -> { });
		KafkaMessageListenerContainer<Integer, String> container =
				new KafkaMessageListenerContainer<>(factory, properties);
		try {
			container.start();
			assertThat(committedLatch.await(10, TimeUnit.SECONDS)).isTrue();
			verify(callback).onComplete(same(committed.get()),
					same(error));
			verifyNoMoreInteractions(callback);
			assertThat(committed.get()).containsEntry(partition, new OffsetAndMetadata(44L, Optional.of(9), ""));
		}
		finally {
			container.stop();
		}
	}

	@Test
	@SuppressWarnings("unchecked")
	void revokeFlushesPendingEpochBetweenCallbacks() throws InterruptedException {
		ConsumerFactory<Integer, String> factory = mock();
		Consumer<Integer, String> consumer = mock();
		given(factory.createConsumer(any(), any(), any(), any())).willReturn(consumer);
		TopicPartition partition = new TopicPartition("topic", 0);
		AtomicReference<ConsumerRebalanceListener> rebalance = new AtomicReference<>();
		willAnswer(inv -> {
			rebalance.set(inv.getArgument(1));
			return null;
		}).given(consumer).subscribe(any(Collection.class), any());
		List<String> events = new ArrayList<>();
		List<Map<TopicPartition, OffsetAndMetadata>> commits = new ArrayList<>();
		Map<TopicPartition, OffsetAndMetadata> expected =
				Map.of(partition, new OffsetAndMetadata(44, Optional.of(8), ""));
		willAnswer(inv -> {
			events.add("commit");
			commits.add(Map.copyOf(inv.getArgument(0)));
			return null;
		}).given(consumer).commitSync(anyMap(), any(Duration.class));
		AtomicInteger poll = new AtomicInteger();
		CountDownLatch revoked = new CountDownLatch(1);
		given(consumer.poll(any(Duration.class))).willAnswer(inv -> {
			int phase = poll.getAndIncrement();
			if (phase == 0) {
				rebalance.get().onPartitionsAssigned(List.of(partition));
				return new ConsumerRecords<>(Map.of(partition, List.of(epochRecord(partition, 43, Optional.of(8)))), Map.of());
			}
			if (phase == 1) {
				rebalance.get().onPartitionsRevoked(List.of(partition));
				revoked.countDown();
			}
			Thread.sleep(10);
			return ConsumerRecords.empty();
		});
		ContainerProperties properties = new ContainerProperties("topic");
		properties.setGroupId("grp");
		properties.setAckMode(AckMode.COUNT);
		properties.setAckCount(100);
		properties.setAssignmentCommitOption(ContainerProperties.AssignmentCommitOption.NEVER);
		properties.setMessageListener((MessageListener<Integer, String>) record -> { });
		properties.setConsumerRebalanceListener(new ConsumerAwareRebalanceListener() {
			@Override
			public void onPartitionsRevokedBeforeCommit(Consumer<?, ?> source, Collection<TopicPartition> partitions) {
				assertThat(commits).isEmpty();
				events.add("before");
			}

			@Override
			public void onPartitionsRevokedAfterCommit(Consumer<?, ?> source, Collection<TopicPartition> partitions) {
				assertThat(commits).containsExactly(expected);
				events.add("after");
			}
		});
		KafkaMessageListenerContainer<Integer, String> container =
				new KafkaMessageListenerContainer<>(factory, properties);
		try {
			container.start();
			assertThat(revoked.await(10, TimeUnit.SECONDS)).isTrue();
		}
		finally {
			container.stop();
		}
		assertThat(events).containsExactly("before", "commit", "after");
		assertThat(commits).containsExactly(expected);
		verify(consumer, times(1)).commitSync(anyMap(), any(Duration.class));
	}

	private static ConsumerRecord<Integer, String> epochRecord(TopicPartition partition, long offset,
			Optional<Integer> epoch) {

		return epochRecord(partition, offset, "value", epoch);
	}

	private static ConsumerRecord<Integer, String> epochRecord(TopicPartition partition, long offset, String value,
			Optional<Integer> epoch) {

		return new ConsumerRecord<>(partition.topic(), partition.partition(), offset, 0L,
				TimestampType.NO_TIMESTAMP_TYPE, 0, 0, 1, value, new RecordHeaders(), epoch);
	}

	@SuppressWarnings("unchecked")
	private static ConsumerFactory<Integer, String> consumerFactory(Consumer<Integer, String> consumer) {
		ConsumerFactory<Integer, String> factory = mock();
		given(factory.getConfigurationProperties()).willReturn(Map.of(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false));
		given(factory.createConsumer(eq("grp"), eq("clientId"), isNull(), any())).willReturn(consumer);
		return factory;
	}

	private static void pollOnce(Consumer<Integer, String> consumer, ConsumerRecords<Integer, String> records) {
		AtomicBoolean firstPoll = new AtomicBoolean(true);
		given(consumer.poll(any(Duration.class))).willAnswer(invocation -> {
			if (firstPoll.getAndSet(false)) {
				return records;
			}
			Thread.sleep(10);
			return ConsumerRecords.empty();
		});
	}

	@Test
	@SuppressWarnings("unchecked")
	void recordCommitPreservesLeaderEpoch() throws InterruptedException {
		Consumer<Integer, String> consumer = mock();
		ConsumerFactory<Integer, String> factory = consumerFactory(consumer);
		TopicPartition partition = new TopicPartition("topic", 0);
		ConsumerRecord<Integer, String> record = epochRecord(partition, 41L, Optional.of(7));
		ConsumerRecords<Integer, String> records = new ConsumerRecords<>(Map.of(partition, List.of(record)), Map.of());
		pollOnce(consumer, records);
		CountDownLatch committedLatch = new CountDownLatch(1);
		AtomicReference<Map<TopicPartition, OffsetAndMetadata>> committed = new AtomicReference<>();
		willAnswer(invocation -> {
			Map<TopicPartition, OffsetAndMetadata> offsets = invocation.getArgument(0);
			committed.set(Map.copyOf(offsets));
			committedLatch.countDown();
			return null;
		}).given(consumer).commitSync(anyMap(), any(Duration.class));
		ContainerProperties properties = new ContainerProperties(new TopicPartitionOffset("topic", 0));
		properties.setGroupId("grp");
		properties.setClientId("clientId");
		properties.setAckMode(AckMode.RECORD);
		properties.setSyncCommits(true);
		properties.setFixTxOffsets(false);
		properties.setMessageListener((MessageListener<Integer, String>) data -> { });
		KafkaMessageListenerContainer<Integer, String> container =
				new KafkaMessageListenerContainer<>(factory, properties);
		try {
			container.start();
			assertThat(committedLatch.await(10, TimeUnit.SECONDS)).isTrue();
			assertThat(committed.get()).containsEntry(partition, new OffsetAndMetadata(42L, Optional.of(7), ""));
		}
		finally {
			container.stop();
		}
	}

	@Test
	@SuppressWarnings("unchecked")
	void batchCommitPreservesLastAcknowledgedEpoch() throws InterruptedException {
		Consumer<Integer, String> consumer = mock();
		ConsumerFactory<Integer, String> factory = consumerFactory(consumer);
		TopicPartition partition = new TopicPartition("topic", 0);
		ConsumerRecord<Integer, String> record = epochRecord(partition, 41L, Optional.of(7));
		ConsumerRecords<Integer, String> records = new ConsumerRecords<>(Map.of(partition, List.of(
				record,
				epochRecord(partition, 42L, "second", Optional.of(8)),
				epochRecord(partition, 43L, "third", Optional.of(9)))), Map.of());
		pollOnce(consumer, records);
		CountDownLatch committedLatch = new CountDownLatch(1);
		AtomicReference<Map<TopicPartition, OffsetAndMetadata>> committed = new AtomicReference<>();
		willAnswer(invocation -> {
			Map<TopicPartition, OffsetAndMetadata> offsets = invocation.getArgument(0);
			committed.set(Map.copyOf(offsets));
			committedLatch.countDown();
			return null;
		}).given(consumer).commitSync(anyMap(), any(Duration.class));
		ContainerProperties properties = new ContainerProperties(new TopicPartitionOffset("topic", 0));
		properties.setGroupId("grp");
		properties.setClientId("clientId");
		properties.setAckMode(AckMode.BATCH);
		properties.setSyncCommits(true);
		properties.setFixTxOffsets(false);
		properties.setMessageListener((MessageListener<Integer, String>) data -> { });
		KafkaMessageListenerContainer<Integer, String> container =
				new KafkaMessageListenerContainer<>(factory, properties);
		try {
			container.start();
			assertThat(committedLatch.await(10, TimeUnit.SECONDS)).isTrue();
			assertThat(committed.get()).containsEntry(partition, new OffsetAndMetadata(44L, Optional.of(9), ""));
		}
		finally {
			container.stop();
		}
	}

	@Test
	@SuppressWarnings("unchecked")
	void immediateBatchCommitPreservesLastIncludedEpoch() throws InterruptedException {
		Consumer<Integer, String> consumer = mock();
		ConsumerFactory<Integer, String> factory = consumerFactory(consumer);
		TopicPartition partition = new TopicPartition("topic", 0);
		ConsumerRecord<Integer, String> record = epochRecord(partition, 41L, Optional.of(7));
		ConsumerRecords<Integer, String> records = new ConsumerRecords<>(Map.of(partition, List.of(
				record,
				epochRecord(partition, 42L, "second", Optional.of(8)),
				epochRecord(partition, 43L, "third", Optional.of(9)))), Map.of());
		pollOnce(consumer, records);
		CountDownLatch committedLatch = new CountDownLatch(1);
		AtomicReference<Map<TopicPartition, OffsetAndMetadata>> committed = new AtomicReference<>();
		willAnswer(invocation -> {
			Map<TopicPartition, OffsetAndMetadata> offsets = invocation.getArgument(0);
			committed.set(Map.copyOf(offsets));
			committedLatch.countDown();
			return null;
		}).given(consumer).commitSync(anyMap(), any(Duration.class));
		ContainerProperties properties = new ContainerProperties(new TopicPartitionOffset("topic", 0));
		properties.setGroupId("grp");
		properties.setClientId("clientId");
		properties.setAckMode(AckMode.MANUAL_IMMEDIATE);
		properties.setSyncCommits(true);
		properties.setFixTxOffsets(false);
		properties.setMessageListener((BatchAcknowledgingMessageListener<Integer, String>)
				(data, acknowledgment) -> acknowledgment.acknowledge());
		KafkaMessageListenerContainer<Integer, String> container =
				new KafkaMessageListenerContainer<>(factory, properties);
		try {
			container.start();
			assertThat(committedLatch.await(10, TimeUnit.SECONDS)).isTrue();
			assertThat(committed.get()).containsEntry(partition, new OffsetAndMetadata(44L, Optional.of(9), ""));
		}
		finally {
			container.stop();
		}
	}

	@Test
	@SuppressWarnings("unchecked")
	void recordCommitsUseCachedProviderAfterListenerReturns() throws InterruptedException {
		Consumer<Integer, String> consumer = mock();
		ConsumerFactory<Integer, String> factory = consumerFactory(consumer);
		TopicPartition partition = new TopicPartition("topic", 0);
		ConsumerRecord<Integer, String> first = epochRecord(partition, 41L, "first", Optional.of(7));
		ConsumerRecord<Integer, String> second = epochRecord(partition, 42L, "second", Optional.of(8));
		ConsumerRecords<Integer, String> records = new ConsumerRecords<>(
				Map.of(partition, List.of(first, second)), Map.of());
		pollOnce(consumer, records);
		OffsetAndMetadataProvider provider = mock();
		OffsetAndMetadataProvider replacement = mock();
		given(provider.provide(any(), anyLong())).willAnswer(invocation ->
				new OffsetAndMetadata(invocation.getArgument(1, Long.class), "cached"));
		CountDownLatch listenerEntered = new CountDownLatch(1);
		CountDownLatch releaseListener = new CountDownLatch(1);
		CountDownLatch committedLatch = new CountDownLatch(2);
		List<Map<TopicPartition, OffsetAndMetadata>> committed = new ArrayList<>();
		willAnswer(invocation -> {
			Map<TopicPartition, OffsetAndMetadata> offsets = invocation.getArgument(0);
			committed.add(Map.copyOf(offsets));
			committedLatch.countDown();
			return null;
		}).given(consumer).commitSync(anyMap(), any(Duration.class));
		ContainerProperties properties = new ContainerProperties(new TopicPartitionOffset("topic", 0));
		properties.setGroupId("grp");
		properties.setClientId("clientId");
		properties.setAckMode(AckMode.RECORD);
		properties.setSyncCommits(true);
		properties.setFixTxOffsets(false);
		properties.setOffsetAndMetadataProvider(provider);
		properties.setMessageListener((MessageListener<Integer, String>) data -> {
			listenerEntered.countDown();
			try {
				if (!releaseListener.await(10, TimeUnit.SECONDS)) {
					throw new IllegalStateException("Listener was not released");
				}
			}
			catch (InterruptedException ex) {
				Thread.currentThread().interrupt();
				throw new IllegalStateException(ex);
			}
		});
		KafkaMessageListenerContainer<Integer, String> container =
				new KafkaMessageListenerContainer<>(factory, properties);
		try {
			container.start();
			assertThat(listenerEntered.await(10, TimeUnit.SECONDS)).isTrue();
			verifyNoInteractions(provider, replacement);
			container.getContainerProperties().setOffsetAndMetadataProvider(replacement);
			releaseListener.countDown();
			assertThat(committedLatch.await(10, TimeUnit.SECONDS)).isTrue();
			assertThat(committed).containsExactly(
					Map.of(partition, new OffsetAndMetadata(42L, Optional.of(7), "cached")),
					Map.of(partition, new OffsetAndMetadata(43L, Optional.of(8), "cached")));
			ArgumentCaptor<ListenerMetadata> metadata = ArgumentCaptor.forClass(ListenerMetadata.class);
			verify(provider).provide(metadata.capture(), eq(42L));
			verify(provider).provide(metadata.capture(), eq(43L));
			assertThat(metadata.getAllValues().get(0)).isSameAs(metadata.getAllValues().get(1));
			assertThat(metadata.getValue().getGroupId()).isEqualTo("grp");
			verifyNoMoreInteractions(provider);
			verifyNoInteractions(replacement);
		}
		finally {
			releaseListener.countDown();
			container.stop();
		}
	}

	@SuppressWarnings({ "rawtypes", "unchecked" })
	@Test
	void testThreadStarvation() throws InterruptedException {
		ConsumerFactory consumerFactory = mock(ConsumerFactory.class);
		final Consumer consumer = mock(Consumer.class);
		Set<String> consumerThreads = ConcurrentHashMap.newKeySet();
		CountDownLatch latch = new CountDownLatch(2);
		willAnswer(invocation -> {
			consumerThreads.add(Thread.currentThread().getName());
			latch.countDown();
			Thread.sleep(50);
			return new ConsumerRecords<>(Collections.emptyMap(), Map.of());
		}).given(consumer).poll(any());
		given(consumerFactory.createConsumer(anyString(), anyString(), anyString(),
				eq(KafkaTestUtils.defaultPropertyOverrides())))
						.willReturn(consumer);
		ContainerProperties containerProperties = new ContainerProperties("foo");
		containerProperties.setGroupId("grp");
		containerProperties.setMessageListener((MessageListener) record -> { });
		containerProperties.setMissingTopicsFatal(false);
		ThreadPoolTaskExecutor exec = new ThreadPoolTaskExecutor();
		exec.setCorePoolSize(1);
		exec.afterPropertiesSet();
		containerProperties.setListenerTaskExecutor(exec);
		containerProperties.setConsumerStartTimeout(Duration.ofMillis(50));
		ConcurrentMessageListenerContainer container = new ConcurrentMessageListenerContainer<>(consumerFactory,
				containerProperties);
		container.setConcurrency(2);
		CountDownLatch startedLatch = new CountDownLatch(2);
		CountDownLatch failedLatch = new CountDownLatch(1);
		container.setApplicationEventPublisher(event -> {
			if (event instanceof ConsumerStartingEvent || event instanceof ConsumerStartedEvent) {
				startedLatch.countDown();
			}
			else if (event instanceof ConsumerFailedToStartEvent) {
				failedLatch.countDown();
			}
		});
		container.start();
		assertThat(latch.await(10, TimeUnit.SECONDS)).isTrue();
		assertThat(consumerThreads).hasSize(1);
		assertThat(startedLatch.await(10, TimeUnit.SECONDS)).isTrue();
		assertThat(failedLatch.await(10, TimeUnit.SECONDS)).isTrue();
		container.stop();
		exec.destroy();
	}

	@SuppressWarnings({ "rawtypes", "unchecked" })
	@Test
	void testCorrectContainerForConsumerError() throws InterruptedException {
		ConsumerFactory consumerFactory = mock(ConsumerFactory.class);
		final Consumer consumer = mock(Consumer.class);
		AtomicBoolean first = new AtomicBoolean(true);
		willAnswer(invocation -> {
			if (first.getAndSet(false)) {
				throw new RuntimeException("planned");
			}
			Thread.sleep(100);
			return new ConsumerRecords<>(Collections.emptyMap(), Map.of());
		}).given(consumer).poll(any());
		given(consumerFactory.createConsumer("grp", "", "-0", KafkaTestUtils.defaultPropertyOverrides()))
			.willReturn(consumer);
		ContainerProperties containerProperties = new ContainerProperties("foo");
		containerProperties.setGroupId("grp");
		containerProperties.setMessageListener((MessageListener) record -> { });
		containerProperties.setMissingTopicsFatal(false);
		ConcurrentMessageListenerContainer container = new ConcurrentMessageListenerContainer<>(consumerFactory,
				containerProperties);
		CountDownLatch latch = new CountDownLatch(1);
		AtomicReference<MessageListenerContainer> errorContainer = new AtomicReference<>();
		container.setCommonErrorHandler(new CommonErrorHandler() {

			@Override
			public boolean seeksAfterHandling() {
				return true;
			}

			@Override
			public void handleOtherException(Exception thrownException, Consumer<?, ?> consumer,
					MessageListenerContainer container, boolean batchListener) {

				errorContainer.set(container);
				latch.countDown();
			}

		});
		container.start();
		assertThat(latch.await(10, TimeUnit.SECONDS)).isTrue();
		assertThat(errorContainer.get()).isSameAs(container);
		container.stop();
	}

	@SuppressWarnings({ "rawtypes", "unchecked" })
	@Test
	void delayedIdleEvent() throws InterruptedException {
		ConsumerFactory consumerFactory = mock(ConsumerFactory.class);
		final Consumer consumer = mock(Consumer.class);
		AtomicBoolean firstEvent = new AtomicBoolean(false);
		Map<TopicPartition, List<ConsumerRecord<String, String>>> recordMap = new HashMap<>();
		recordMap.put(new TopicPartition("foo", 0),
				Collections.singletonList(new ConsumerRecord("foo", 0, 0, null, "bar")));
		ConsumerRecords records = new ConsumerRecords<>(recordMap, Map.of());
		willAnswer(invocation -> {
			Thread.sleep(50);
			return firstEvent.getAndSet(false) ? records : new ConsumerRecords<>(Collections.emptyMap(), Map.of());
		}).given(consumer).poll(any());
		given(consumerFactory.createConsumer("grp", "", "-0", KafkaTestUtils.defaultPropertyOverrides()))
			.willReturn(consumer);
		ContainerProperties containerProperties = new ContainerProperties("foo");
		containerProperties.setGroupId("grp");
		containerProperties.setMessageListener((MessageListener) record -> { });
		containerProperties.setMissingTopicsFatal(false);
		containerProperties.setIdleEventInterval(100L);
		ConcurrentMessageListenerContainer container = new ConcurrentMessageListenerContainer<>(consumerFactory,
				containerProperties);
		CountDownLatch latch1 = new CountDownLatch(1);
		CountDownLatch latch2 = new CountDownLatch(2);
		container.setApplicationEventPublisher(event -> {
			if (event instanceof ListenerContainerIdleEvent) {
				latch1.countDown();
				latch2.countDown();
			}
		});
		container.start();
		long t1 = System.currentTimeMillis();
		assertThat(latch1.await(10, TimeUnit.SECONDS)).isTrue();
		assertThat(System.currentTimeMillis() - t1).isGreaterThanOrEqualTo(500L);
		firstEvent.set(true);
		long t2 = System.currentTimeMillis();
		assertThat(latch2.await(10, TimeUnit.SECONDS)).isTrue();
		assertThat(System.currentTimeMillis() - t2).isLessThan(500L);
		container.stop();
	}

	@SuppressWarnings({ "rawtypes", "unchecked" })
	@Test
	void testConsumerExitWhenNotAuthorized() throws InterruptedException {
		ConsumerFactory consumerFactory = mock(ConsumerFactory.class);
		final Consumer consumer = mock(Consumer.class);
		willAnswer(invocation -> {
			Thread.sleep(100);
			throw new GroupAuthorizationException("grp");
		}).given(consumer).poll(any());
		CountDownLatch latch = new CountDownLatch(1);
		willAnswer(invocation -> {
			latch.countDown();
			return null;
		}).given(consumer).close();
		given(consumerFactory.createConsumer("grp", "", "-0", KafkaTestUtils.defaultPropertyOverrides()))
			.willReturn(consumer);
		ContainerProperties containerProperties = new ContainerProperties("foo");
		containerProperties.setGroupId("grp");
		containerProperties.setMessageListener((MessageListener) record -> { });
		containerProperties.setMissingTopicsFatal(false);
		containerProperties.setShutdownTimeout(10);
		ConcurrentMessageListenerContainer container = new ConcurrentMessageListenerContainer<>(consumerFactory,
				containerProperties);
		container.start();
		assertThat(latch.await(10, TimeUnit.SECONDS)).isTrue();
		verify(consumer).close();
		container.stop();
	}

	@SuppressWarnings({ "rawtypes", "unchecked" })
	@Test
	@DisplayName("Seek on TL callback when idle")
	void testSyncRelativeSeeks() throws InterruptedException {
		ConsumerFactory consumerFactory = mock(ConsumerFactory.class);
		final Consumer consumer = mock(Consumer.class);
		TestMessageListener1 listener = new TestMessageListener1();
		ConsumerRecords empty = new ConsumerRecords<>(Collections.emptyMap(), Map.of());
		willAnswer(invocation -> {
			Thread.sleep(10);
			return empty;
		}).given(consumer).poll(any());
		TopicPartition tp0 = new TopicPartition("foo", 0);
		TopicPartition tp1 = new TopicPartition("foo", 1);
		TopicPartition tp2 = new TopicPartition("foo", 2);
		TopicPartition tp3 = new TopicPartition("foo", 3);
		List<TopicPartition> assignments = List.of(tp0, tp1, tp2, tp3);
		willAnswer(invocation -> {
			((ConsumerRebalanceListener) invocation.getArgument(1))
				.onPartitionsAssigned(assignments);
			return null;
		}).given(consumer).subscribe(any(Collection.class), any());
		given(consumer.position(any())).willReturn(100L);
		given(consumer.beginningOffsets(any())).willReturn(assignments.stream()
				.collect(Collectors.toMap(tp -> tp, tp -> 0L)));
		given(consumer.endOffsets(any())).willReturn(assignments.stream()
				.collect(Collectors.toMap(tp -> tp, tp -> 200L)));
		given(consumerFactory.createConsumer("grp", "", "-0", KafkaTestUtils.defaultPropertyOverrides()))
			.willReturn(consumer);
		ContainerProperties containerProperties = new ContainerProperties("foo");
		containerProperties.setGroupId("grp");
		containerProperties.setMessageListener(listener);
		containerProperties.setIdleEventInterval(10L);
		containerProperties.setMissingTopicsFatal(false);
		ConcurrentMessageListenerContainer container = new ConcurrentMessageListenerContainer(consumerFactory,
				containerProperties);
		container.start();
		assertThat(listener.latch.await(10, TimeUnit.SECONDS)).isTrue();
		verify(consumer).seek(tp0, 60L);
		verify(consumer).seek(tp1, 140L);
		verify(consumer).seek(tp2, 160L);
		verify(consumer).seek(tp3, 40L);
		container.stop();
	}

	@SuppressWarnings({ "rawtypes", "unchecked" })
	@Test
	void seekOffsetFromComputeFnOnInitAssignmentAndIdleContainer() throws InterruptedException {
		ConsumerFactory consumerFactory = mock(ConsumerFactory.class);
		final Consumer consumer = mock(Consumer.class);
		TestMessageListener3 listener = new TestMessageListener3();
		ConsumerRecords empty = new ConsumerRecords<>(Collections.emptyMap(), Map.of());
		willAnswer(invocation -> {
			Thread.sleep(10);
			return empty;
		}).given(consumer).poll(any());
		TopicPartition tp0 = new TopicPartition("test-topic", 0);
		TopicPartition tp1 = new TopicPartition("test-topic", 1);
		TopicPartition tp2 = new TopicPartition("test-topic", 2);
		TopicPartition tp3 = new TopicPartition("test-topic", 3);
		List<TopicPartition> assignments = List.of(tp0, tp1, tp2, tp3);
		willAnswer(invocation -> {
			((ConsumerRebalanceListener) invocation.getArgument(1))
					.onPartitionsAssigned(assignments);
			return null;
		}).given(consumer).subscribe(any(Collection.class), any());
		given(consumer.position(any())).willReturn(30L); // current offset position is always 30
		given(consumerFactory.createConsumer("grp", "", "-0", KafkaTestUtils.defaultPropertyOverrides()))
				.willReturn(consumer);
		ContainerProperties containerProperties = new ContainerProperties("test-topic");
		containerProperties.setGroupId("grp");
		containerProperties.setMessageListener(listener);
		containerProperties.setIdleEventInterval(10L);
		containerProperties.setMissingTopicsFatal(false);
		ConcurrentMessageListenerContainer container = new ConcurrentMessageListenerContainer(consumerFactory,
				containerProperties);
		container.start();
		assertThat(listener.latch.await(10, TimeUnit.SECONDS)).isTrue();
		verify(consumer).seek(tp0, 20L);
		verify(consumer).seek(tp1, 21L);
		verify(consumer).seek(tp2, 22L);
		verify(consumer).seek(tp3, 23L);

		verify(consumer).seek(tp0, 30L);
		verify(consumer).seek(tp1, 30L);
		verify(consumer).seek(tp2, 30L);
		verify(consumer).seek(tp3, 30L);
		container.stop();
	}

	@SuppressWarnings({ "rawtypes", "unchecked" })
	@Test
	void seekOffsetFromComputeFnFromActiveListener() throws InterruptedException {
		ConsumerFactory consumerFactory = mock(ConsumerFactory.class);
		final Consumer consumer = mock(Consumer.class);
		TestMessageListener4 listener = new TestMessageListener4();
		CountDownLatch latch = new CountDownLatch(2);
		TopicPartition tp0 = new TopicPartition("test-topic", 0);
		TopicPartition tp1 = new TopicPartition("test-topic", 1);
		TopicPartition tp2 = new TopicPartition("test-topic", 2);
		TopicPartition tp3 = new TopicPartition("test-topic", 3);
		List<TopicPartition> assignments = List.of(tp0, tp1, tp2, tp3);
		Map<TopicPartition, List<ConsumerRecord<String, String>>> recordMap = new HashMap<>();
		recordMap.put(tp0, Collections.singletonList(new ConsumerRecord("test-topic", 0, 0, null, "test-data")));
		recordMap.put(tp1, Collections.singletonList(new ConsumerRecord("test-topic", 1, 0, null, "test-data")));
		recordMap.put(tp2, Collections.singletonList(new ConsumerRecord("test-topic", 2, 0, null, "test-data")));
		recordMap.put(tp3, Collections.singletonList(new ConsumerRecord("test-topic", 3, 0, null, "test-data")));
		ConsumerRecords records = new ConsumerRecords<>(recordMap, Map.of());
		willAnswer(invocation -> {
			Thread.sleep(10);
			if (listener.latch.getCount() <= 0) {
				latch.countDown();
			}
			return records;
		}).given(consumer).poll(any());
		willAnswer(invocation -> {
			((ConsumerRebalanceListener) invocation.getArgument(1))
					.onPartitionsAssigned(assignments);
			return null;
		}).given(consumer).subscribe(any(Collection.class), any());
		given(consumer.position(tp0)).willReturn(30L); // current offset 30, target 20 (see hard-coded in onMessage)
		given(consumer.position(tp1)).willReturn(10L); // current 10, target 21
		given(consumer.position(tp2)).willReturn(22L); // current 22, target 22
		given(consumer.position(tp3)).willReturn(22L); // current 22, target 23
		given(consumer.beginningOffsets(any())).willReturn(assignments.stream()
				.collect(Collectors.toMap(tp -> tp, tp -> 0L)));
		given(consumer.endOffsets(any())).willReturn(assignments.stream()
				.collect(Collectors.toMap(tp -> tp, tp -> 100L)));
		given(consumerFactory.createConsumer("grp", "", "-0", KafkaTestUtils.defaultPropertyOverrides()))
				.willReturn(consumer);
		ContainerProperties containerProperties = new ContainerProperties("test-topic");
		containerProperties.setGroupId("grp");
		containerProperties.setMessageListener(listener);
		containerProperties.setMissingTopicsFatal(false);
		ConcurrentMessageListenerContainer container = new ConcurrentMessageListenerContainer(consumerFactory,
				containerProperties);
		container.start();
		assertThat(latch.await(10, TimeUnit.SECONDS)).isTrue();
		verify(consumer).seek(tp0, 20L);
		verify(consumer).seek(tp1, 10L);
		verify(consumer).seek(tp2, 22L);
		verify(consumer).seek(tp3, 22L);
		container.stop();
	}

	@SuppressWarnings({ "rawtypes", "unchecked" })
	@Test
	@DisplayName("Seek from activeListener")
	void testAsyncRelativeSeeks() throws InterruptedException {
		ConsumerFactory consumerFactory = mock(ConsumerFactory.class);
		final Consumer consumer = mock(Consumer.class);
		TestMessageListener1 listener = new TestMessageListener1();
		CountDownLatch latch = new CountDownLatch(2);
		TopicPartition tp0 = new TopicPartition("foo", 0);
		TopicPartition tp1 = new TopicPartition("foo", 1);
		TopicPartition tp2 = new TopicPartition("foo", 2);
		TopicPartition tp3 = new TopicPartition("foo", 3);
		List<TopicPartition> assignments = List.of(tp0, tp1, tp2, tp3);
		Map<TopicPartition, List<ConsumerRecord<String, String>>> recordMap = new HashMap<>();
		recordMap.put(tp0, Collections.singletonList(new ConsumerRecord("foo", 0, 0, null, "bar")));
		recordMap.put(tp1, Collections.singletonList(new ConsumerRecord("foo", 1, 0, null, "bar")));
		recordMap.put(tp2, Collections.singletonList(new ConsumerRecord("foo", 2, 0, null, "bar")));
		recordMap.put(tp3, Collections.singletonList(new ConsumerRecord("foo", 3, 0, null, "bar")));
		ConsumerRecords records = new ConsumerRecords<>(recordMap, Map.of());
		willAnswer(invocation -> {
			Thread.sleep(10);
			if (listener.latch.getCount() <= 0) {
				latch.countDown();
			}
			return records;
		}).given(consumer).poll(any());
		willAnswer(invocation -> {
			((ConsumerRebalanceListener) invocation.getArgument(1))
				.onPartitionsAssigned(assignments);
			return null;
		}).given(consumer).subscribe(any(Collection.class), any());
		given(consumer.position(any())).willReturn(100L);
		given(consumer.beginningOffsets(any())).willReturn(assignments.stream()
				.collect(Collectors.toMap(tp -> tp, tp -> 0L)));
		given(consumer.endOffsets(any())).willReturn(assignments.stream()
				.collect(Collectors.toMap(tp -> tp, tp -> 200L)));
		given(consumerFactory.createConsumer("grp", "", "-0", KafkaTestUtils.defaultPropertyOverrides()))
			.willReturn(consumer);
		ContainerProperties containerProperties = new ContainerProperties("foo");
		containerProperties.setGroupId("grp");
		containerProperties.setMessageListener(listener);
		containerProperties.setMissingTopicsFatal(false);
		ConcurrentMessageListenerContainer container = new ConcurrentMessageListenerContainer(consumerFactory,
				containerProperties);
		container.start();
		assertThat(latch.await(10, TimeUnit.SECONDS)).isTrue();
		verify(consumer).seek(tp0, 70L);
		verify(consumer).seek(tp1, 130L);
		verify(consumer).seekToEnd(Collections.singletonList(tp2));
		verify(consumer).seek(tp2, 70L); // position - 30 (seekToEnd ignored by mock)
		verify(consumer).seekToBeginning(Collections.singletonList(tp3));
		verify(consumer).seek(tp3, 130L); // position + 30 (seekToBeginning ignored by mock)
		container.stop();
	}

	@SuppressWarnings({ "rawtypes", "unchecked" })
	@Test
	@DisplayName("Seek timestamp TL callback when idle")
	void testSyncTimestampSeeks() throws InterruptedException {
		ConsumerFactory consumerFactory = mock(ConsumerFactory.class);
		final Consumer consumer = mock(Consumer.class);
		TestMessageListener2 listener = new TestMessageListener2();
		ConsumerRecords empty = new ConsumerRecords<>(Collections.emptyMap(), Map.of());
		willAnswer(invocation -> {
			Thread.sleep(10);
			return empty;
		}).given(consumer).poll(any());
		TopicPartition tp0 = new TopicPartition("foo", 0);
		TopicPartition tp1 = new TopicPartition("foo", 1);
		TopicPartition tp2 = new TopicPartition("foo", 2);
		TopicPartition tp3 = new TopicPartition("foo", 3);
		List<TopicPartition> assignments = List.of(tp0, tp1, tp2, tp3);
		willAnswer(invocation -> {
			((ConsumerRebalanceListener) invocation.getArgument(1))
				.onPartitionsAssigned(assignments);
			return null;
		}).given(consumer).subscribe(any(Collection.class), any());
		given(consumer.position(any())).willReturn(100L);
		given(consumer.beginningOffsets(any())).willReturn(assignments.stream()
				.collect(Collectors.toMap(tp -> tp, tp -> 0L)));
		given(consumer.endOffsets(any())).willReturn(assignments.stream()
				.collect(Collectors.toMap(tp -> tp, tp -> 200L)));
		given(consumer.offsetsForTimes(Collections.singletonMap(tp0, 42L)))
				.willReturn(Collections.singletonMap(tp0, new OffsetAndTimestamp(63L, 42L)));
		given(consumer.offsetsForTimes(assignments.stream().collect(Collectors.toMap(tp -> tp, tp -> 43L))))
				.willReturn(assignments.stream()
						.collect(Collectors.toMap(tp -> tp, tp -> new OffsetAndTimestamp(82L, 43L))));
		given(consumerFactory.createConsumer("grp", "", "-0", KafkaTestUtils.defaultPropertyOverrides()))
			.willReturn(consumer);
		ContainerProperties containerProperties = new ContainerProperties("foo");
		containerProperties.setGroupId("grp");
		containerProperties.setMessageListener(listener);
		containerProperties.setIdleEventInterval(10L);
		containerProperties.setMissingTopicsFatal(false);
		ConcurrentMessageListenerContainer container = new ConcurrentMessageListenerContainer(consumerFactory,
				containerProperties);
		container.start();
		assertThat(listener.latch.await(10, TimeUnit.SECONDS)).isTrue();
		verify(consumer).seek(tp0, 63L);
		verify(consumer).seek(tp0, 82L);
		verify(consumer).seek(tp1, 82L);
		verify(consumer).seek(tp2, 82L);
		verify(consumer).seek(tp3, 82L);
		container.stop();
	}

	@SuppressWarnings({ "rawtypes", "unchecked" })
	@Test
	@DisplayName("Timestamp seek from activeListener")
	void testAsyncTimestampSeeks() throws InterruptedException {
		ConsumerFactory consumerFactory = mock(ConsumerFactory.class);
		final Consumer consumer = mock(Consumer.class);
		TestMessageListener2 listener = new TestMessageListener2();
		CountDownLatch latch = new CountDownLatch(2);
		TopicPartition tp0 = new TopicPartition("foo", 0);
		TopicPartition tp1 = new TopicPartition("foo", 1);
		TopicPartition tp2 = new TopicPartition("foo", 2);
		TopicPartition tp3 = new TopicPartition("foo", 3);
		List<TopicPartition> assignments = List.of(tp0, tp1, tp2, tp3);
		Map<TopicPartition, List<ConsumerRecord<String, String>>> recordMap = new HashMap<>();
		recordMap.put(tp0, Collections.singletonList(new ConsumerRecord("foo", 0, 0, null, "bar")));
		recordMap.put(tp1, Collections.singletonList(new ConsumerRecord("foo", 1, 0, null, "bar")));
		recordMap.put(tp2, Collections.singletonList(new ConsumerRecord("foo", 2, 0, null, "bar")));
		recordMap.put(tp3, Collections.singletonList(new ConsumerRecord("foo", 3, 0, null, "bar")));
		ConsumerRecords records = new ConsumerRecords<>(recordMap, Map.of());
		willAnswer(invocation -> {
			Thread.sleep(10);
			if (listener.latch.getCount() <= 0) {
				latch.countDown();
			}
			return records;
		}).given(consumer).poll(any());
		willAnswer(invocation -> {
			((ConsumerRebalanceListener) invocation.getArgument(1))
				.onPartitionsAssigned(assignments);
			return null;
		}).given(consumer).subscribe(any(Collection.class), any());
		given(consumer.position(any())).willReturn(100L);
		given(consumer.beginningOffsets(any())).willReturn(assignments.stream()
				.collect(Collectors.toMap(tp -> tp, tp -> 0L)));
		given(consumer.endOffsets(any())).willReturn(assignments.stream()
				.collect(Collectors.toMap(tp -> tp, tp -> 200L)));
		given(consumer.offsetsForTimes(Collections.singletonMap(tp0, 42L)))
				.willReturn(Collections.singletonMap(tp0, new OffsetAndTimestamp(73L, 42L)));
		Map<TopicPartition, OffsetAndTimestamp> map = new HashMap<>(assignments.stream()
				.collect(Collectors.toMap(tp -> tp,
						tp -> new OffsetAndTimestamp(tp.equals(tp0) ? 73L : 92L, 43L))));
		map.put(new TopicPartition("foo", 5), null);
		given(consumer.offsetsForTimes(any()))
				.willReturn(map);
		given(consumerFactory.createConsumer("grp", "", "-0", KafkaTestUtils.defaultPropertyOverrides()))
			.willReturn(consumer);
		ContainerProperties containerProperties = new ContainerProperties("foo");
		containerProperties.setGroupId("grp");
		containerProperties.setMessageListener(listener);
		containerProperties.setMissingTopicsFatal(false);
		ConcurrentMessageListenerContainer container = new ConcurrentMessageListenerContainer(consumerFactory,
				containerProperties);
		container.start();
		assertThat(latch.await(10, TimeUnit.SECONDS)).isTrue();
		verify(consumer).seek(tp0, 73L);
		verify(consumer).seek(tp1, 92L);
		verify(consumer).seek(tp2, 92L);
		verify(consumer).seek(tp3, 92L);
		container.stop();
	}

	@Test
	@DisplayName("Intercept after tx start")
	void testInterceptAfterTx() throws InterruptedException {
		testIntercept(false, AssignmentCommitOption.ALWAYS, false);
	}

	@Test
	@DisplayName("Intercept before tx start")
	void testInterceptBeforeTx() throws InterruptedException {
		testIntercept(true, AssignmentCommitOption.ALWAYS, false);
	}

	@Test
	@DisplayName("Intercept after tx start no initial commit")
	void testInterceptAfterTx1() throws InterruptedException {
		testIntercept(false, null, false);
	}

	@Test
	@DisplayName("Intercept before tx start no initial commit")
	void testInterceptBeforeTx1() throws InterruptedException {
		testIntercept(true, null, false);
	}

	@Test
	@DisplayName("Intercept batch after tx start")
	void testBatchInterceptAfterTx() throws InterruptedException {
		testIntercept(false, AssignmentCommitOption.ALWAYS, true);
	}

	@Test
	@DisplayName("Intercept batch before tx start")
	void testBatchInterceptBeforeTx() throws InterruptedException {
		testIntercept(true, AssignmentCommitOption.ALWAYS, true);
	}

	@Test
	@DisplayName("Intercept batch after tx start no initial commit")
	void testBatchInterceptAfterTx1() throws InterruptedException {
		testIntercept(false, null, true);
	}

	@Test
	@DisplayName("Intercept batch before tx start no initial commit")
	void testBatchInterceptBeforeTx1() throws InterruptedException {
		testIntercept(true, null, true);
	}

	@SuppressWarnings({ "rawtypes", "unchecked" })
	void testIntercept(boolean beforeTx, @Nullable AssignmentCommitOption option, boolean batch)
			throws InterruptedException {

		ConsumerFactory consumerFactory = mock(ConsumerFactory.class);
		final Consumer consumer = mock(Consumer.class);
		TopicPartition tp0 = new TopicPartition("foo", 0);
		ConsumerRecord record1 = new ConsumerRecord("foo", 0, 0L, "bar", "baz");
		ConsumerRecord record2 = new ConsumerRecord("foo", 0, 1L, null, null);
		ConsumerRecords records = batch
				? new ConsumerRecords(Collections.singletonMap(tp0, List.of(record1, record2)), Map.of())
				: new ConsumerRecords(Collections.singletonMap(tp0, Collections.singletonList(record1)), Map.of());
		ConsumerRecords empty = new ConsumerRecords<>(Collections.emptyMap(), Map.of());
		AtomicInteger firstOrSecondPoll = new AtomicInteger();
		willAnswer(invocation -> {
			Thread.sleep(10);
			return firstOrSecondPoll.incrementAndGet() < 3 ? records : empty;
		}).given(consumer).poll(any());
		List<TopicPartition> assignments = List.of(tp0);
		willAnswer(invocation -> {
			((ConsumerRebalanceListener) invocation.getArgument(1))
				.onPartitionsAssigned(assignments);
			return null;
		}).given(consumer).subscribe(any(Collection.class), any());
		given(consumer.position(any())).willReturn(0L);
		given(consumerFactory.createConsumer("grp", "", "-0", KafkaTestUtils.defaultPropertyOverrides()))
			.willReturn(consumer);
		ContainerProperties containerProperties = new ContainerProperties("foo");
		containerProperties.setGroupId("grp");
		AtomicBoolean first = new AtomicBoolean(true);
		AtomicReference<List<ConsumerRecord<String, String>>> received = new AtomicReference<>();
		if (batch) {
			containerProperties.setMessageListener((BatchMessageListener<String, String>) recs -> {
				if (first.getAndSet(false)) {
					throw new RuntimeException("test");
				}
				received.set(recs);
			});
		}
		else {
			containerProperties.setMessageListener((MessageListener) rec -> {
				if (first.getAndSet(false)) {
					throw new RuntimeException("test");
				}
			});
		}
		containerProperties.setMissingTopicsFatal(false);
		if (option != null) {
			containerProperties.setAssignmentCommitOption(option);
		}
		KafkaAwareTransactionManager tm = mock(KafkaAwareTransactionManager.class);
		ProducerFactory pf = mock(ProducerFactory.class);
		given(tm.getProducerFactory()).willReturn(pf);
		Producer producer = mock(Producer.class);
		given(pf.createProducer()).willReturn(producer);
		containerProperties.setKafkaAwareTransactionManager(tm);
		List<String> order = new ArrayList<>();
		CountDownLatch latch = new CountDownLatch(option == null ? 2 : 3);
		willAnswer(inv -> {
			order.add("tx");
			TransactionSynchronizationManager.bindResource(pf,
					new KafkaResourceHolder<>(producer, Duration.ofSeconds(5L)));
			latch.countDown();
			return null;
		}).given(tm).getTransaction(any());
		willAnswer(inv -> {
			TransactionSynchronizationManager.unbindResource(pf);
			return null;
		}).given(tm).commit(any());
		willAnswer(inv -> {
			TransactionSynchronizationManager.unbindResource(pf);
			return null;
		}).given(tm).rollback(any());
		ConcurrentMessageListenerContainer container = new ConcurrentMessageListenerContainer(consumerFactory,
				containerProperties);
		CountDownLatch successCalled = new CountDownLatch(1);
		CountDownLatch failureCalled = new CountDownLatch(1);
		container.setRecordInterceptor(new RecordInterceptor() {

			@Override
			public @Nullable ConsumerRecord intercept(ConsumerRecord rec, Consumer consumer) {
				order.add("interceptor");
				latch.countDown();
				return rec;
			}

			@Override
			public void success(ConsumerRecord record, Consumer consumer) {
				order.add("success");
				successCalled.countDown();
			}

			@Override
			public void failure(ConsumerRecord record, Exception exception, Consumer consumer) {
				order.add("failure");
				failureCalled.countDown();
			}

		});
		container.setBatchInterceptor(new BatchInterceptor() {

			@Override
			public @Nullable ConsumerRecords intercept(ConsumerRecords recs, Consumer consumer) {
				order.add("interceptor");
				latch.countDown();
				return new ConsumerRecords(Collections.singletonMap(tp0, Collections.singletonList(record1)), Map.of());
			}

			@Override
			public void success(ConsumerRecords records, Consumer consumer) {
				order.add("success");
				successCalled.countDown();
			}

			@Override
			public void failure(ConsumerRecords records, Exception exception, Consumer consumer) {
				order.add("failure");
				failureCalled.countDown();
			}

		});
		container.setInterceptBeforeTx(beforeTx);
		container.start();
		try {
			assertThat(latch.await(10, TimeUnit.SECONDS)).isTrue();
			assertThat(failureCalled.await(10, TimeUnit.SECONDS)).isTrue();
			assertThat(successCalled.await(10, TimeUnit.SECONDS)).isTrue();
			if (beforeTx) {
				if (option == null) {
					assertThat(order).containsExactly("interceptor", "tx", "failure", "interceptor", "tx", "success");
				}
				else {
					assertThat(order).containsExactly("tx", "interceptor", "tx", "failure", "interceptor", "tx",
							"success"); // first one is on assignment
				}
			}
			else {
				if (option == null) {
					assertThat(order).containsExactly("tx", "interceptor", "failure", "tx", "interceptor", "success");
				}
				else {
					assertThat(order).containsExactly("tx", "tx", "interceptor", "failure", "tx", "interceptor",
							"success");
				}
			}
			if (batch) {
				assertThat(received.get()).hasSize(1);
			}
		}
		finally {
			container.stop();
		}
	}

	@SuppressWarnings({ "rawtypes", "unchecked" })
	@Test
	void testNoCommitOnAssignmentWithEarliest() throws InterruptedException {
		Consumer consumer = mock(Consumer.class);
		ConsumerRecords records = new ConsumerRecords<>(Collections.emptyMap(), Map.of());
		CountDownLatch latch = new CountDownLatch(1);
		willAnswer(inv -> {
			latch.countDown();
			Thread.sleep(50);
			return records;
		}).given(consumer).poll(any());
		TopicPartition tp0 = new TopicPartition("foo", 0);
		List<TopicPartition> assignments = List.of(tp0);
		willAnswer(invocation -> {
			((ConsumerRebalanceListener) invocation.getArgument(1))
				.onPartitionsAssigned(assignments);
			return null;
		}).given(consumer).subscribe(any(Collection.class), any());
		ConsumerFactory cf = mock(ConsumerFactory.class);
		given(cf.createConsumer(any(), any(), any(), any())).willReturn(consumer);
		given(cf.getConfigurationProperties())
				.willReturn(Collections.singletonMap(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest"));
		ContainerProperties containerProperties = new ContainerProperties("foo");
		containerProperties.setGroupId("grp");
		containerProperties.setMessageListener((MessageListener) rec -> { });
		containerProperties.setMissingTopicsFatal(false);
		containerProperties.setAssignmentCommitOption(AssignmentCommitOption.LATEST_ONLY);
		ConcurrentMessageListenerContainer container = new ConcurrentMessageListenerContainer(cf,
				containerProperties);
		container.start();
		assertThat(latch.await(10, TimeUnit.SECONDS)).isTrue();
		verify(consumer, never()).commitSync(any(), any());
	}

	@Test
	void testNoInitialCommitIfAlreadyCommitted() throws InterruptedException {
		testInitialCommitIBasedOnCommitted(true);
	}

	@Test
	void testInitialCommitIfNotAlreadyCommitted() throws InterruptedException {
		testInitialCommitIBasedOnCommitted(false);
	}

	@SuppressWarnings({ "rawtypes", "unchecked" })
	private void testInitialCommitIBasedOnCommitted(boolean committed) throws InterruptedException {
		Consumer consumer = mock(Consumer.class);
		ConsumerRecords records = new ConsumerRecords<>(Collections.emptyMap(), Map.of());
		CountDownLatch latch = new CountDownLatch(1);
		willAnswer(inv -> {
			latch.countDown();
			Thread.sleep(50);
			return records;
		}).given(consumer).poll(any());
		TopicPartition tp0 = new TopicPartition("foo", 0);
		List<TopicPartition> assignments = List.of(tp0);
		willAnswer(invocation -> {
			((ConsumerRebalanceListener) invocation.getArgument(1))
				.onPartitionsAssigned(assignments);
			return null;
		}).given(consumer).subscribe(any(Collection.class), any());
		if (committed) {
			given(consumer.committed(Collections.singleton(tp0)))
					.willReturn(Collections.singletonMap(tp0, new OffsetAndMetadata(0L)));
		}
		else {
			given(consumer.committed(Collections.singleton(tp0)))
					.willReturn(Collections.singletonMap(tp0, null));
		}
		ConsumerFactory cf = mock(ConsumerFactory.class);
		given(cf.createConsumer(any(), any(), any(), any())).willReturn(consumer);
		given(cf.getConfigurationProperties())
				.willReturn(Collections.singletonMap(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest"));
		ContainerProperties containerProperties = new ContainerProperties("foo");
		containerProperties.setGroupId("grp");
		containerProperties.setMessageListener((MessageListener) rec -> { });
		containerProperties.setMissingTopicsFatal(false);
		containerProperties.setAssignmentCommitOption(AssignmentCommitOption.LATEST_ONLY);
		ConcurrentMessageListenerContainer container = new ConcurrentMessageListenerContainer(cf,
				containerProperties);
		container.start();
		assertThat(latch.await(10, TimeUnit.SECONDS)).isTrue();
		if (committed) {
			verify(consumer, never()).commitSync(any(), any());
		}
		else {
			verify(consumer).commitSync(any(), any());
		}
	}

	@SuppressWarnings({ "unchecked", "rawtypes" })
	@ParameterizedTest
	@ValueSource(strings = { "wakeup", "retriable" })
	void commitRetriedAfterWakeup(String failure) throws InterruptedException {
		TopicPartition tp0 = new TopicPartition("foo", 0);
		ConsumerRecords records = new ConsumerRecords<>(
				Map.of(tp0, List.of(epochRecord(tp0, 0, Optional.of(7)))), Map.of());
		AtomicInteger pollPhase = new AtomicInteger();
		Consumer consumer = mock(Consumer.class);
		AtomicReference<ConsumerRebalanceListener> rebal = new AtomicReference<>();
		willAnswer(invocation -> {
			rebal.set(invocation.getArgument(1));
			return null;
		}).given(consumer).subscribe(any(Collection.class), any());
		willAnswer(inv -> {
			if (pollPhase.getAndIncrement() == 0) {
				rebal.get().onPartitionsAssigned(List.of(tp0));
				return records;
			}
			Thread.sleep(50);
			return ConsumerRecords.empty();
		}).given(consumer).poll(any());
		AtomicInteger commitAttempts = new AtomicInteger();
		CountDownLatch committed = new CountDownLatch(1);
		willAnswer(inv -> {
			// a wakeup that landed after the poll returned aborts the first attempt
			if (commitAttempts.getAndIncrement() == 0) {
				if (failure.equals("wakeup")) {
					throw new WakeupException();
				}
				throw new org.apache.kafka.clients.consumer.RetriableCommitFailedException("retry");
			}
			committed.countDown();
			return null;
		}).given(consumer).commitSync(any(Map.class), any(Duration.class));
		ConsumerFactory cf = mock(ConsumerFactory.class);
		given(cf.createConsumer(any(), any(), any(), any())).willReturn(consumer);
		given(cf.getConfigurationProperties())
				.willReturn(Collections.singletonMap(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest"));
		ContainerProperties containerProperties = new ContainerProperties("foo");
		containerProperties.setGroupId("grp");
		OffsetAndMetadataProvider provider = mock();
		given(provider.provide(any(), eq(1L))).willReturn(new OffsetAndMetadata(1));
		containerProperties.setOffsetAndMetadataProvider(provider);
		containerProperties.setAckMode(AckMode.MANUAL);
		containerProperties.setMessageListener((AcknowledgingMessageListener<String, String>) (rec, ack) -> {
			ack.acknowledge();
		});
		ConcurrentMessageListenerContainer container = new ConcurrentMessageListenerContainer(cf,
				containerProperties);
		container.start();
		assertThat(committed.await(10, TimeUnit.SECONDS)).isTrue();
		container.stop();
		ArgumentCaptor<Map<TopicPartition, OffsetAndMetadata>> commits = ArgumentCaptor.forClass(Map.class);
		verify(consumer, times(2)).commitSync(commits.capture(), any(Duration.class));
		assertThat(commits.getAllValues().get(0)).containsEntry(tp0, new OffsetAndMetadata(1, Optional.of(7), ""));
		assertThat(commits.getAllValues().get(1)).isSameAs(commits.getAllValues().get(0));
		verify(provider).provide(any(), eq(1L));
		verifyNoMoreInteractions(provider);
	}

	@SuppressWarnings({ "unchecked", "rawtypes" })
	@Test
	void inFlightAsyncResultsCancelledOnFatalError() throws Exception {
		TopicPartition tp0 = new TopicPartition("foo", 0);
		ConsumerRecords records = new ConsumerRecords<>(
				Map.of(tp0, List.of(new ConsumerRecord("foo", 0, 0, null, "bar"))), Map.of());
		AtomicInteger pollPhase = new AtomicInteger();
		Consumer consumer = mock();
		AtomicReference<ConsumerRebalanceListener> rebal = new AtomicReference<>();
		willAnswer(invocation -> {
			rebal.set(invocation.getArgument(1));
			return null;
		}).given(consumer).subscribe(any(Collection.class), any());
		willAnswer(inv -> {
			if (pollPhase.getAndIncrement() == 0) {
				rebal.get().onPartitionsAssigned(List.of(tp0));
				return records;
			}
			Thread.sleep(50);
			// fatal without an authExceptionRetryInterval
			throw new GroupAuthorizationException("grp");
		}).given(consumer).poll(any());
		List<String> events = Collections.synchronizedList(new ArrayList<>());
		CountDownLatch closed = new CountDownLatch(1);
		willAnswer(inv -> {
			events.add("closed");
			closed.countDown();
			return null;
		}).given(consumer).close();
		ConsumerFactory cf = mock();
		given(cf.createConsumer(any(), any(), any(), any())).willReturn(consumer);
		given(cf.getConfigurationProperties())
				.willReturn(Collections.singletonMap(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest"));
		this.pendingResult.whenComplete((r, t) ->
				events.add(t instanceof CancellationException ? "cancelled" : "completed"));
		Method method = getClass().getDeclaredMethod("pendingResult", String.class);
		RecordMessagingMessageListenerAdapter<String, String> adapter =
				new RecordMessagingMessageListenerAdapter<>(this, method);
		adapter.setHandlerMethod(new HandlerAdapter(new KafkaListenerAnnotationBeanPostProcessor<>()
				.getMessageHandlerMethodFactory().createInvocableHandlerMethod(this, method)));
		ContainerProperties containerProperties = new ContainerProperties("foo");
		containerProperties.setGroupId("grp");
		containerProperties.setMessageListener(adapter);
		containerProperties.setAwaitAsyncResultsOnStop(true);
		ConcurrentMessageListenerContainer container = new ConcurrentMessageListenerContainer(cf,
				containerProperties);
		container.start();
		assertThat(closed.await(10, TimeUnit.SECONDS)).isTrue();
		// the in-flight result is cancelled before the consumer is closed
		assertThat(this.pendingResult).isCancelled();
		assertThat(events).containsExactly("cancelled", "closed");
		container.stop();
	}

	public CompletableFuture<Void> pendingResult(String data) {
		return this.pendingResult;
	}

	@SuppressWarnings("unchecked")
	@Test
	void partitionPauseRequestedReflectsChildren() throws InterruptedException {
		Consumer<String, String> consumer0 = mock();
		Consumer<String, String> consumer1 = mock();
		CountDownLatch pollLatch = new CountDownLatch(2);
		for (Consumer<String, String> consumer : List.of(consumer0, consumer1)) {
			willAnswer(inv -> {
				pollLatch.countDown();
				Thread.sleep(50);
				return ConsumerRecords.empty();
			}).given(consumer).poll(any());
		}
		ConsumerFactory<String, String> cf = mock();
		given(cf.createConsumer(any(), any(), any(), any())).willReturn(consumer0, consumer1);
		ContainerProperties containerProperties = new ContainerProperties(
				new TopicPartitionOffset("foo", 0), new TopicPartitionOffset("foo", 1));
		containerProperties.setGroupId("grp");
		containerProperties.setMessageListener((MessageListener<String, String>) rec -> { });
		ConcurrentMessageListenerContainer<String, String> container =
				new ConcurrentMessageListenerContainer<>(cf, containerProperties);
		container.setConcurrency(2);
		TopicPartition tp0 = new TopicPartition("foo", 0);
		TopicPartition tp1 = new TopicPartition("foo", 1);
		TopicPartition unassigned = new TopicPartition("foo", 2);
		assertThat(container.isPartitionPauseRequested(tp0)).isFalse();
		container.start();
		try {
			assertThat(pollLatch.await(10, TimeUnit.SECONDS)).isTrue();
			assertThat(container.isPartitionPauseRequested(tp0)).isFalse();
			assertThat(container.isPartitionPauseRequested(tp1)).isFalse();
			container.pausePartition(tp1);
			assertThat(container.getContainers().get(1).isPartitionPauseRequested(tp1)).isTrue();
			assertThat(container.isPartitionPauseRequested(tp1)).isTrue();
			assertThat(container.isPartitionPauseRequested(tp0)).isFalse();
			// Like a single container, the request is kept for a partition that is not (yet) assigned
			container.pausePartition(unassigned);
			assertThat(container.isPartitionPauseRequested(unassigned)).isTrue();
			container.resumePartition(unassigned);
			assertThat(container.isPartitionPauseRequested(unassigned)).isFalse();
			container.pausePartition(tp0);
			assertThat(container.isPartitionPauseRequested(tp0)).isTrue();
			container.resumePartition(tp1);
			assertThat(container.getContainers().get(1).isPartitionPauseRequested(tp1)).isFalse();
			assertThat(container.isPartitionPauseRequested(tp1)).isFalse();
			assertThat(container.isPartitionPauseRequested(tp0)).isTrue();
			container.resumePartition(tp0);
			assertThat(container.isPartitionPauseRequested(tp0)).isFalse();
		}
		finally {
			container.stop();
		}
	}

	@SuppressWarnings({ "unchecked", "rawtypes" })
	@Test
	void removeFromPartitionPauseRequestedWhenNotAssigned() throws InterruptedException {
		Consumer consumer = mock(Consumer.class);
		CountDownLatch pollLatch = new CountDownLatch(1);
		willAnswer(inv -> {
			pollLatch.countDown();
			Thread.sleep(50);
			return ConsumerRecords.empty();
		}).given(consumer).poll(any());
		CountDownLatch pauseLatch = new CountDownLatch(1);
		willAnswer(inv -> {
			pauseLatch.countDown();
			return null;
		}).given(consumer).pause(any());
		TopicPartition tp0 = new TopicPartition("foo", 0);
		List<TopicPartition> assignments = List.of(tp0);
		AtomicReference<ConsumerRebalanceListener> rebal = new AtomicReference<>();
		willAnswer(invocation -> {
			rebal.set(invocation.getArgument(1));
			rebal.get().onPartitionsAssigned(assignments);
			return null;
		}).given(consumer).subscribe(any(Collection.class), any());
		ConsumerFactory cf = mock(ConsumerFactory.class);
		given(cf.createConsumer(any(), any(), any(), any())).willReturn(consumer);
		given(cf.getConfigurationProperties())
				.willReturn(Collections.singletonMap(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest"));
		ContainerProperties containerProperties = new ContainerProperties("foo");
		containerProperties.setGroupId("grp");
		containerProperties.setMessageListener((MessageListener) rec -> { });
		containerProperties.setMissingTopicsFatal(false);
		containerProperties.setAssignmentCommitOption(AssignmentCommitOption.LATEST_ONLY);
		ConcurrentMessageListenerContainer container = new ConcurrentMessageListenerContainer(cf,
				containerProperties);
		container.start();
		assertThat(pollLatch.await(10, TimeUnit.SECONDS)).isTrue();
		container.pausePartition(tp0);
		KafkaMessageListenerContainer child = (KafkaMessageListenerContainer) KafkaTestUtils
				.getPropertyValue(container, "containers", List.class).get(0);
		assertThat(child.isPartitionPauseRequested(tp0)).isTrue();
		assertThat(container.isPartitionPauseRequested(tp0)).isTrue();
		assertThat(pauseLatch.await(10, TimeUnit.SECONDS)).isTrue();
		rebal.get().onPartitionsRevoked(assignments);
		assertThat(child.isPartitionPauseRequested(tp0)).isTrue();
		assertThat(container.isPartitionPauseRequested(tp0)).isTrue();
		// immediate pause when re-assigned
		rebal.get().onPartitionsAssigned(assignments);
		verify(consumer, times(2)).pause(any());
		rebal.get().onPartitionsRevoked(assignments);
		// resume partition while unassigned
		container.resumePartition(tp0);
		assertThat(child.isPartitionPauseRequested(tp0)).isFalse();
		assertThat(container.isPartitionPauseRequested(tp0)).isFalse();
		rebal.get().onPartitionsAssigned(assignments);
		verify(consumer, times(2)).pause(any()); // no immediate pause this time
		container.stop();
	}

	@SuppressWarnings("unchecked")
	@Test
	void partitionPauseRequestFollowsPartitionToAnotherChild() throws InterruptedException {
		Consumer<String, String> consumer0 = mock();
		Consumer<String, String> consumer1 = mock();
		CountDownLatch pollLatch = new CountDownLatch(2);
		Map<Consumer<String, String>, ConsumerRebalanceListener> rebalanceListeners = new ConcurrentHashMap<>();
		for (Consumer<String, String> consumer : List.of(consumer0, consumer1)) {
			willAnswer(inv -> {
				pollLatch.countDown();
				Thread.sleep(50);
				return ConsumerRecords.empty();
			}).given(consumer).poll(any());
			willAnswer(inv -> {
				rebalanceListeners.put(consumer, inv.getArgument(1));
				return null;
			}).given(consumer).subscribe(any(Collection.class), any());
		}
		ConsumerFactory<String, String> cf = mock();
		given(cf.createConsumer(any(), any(), any(), any())).willReturn(consumer0, consumer1);
		ContainerProperties containerProperties = new ContainerProperties("orders");
		containerProperties.setGroupId("grp");
		containerProperties.setMessageListener((MessageListener<String, String>) rec -> { });
		containerProperties.setMissingTopicsFatal(false);
		containerProperties.setAssignmentCommitOption(AssignmentCommitOption.NEVER);
		ConcurrentMessageListenerContainer<String, String> container =
				new ConcurrentMessageListenerContainer<>(cf, containerProperties);
		container.setConcurrency(2);
		TopicPartition orders0 = new TopicPartition("orders", 0);
		container.start();
		try {
			assertThat(pollLatch.await(10, TimeUnit.SECONDS)).isTrue();
			rebalanceListeners.get(consumer0).onPartitionsAssigned(List.of(orders0));
			container.pausePartition(orders0);
			assertThat(container.isPartitionPauseRequested(orders0)).isTrue();

			// The partition moves to the other child; it must stay paused there
			rebalanceListeners.get(consumer0).onPartitionsRevoked(List.of(orders0));
			rebalanceListeners.get(consumer1).onPartitionsAssigned(List.of(orders0));
			verify(consumer1).pause(List.of(orders0));
			assertThat(container.getContainers().get(1).isPartitionPauseRequested(orders0)).isTrue();
			assertThat(container.isPartitionPauseRequested(orders0)).isTrue();

			container.resumePartition(orders0);
			assertThat(container.isPartitionPauseRequested(orders0)).isFalse();
			verify(consumer1, timeout(10_000)).resume(List.of(orders0));
		}
		finally {
			container.stop();
		}
	}

	@SuppressWarnings({ "unchecked", "rawtypes" })
	@Test
	void pruneRevokedPartitionsFromRemainingRecordsWhenSeekAfterErrorFalseLegacyAssignor() throws InterruptedException {
		TopicPartition tp0 = new TopicPartition("foo", 0);
		TopicPartition tp1 = new TopicPartition("foo", 1);
		TopicPartition tp2 = new TopicPartition("foo", 2);
		TopicPartition tp3 = new TopicPartition("foo", 3);
		List<TopicPartition> allAssignments = List.of(tp0, tp1, tp2, tp3);
		Map<TopicPartition, List<ConsumerRecord<String, String>>> allRecordMap = new HashMap<>();
		allRecordMap.put(tp0, Collections.singletonList(new ConsumerRecord("foo", 0, 0, null, "bar")));
		allRecordMap.put(tp1, Collections.singletonList(new ConsumerRecord("foo", 1, 0, null, "bar")));
		allRecordMap.put(tp2, Collections.singletonList(new ConsumerRecord("foo", 2, 0, null, "bar")));
		allRecordMap.put(tp3, Collections.singletonList(new ConsumerRecord("foo", 3, 0, null, "bar")));
		ConsumerRecords allRecords = new ConsumerRecords<>(allRecordMap, Map.of());
		List<TopicPartition> afterRevokeAssignments = List.of(tp1, tp3);
		Map<TopicPartition, List<ConsumerRecord<String, String>>> afterRevokeRecordMap = new HashMap<>();
		afterRevokeRecordMap.put(tp1, Collections.singletonList(new ConsumerRecord("foo", 1, 0, null, "bar")));
		afterRevokeRecordMap.put(tp3, Collections.singletonList(new ConsumerRecord("foo", 3, 0, null, "bar")));
		ConsumerRecords afterRevokeRecords = new ConsumerRecords<>(afterRevokeRecordMap, Map.of());
		AtomicInteger pollPhase = new AtomicInteger();

		Consumer consumer = mock(Consumer.class);
		AtomicReference<ConsumerRebalanceListener> rebal = new AtomicReference<>();
		CountDownLatch subscribeLatch = new CountDownLatch(1);
		willAnswer(invocation -> {
			rebal.set(invocation.getArgument(1));
			subscribeLatch.countDown();
			return null;
		}).given(consumer).subscribe(any(Collection.class), any());
		CountDownLatch pauseLatch = new CountDownLatch(1);
		AtomicBoolean paused = new AtomicBoolean();
		willAnswer(inv -> {
			paused.set(true);
			pauseLatch.countDown();
			return null;
		}).given(consumer).pause(any());
		CountDownLatch resumeLatch = new CountDownLatch(1);
		willAnswer(inv -> {
			paused.set(false);
			resumeLatch.countDown();
			return null;
		}).given(consumer).resume(any());
		ConsumerFactory cf = mock(ConsumerFactory.class);
		given(cf.createConsumer(any(), any(), any(), any())).willReturn(consumer);
		given(cf.getConfigurationProperties())
				.willReturn(Collections.singletonMap(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest"));
		ContainerProperties containerProperties = new ContainerProperties("foo");
		containerProperties.setGroupId("grp");
		containerProperties.setMessageListener((MessageListener) rec -> {
			throw new RuntimeException("test");
		});
		ConcurrentMessageListenerContainer container = new ConcurrentMessageListenerContainer(cf,
				containerProperties);
		container.setCommonErrorHandler(new CommonErrorHandler() {

			@Override
			public boolean seeksAfterHandling() {
				return false; // pause and use remainingRecords
			}

			@Override
			public boolean handleOne(Exception thrownException, ConsumerRecord<?, ?> record, Consumer<?, ?> consumer,
					MessageListenerContainer container) {

				return false; // not handled
			}

		});
		CountDownLatch pollLatch = new CountDownLatch(2);
		CountDownLatch rebalLatch = new CountDownLatch(1);
		CountDownLatch continueLatch = new CountDownLatch(1);
		willAnswer(inv -> {
			Thread.sleep(50);
			pollLatch.countDown();
			switch (pollPhase.getAndIncrement()) {
				case 0 -> {
					rebal.get().onPartitionsAssigned(allAssignments);
					return allRecords;
				}
				case 1 -> {
					rebal.get().onPartitionsRevoked(allAssignments);
					rebal.get().onPartitionsAssigned(afterRevokeAssignments);
					rebalLatch.countDown();
					continueLatch.await(10, TimeUnit.SECONDS);
					if (paused.get()) {
						return ConsumerRecords.empty();
					}
					return afterRevokeRecords;
				}
				default -> {
					if (paused.get()) {
						return ConsumerRecords.empty();
					}
					return afterRevokeRecords;
				}
			}
		}).given(consumer).poll(any());
		container.start();
		assertThat(subscribeLatch.await(10, TimeUnit.SECONDS)).isTrue();
		KafkaMessageListenerContainer child = (KafkaMessageListenerContainer) KafkaTestUtils
				.getPropertyValue(container, "containers", List.class).get(0);
		assertThat(pollLatch.await(10, TimeUnit.SECONDS)).isTrue();
		assertThat(pauseLatch.await(10, TimeUnit.SECONDS)).isTrue();
		assertThat(rebalLatch.await(10, TimeUnit.SECONDS)).isTrue();
		ConsumerRecords remaining = KafkaTestUtils.getPropertyValue(child, "listenerConsumer.remainingRecords",
				ConsumerRecords.class);
		assertThat(remaining).isNull();
		continueLatch.countDown();
		assertThat(resumeLatch.await(10, TimeUnit.SECONDS)).isTrue();
		// no pause when re-assigned because all revoked and remainingRecords == null
		verify(consumer).pause(any());
		verify(consumer).resume(any());
		container.stop();
	}

	@SuppressWarnings({ "unchecked", "rawtypes" })
	@Test
	void pruneRevokedPartitionsFromRemainingRecordsWhenSeekAfterErrorFalseCoopAssignor() throws InterruptedException {
		TopicPartition tp0 = new TopicPartition("foo", 0);
		TopicPartition tp1 = new TopicPartition("foo", 1);
		TopicPartition tp2 = new TopicPartition("foo", 2);
		TopicPartition tp3 = new TopicPartition("foo", 3);
		List<TopicPartition> allAssignments = List.of(tp0, tp1, tp2, tp3);
		Map<TopicPartition, List<ConsumerRecord<String, String>>> allRecordMap = new LinkedHashMap<>();
		ConsumerRecord record0 = new ConsumerRecord("foo", 0, 0, null, "bar");
		ConsumerRecord record1 = new ConsumerRecord("foo", 1, 0, null, "bar");
		allRecordMap.put(tp0, Collections.singletonList(record0));
		allRecordMap.put(tp1, Collections.singletonList(record1));
		allRecordMap.put(tp2, Collections.singletonList(new ConsumerRecord("foo", 2, 0, null, "bar")));
		allRecordMap.put(tp3, Collections.singletonList(new ConsumerRecord("foo", 3, 0, null, "bar")));
		ConsumerRecords allRecords = new ConsumerRecords<>(allRecordMap, Map.of());
		List<TopicPartition> revokedAssignments = List.of(tp0, tp2);
		AtomicInteger pollPhase = new AtomicInteger();

		Consumer consumer = mock(Consumer.class);
		AtomicReference<ConsumerRebalanceListener> rebal = new AtomicReference<>();
		CountDownLatch subscribeLatch = new CountDownLatch(1);
		willAnswer(invocation -> {
			rebal.set(invocation.getArgument(1));
			subscribeLatch.countDown();
			return null;
		}).given(consumer).subscribe(any(Collection.class), any());
		CountDownLatch pauseLatch = new CountDownLatch(1);
		willAnswer(inv -> {
			pauseLatch.countDown();
			return null;
		}).given(consumer).pause(any());
		ConsumerFactory cf = mock(ConsumerFactory.class);
		given(cf.createConsumer(any(), any(), any(), any())).willReturn(consumer);
		given(cf.getConfigurationProperties())
				.willReturn(Collections.singletonMap(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest"));
		ContainerProperties containerProperties = new ContainerProperties("foo");
		containerProperties.setGroupId("grp");
		List<ConsumerRecord> recordsDelivered = new ArrayList<>();
		CountDownLatch consumeLatch = new CountDownLatch(3);
		containerProperties.setMessageListener((MessageListener) rec -> {
			recordsDelivered.add((ConsumerRecord) rec);
			consumeLatch.countDown();
			throw new RuntimeException("test");
		});
		ConcurrentMessageListenerContainer container = new ConcurrentMessageListenerContainer(cf,
				containerProperties);
		container.setCommonErrorHandler(new CommonErrorHandler() {

			@Override
			public boolean seeksAfterHandling() {
				return false; // pause and use remainingRecords
			}

			@Override
			public boolean handleOne(Exception thrownException, ConsumerRecord<?, ?> record, Consumer<?, ?> consumer,
					MessageListenerContainer container) {

				return false; // not handled
			}

		});
		CountDownLatch pollLatch = new CountDownLatch(2);
		CountDownLatch rebalLatch = new CountDownLatch(1);
		CountDownLatch continueLatch = new CountDownLatch(1);
		willAnswer(inv -> {
			Thread.sleep(50);
			pollLatch.countDown();
			switch (pollPhase.getAndIncrement()) {
				case 0 -> {
					rebal.get().onPartitionsAssigned(allAssignments);
					return allRecords;
				}
				case 1 -> {
					rebal.get().onPartitionsRevoked(revokedAssignments);
					rebal.get().onPartitionsAssigned(Collections.emptyList());
					rebalLatch.countDown();
					continueLatch.await(10, TimeUnit.SECONDS);
					return ConsumerRecords.empty();
				}
				default -> {
					return ConsumerRecords.empty();
				}
			}
		}).given(consumer).poll(any());
		container.start();
		assertThat(subscribeLatch.await(10, TimeUnit.SECONDS)).isTrue();
		KafkaMessageListenerContainer child = (KafkaMessageListenerContainer) KafkaTestUtils
				.getPropertyValue(container, "containers", List.class).get(0);
		assertThat(pollLatch.await(10, TimeUnit.SECONDS)).isTrue();
		assertThat(pauseLatch.await(10, TimeUnit.SECONDS)).isTrue();
		assertThat(rebalLatch.await(10, TimeUnit.SECONDS)).isTrue();
		ConsumerRecords remaining = KafkaTestUtils.getPropertyValue(child, "listenerConsumer.remainingRecords",
				ConsumerRecords.class);
		assertThat(remaining.count()).isEqualTo(2);
		assertThat(remaining.partitions()).contains(tp1, tp3);
		continueLatch.countDown();
		verify(consumer, atLeastOnce()).pause(any());
		verify(consumer, never()).resume(any());
		assertThat(consumeLatch.await(10, TimeUnit.SECONDS)).isTrue();
		container.stop();
		assertThat(recordsDelivered).hasSizeGreaterThanOrEqualTo(3);
		// partitions 0, 2 revoked during second poll.
		assertThat(recordsDelivered.get(0)).isEqualTo(record0);
		assertThat(recordsDelivered.get(1)).isEqualTo(record1);
		assertThat(recordsDelivered.get(2)).isEqualTo(record1);
	}

	@Test
	@SuppressWarnings("unchecked")
	void ignoreAsyncAcksFromBeforeRebalanceAfterRedelivery() throws InterruptedException {
		TopicPartition partition = new TopicPartition("foo", 0);
		Consumer<String, String> consumer = mock();
		ConsumerFactory<String, String> factory = mock();
		given(factory.createConsumer(any(), any(), any(), any())).willReturn(consumer);
		given(factory.getConfigurationProperties()).willReturn(Map.of());
		given(consumer.assignment()).willReturn(Set.of(partition));
		AtomicReference<ConsumerRebalanceListener> rebalance = new AtomicReference<>();
		willAnswer(invocation -> {
			rebalance.set(invocation.getArgument(1));
			return null;
		}).given(consumer).subscribe(any(Collection.class), any());
		Map<String, Acknowledgment> acknowledgments = new ConcurrentHashMap<>();
		CountDownLatch redelivered = new CountDownLatch(3);
		CountDownLatch finish = new CountDownLatch(1);
		ContainerProperties properties = new ContainerProperties("foo");
		properties.setGroupId("async-rebalance");
		properties.setAckMode(AckMode.MANUAL);
		properties.setAsyncAcks(true);
		properties.setAssignmentCommitOption(AssignmentCommitOption.NEVER);
		properties.setMessageListener((AcknowledgingMessageListener<String, String>) (record, ack) -> {
			acknowledgments.put(record.value() + record.offset(), ack);
			if (record.value().equals("new")) {
				redelivered.countDown();
			}
		});
		AtomicInteger polls = new AtomicInteger();
		willAnswer(invocation -> {
			int poll = polls.getAndIncrement();
			if (poll == 0) {
				rebalance.get().onPartitionsAssigned(List.of(partition));
			}
			else if (poll == 1) {
				rebalance.get().onPartitionsRevoked(List.of(partition));
				rebalance.get().onPartitionsAssigned(List.of(partition));
			}
			else {
				finish.await(10, TimeUnit.SECONDS);
				return ConsumerRecords.empty();
			}
			String value = poll == 0 ? "old" : "new";
			return new ConsumerRecords<>(Map.of(partition, List.of(
					new ConsumerRecord<>("foo", 0, 0L, 0L, TimestampType.NO_TIMESTAMP_TYPE,
							0, 0, "key", value, new RecordHeaders(), Optional.of(poll == 0 ? 7 : 9)),
					new ConsumerRecord<>("foo", 0, 1L, 0L, TimestampType.NO_TIMESTAMP_TYPE,
							0, 0, "key", value, new RecordHeaders(), Optional.of(poll == 0 ? 7 : 9)),
					new ConsumerRecord<>("foo", 0, 2L, 0L, TimestampType.NO_TIMESTAMP_TYPE,
							0, 0, "key", value, new RecordHeaders(), Optional.of(poll == 0 ? 7 : 9)))), Map.of());
		}).given(consumer).poll(any());
		KafkaMessageListenerContainer<String, String> container = new KafkaMessageListenerContainer<>(factory, properties);
		container.start();
		try {
			assertThat(redelivered.await(10, TimeUnit.SECONDS)).isTrue();
			acknowledgments.get("old2").acknowledge();
			acknowledgments.get("new2").acknowledge();
			acknowledgments.get("old1").acknowledge();
			acknowledgments.get("new1").acknowledge();
			acknowledgments.get("old0").acknowledge();
			Map<?, ?> offsets = KafkaTestUtils.getPropertyValue(container, "listenerConsumer.offsetsInThisBatch", Map.class);
			assertThat(offsets.get(partition)).isEqualTo(List.of(0L, 1L, 2L));
			verify(consumer, never()).commitSync(anyMap(), any());
			acknowledgments.get("new0").acknowledge();
			assertThat(offsets).isEmpty();
			assertThat(KafkaTestUtils.getPropertyValue(container, "listenerConsumer.deferredOffsets", Map.class)).isEmpty();
		}
		finally {
			finish.countDown();
			container.stop();
		}

		verify(consumer).commitSync(Map.of(partition, new OffsetAndMetadata(3, Optional.of(9), "")),
				Duration.ofSeconds(60));
		verify(consumer, times(1)).commitSync(anyMap(), any());
	}

	@SuppressWarnings({ "unchecked", "rawtypes" })
	@Test
	void pruneRevokedPartitionsFromPendingOutOfOrderCommitsLegacyAssignor() throws InterruptedException {
		TopicPartition tp0 = new TopicPartition("foo", 0);
		TopicPartition tp1 = new TopicPartition("foo", 1);
		List<TopicPartition> allAssignments = List.of(tp0, tp1);
		Map<TopicPartition, List<ConsumerRecord<String, String>>> allRecordMap = new HashMap<>();
		allRecordMap.put(tp0,
				List.of(new ConsumerRecord("foo", 0, 0, null, "bar"), new ConsumerRecord("foo", 0, 1, null, "bar")));
		allRecordMap.put(tp1,
				List.of(new ConsumerRecord("foo", 1, 0, null, "bar"), new ConsumerRecord("foo", 1, 1, null, "bar")));
		ConsumerRecords allRecords = new ConsumerRecords<>(allRecordMap, Map.of());
		List<TopicPartition> afterRevokeAssignments = List.of(tp1);
		AtomicInteger pollPhase = new AtomicInteger();

		Consumer consumer = mock(Consumer.class);
		AtomicReference<ConsumerRebalanceListener> rebal = new AtomicReference<>();
		CountDownLatch subscribeLatch = new CountDownLatch(1);
		willAnswer(invocation -> {
			rebal.set(invocation.getArgument(1));
			subscribeLatch.countDown();
			return null;
		}).given(consumer).subscribe(any(Collection.class), any());
		CountDownLatch pauseLatch = new CountDownLatch(1);
		willAnswer(inv -> {
			pauseLatch.countDown();
			return null;
		}).given(consumer).pause(any());
		ConsumerFactory cf = mock(ConsumerFactory.class);
		given(cf.createConsumer(any(), any(), any(), any())).willReturn(consumer);
		given(cf.getConfigurationProperties())
				.willReturn(Collections.singletonMap(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest"));
		ContainerProperties containerProperties = new ContainerProperties("foo");
		containerProperties.setGroupId("grp");
		containerProperties.setAckMode(AckMode.MANUAL);
		containerProperties.setMessageListener(ackOffset1());
		containerProperties.setAsyncAcks(true);
		ConcurrentMessageListenerContainer container = new ConcurrentMessageListenerContainer(cf,
				containerProperties);
		CountDownLatch pollLatch = new CountDownLatch(2);
		CountDownLatch rebalLatch = new CountDownLatch(1);
		CountDownLatch continueLatch = new CountDownLatch(1);
		willAnswer(inv -> {
			Thread.sleep(50);
			pollLatch.countDown();
			switch (pollPhase.getAndIncrement()) {
				case 0 -> {
					rebal.get().onPartitionsAssigned(allAssignments);
					return allRecords;
				}
				case 1 -> {
					rebal.get().onPartitionsRevoked(allAssignments);
					rebal.get().onPartitionsAssigned(afterRevokeAssignments);
					rebalLatch.countDown();
					continueLatch.await(10, TimeUnit.SECONDS);
					return ConsumerRecords.empty();
				}
				default -> {
					return ConsumerRecords.empty();
				}
			}
		}).given(consumer).poll(any());
		container.start();
		assertThat(subscribeLatch.await(10, TimeUnit.SECONDS)).isTrue();
		KafkaMessageListenerContainer child = (KafkaMessageListenerContainer) KafkaTestUtils
				.getPropertyValue(container, "containers", List.class).get(0);
		assertThat(pollLatch.await(10, TimeUnit.SECONDS)).isTrue();
		assertThat(pauseLatch.await(10, TimeUnit.SECONDS)).isTrue();
		assertThat(rebalLatch.await(10, TimeUnit.SECONDS)).isTrue();
		Map offsets = KafkaTestUtils.getPropertyValue(child, "listenerConsumer.offsetsInThisBatch", Map.class);
		assertThat(offsets).hasSize(0);
		assertThat(KafkaTestUtils.getPropertyValue(child, "listenerConsumer.consumerPaused", Boolean.class)).isFalse();
		continueLatch.countDown();
		// no pause when re-assigned because all revoked
		verify(consumer).pause(any());
		verify(consumer, never()).resume(any());
		container.stop();
	}

	@SuppressWarnings({ "unchecked", "rawtypes" })
	@Test
	void pruneRevokedPartitionsFromPendingOutOfOrderCommitsCoopAssignor() throws InterruptedException {
		TopicPartition tp0 = new TopicPartition("foo", 0);
		TopicPartition tp1 = new TopicPartition("foo", 1);
		List<TopicPartition> allAssignments = List.of(tp0, tp1);
		Map<TopicPartition, List<ConsumerRecord<Integer, String>>> allRecordMap = new HashMap<>();
		allRecordMap.put(tp0,
				List.of(epochRecord(tp0, 0, Optional.of(7)), epochRecord(tp0, 1, Optional.of(8))));
		allRecordMap.put(tp1,
				List.of(epochRecord(tp1, 0, Optional.of(9)), epochRecord(tp1, 1, Optional.of(10))));
		ConsumerRecords allRecords = new ConsumerRecords<>(allRecordMap, Map.of());
		AtomicInteger pollPhase = new AtomicInteger();

		Consumer consumer = mock(Consumer.class);
		AtomicReference<ConsumerRebalanceListener> rebal = new AtomicReference<>();
		CountDownLatch subscribeLatch = new CountDownLatch(1);
		willAnswer(invocation -> {
			rebal.set(invocation.getArgument(1));
			subscribeLatch.countDown();
			return null;
		}).given(consumer).subscribe(any(Collection.class), any());
		CountDownLatch pauseLatch = new CountDownLatch(1);
		willAnswer(inv -> {
			pauseLatch.countDown();
			return null;
		}).given(consumer).pause(any());
		ConsumerFactory cf = mock(ConsumerFactory.class);
		given(cf.createConsumer(any(), any(), any(), any())).willReturn(consumer);
		given(cf.getConfigurationProperties())
				.willReturn(Collections.singletonMap(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest"));
		ContainerProperties containerProperties = new ContainerProperties("foo");
		containerProperties.setGroupId("grp");
		containerProperties.setAckMode(AckMode.MANUAL);
		AtomicReference<Acknowledgment> retainedPrefix = new AtomicReference<>();
		containerProperties.setMessageListener((AcknowledgingMessageListener<Integer, String>) (record, ack) -> {
			if (record.partition() == 1 && record.offset() == 0) {
				retainedPrefix.set(ack);
			}
			if (record.offset() == 1) {
				ack.acknowledge();
			}
		});
		containerProperties.setAsyncAcks(true);
		ConcurrentMessageListenerContainer container = new ConcurrentMessageListenerContainer(cf,
				containerProperties);
		CountDownLatch committedLatch = new CountDownLatch(1);
		willAnswer(inv -> {
			committedLatch.countDown();
			return null;
		}).given(consumer).commitSync(anyMap(), any(Duration.class));
		CountDownLatch pollLatch = new CountDownLatch(2);
		CountDownLatch rebalLatch = new CountDownLatch(1);
		CountDownLatch continueLatch = new CountDownLatch(1);
		willAnswer(inv -> {
			Thread.sleep(50);
			pollLatch.countDown();
			switch (pollPhase.getAndIncrement()) {
				case 0 -> {
					rebal.get().onPartitionsAssigned(allAssignments);
					return allRecords;
				}
				case 1 -> {
					rebal.get().onPartitionsRevoked(List.of(tp0));
					rebal.get().onPartitionsAssigned(List.of(new TopicPartition("foo", 2)));
					rebalLatch.countDown();
					continueLatch.await(10, TimeUnit.SECONDS);
					retainedPrefix.get().acknowledge();
					return ConsumerRecords.empty();
				}
				default -> {
					return ConsumerRecords.empty();
				}
			}
		}).given(consumer).poll(any());
		container.start();
		assertThat(subscribeLatch.await(10, TimeUnit.SECONDS)).isTrue();
		KafkaMessageListenerContainer child = (KafkaMessageListenerContainer) KafkaTestUtils
				.getPropertyValue(container, "containers", List.class).get(0);
		assertThat(pollLatch.await(10, TimeUnit.SECONDS)).isTrue();
		assertThat(pauseLatch.await(10, TimeUnit.SECONDS)).isTrue();
		assertThat(rebalLatch.await(10, TimeUnit.SECONDS)).isTrue();
		Map offsets = KafkaTestUtils.getPropertyValue(child, "listenerConsumer.offsetsInThisBatch", Map.class);
		assertThat(offsets).hasSize(1);
		assertThat(offsets.get(tp1)).isNotNull();
		assertThat(KafkaTestUtils.getPropertyValue(child, "listenerConsumer.consumerPaused", Boolean.class)).isTrue();
		verify(consumer, times(2)).pause(any());
		verify(consumer, never()).resume(any());
		verify(consumer, never()).commitSync(anyMap(), any(Duration.class));
		continueLatch.countDown();
		assertThat(committedLatch.await(10, TimeUnit.SECONDS)).isTrue();
		container.stop();
		verify(consumer).commitSync(Map.of(tp1, new OffsetAndMetadata(2, Optional.of(10), "")),
				Duration.ofSeconds(60));
		verify(consumer, times(1)).commitSync(anyMap(), any(Duration.class));
	}

	@SuppressWarnings({ "unchecked", "rawtypes" })
	@Test
	void resumeRetainedPartitionsWhenCoopRevokeClearsPendingOffsets() throws InterruptedException {
		TopicPartition tp0 = new TopicPartition("coopRevoke", 0);
		TopicPartition tp1 = new TopicPartition("coopRevoke", 1);
		List<TopicPartition> allAssignments = List.of(tp0, tp1);
		Map<TopicPartition, List<ConsumerRecord<String, String>>> allRecordMap = new HashMap<>();
		allRecordMap.put(tp0,
				List.of(new ConsumerRecord("coopRevoke", 0, 0, null, "payload"),
						new ConsumerRecord("coopRevoke", 0, 1, null, "payload")));
		ConsumerRecords allRecords = new ConsumerRecords<>(allRecordMap, Map.of());
		AtomicInteger pollPhase = new AtomicInteger();

		Consumer<String, String> consumer = mock();
		AtomicReference<ConsumerRebalanceListener> rebal = new AtomicReference<>();
		CountDownLatch subscribeLatch = new CountDownLatch(1);
		willAnswer(invocation -> {
			rebal.set(invocation.getArgument(1));
			subscribeLatch.countDown();
			return null;
		}).given(consumer).subscribe(any(Collection.class), any());
		CountDownLatch pauseLatch = new CountDownLatch(1);
		willAnswer(inv -> {
			pauseLatch.countDown();
			return null;
		}).given(consumer).pause(any());
		// a cooperative assignor leaves the retained partition paused across the rebalance
		given(consumer.paused()).willReturn(Set.of(tp1));
		CountDownLatch resumeLatch = new CountDownLatch(1);
		List<Collection<TopicPartition>> resumed = new ArrayList<>();
		willAnswer(inv -> {
			resumed.add(inv.getArgument(0));
			resumeLatch.countDown();
			return null;
		}).given(consumer).resume(any());
		ConsumerFactory<String, String> cf = mock();
		given(cf.createConsumer(any(), any(), any(), any())).willReturn(consumer);
		given(cf.getConfigurationProperties())
				.willReturn(Collections.singletonMap(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest"));
		ContainerProperties containerProperties = new ContainerProperties("coopRevoke");
		containerProperties.setGroupId("grp");
		containerProperties.setAckMode(AckMode.MANUAL);
		containerProperties.setMessageListener(ackOffset1());
		containerProperties.setAsyncAcks(true);
		ConcurrentMessageListenerContainer container = new ConcurrentMessageListenerContainer(cf,
				containerProperties);
		CountDownLatch rebalLatch = new CountDownLatch(1);
		CountDownLatch afterResumeLatch = new CountDownLatch(1);
		willAnswer(inv -> {
			Thread.sleep(50);
			switch (pollPhase.getAndIncrement()) {
				case 0 -> {
					rebal.get().onPartitionsAssigned(allAssignments);
					return allRecords;
				}
				case 1 -> {
					// only tp0 moves; the pending offset on it is dropped, tp1 stays with this member
					rebal.get().onPartitionsRevoked(List.of(tp0));
					rebal.get().onPartitionsAssigned(List.of());
					rebalLatch.countDown();
					return ConsumerRecords.empty();
				}
				default -> {
					if (!resumed.isEmpty()) {
						// the resume happens after the poll returns, so the state it updates is
						// only settled once the consumer thread comes back round to the next poll
						afterResumeLatch.countDown();
					}
					return ConsumerRecords.empty();
				}
			}
		}).given(consumer).poll(any());
		container.start();
		assertThat(subscribeLatch.await(10, TimeUnit.SECONDS)).isTrue();
		KafkaMessageListenerContainer child = (KafkaMessageListenerContainer) KafkaTestUtils
				.getPropertyValue(container, "containers", List.class).get(0);
		assertThat(pauseLatch.await(10, TimeUnit.SECONDS)).isTrue();
		assertThat(rebalLatch.await(10, TimeUnit.SECONDS)).isTrue();
		assertThat(resumeLatch.await(10, TimeUnit.SECONDS)).isTrue();
		assertThat(afterResumeLatch.await(10, TimeUnit.SECONDS)).isTrue();
		assertThat(resumed.get(0)).contains(tp1);
		Map offsets = KafkaTestUtils.getPropertyValue(child, "listenerConsumer.offsetsInThisBatch", Map.class);
		assertThat(offsets).isEmpty();
		assertThat(KafkaTestUtils.getPropertyValue(child, "listenerConsumer.consumerPaused", Boolean.class)).isFalse();
		container.stop();
	}

	@SuppressWarnings({ "unchecked", "rawtypes" })
	@Test
	void removeOffsetsInBatchForRetryRecords() throws InterruptedException {
		String retryTopic = "retry-offsets-topic";
		TopicPartition topicPartition = new TopicPartition(retryTopic, 0);
		Map<TopicPartition, List<ConsumerRecord<String, String>>> recordMap = new LinkedHashMap<>();
		recordMap.put(topicPartition,
				List.of(new ConsumerRecord<>(retryTopic, 0, 0, null, "failed-record-0"),
						new ConsumerRecord<>(retryTopic, 0, 1, null, "failed-record-1")));
		ConsumerRecords<String, String> polledRecords = new ConsumerRecords<>(recordMap, Map.of());
		AtomicInteger pollCount = new AtomicInteger();

		Consumer<String, String> consumer = mock();
		AtomicReference<ConsumerRebalanceListener> rebal = new AtomicReference<>();
		CountDownLatch subscribeLatch = new CountDownLatch(1);
		willAnswer(invocation -> {
			rebal.set(invocation.getArgument(1));
			subscribeLatch.countDown();
			return null;
		}).given(consumer).subscribe(any(Collection.class), any());
		willAnswer(invocation -> {
			if (pollCount.getAndIncrement() == 0) {
				rebal.get().onPartitionsAssigned(List.of(topicPartition));
				return polledRecords;
			}
			return ConsumerRecords.empty();
		}).given(consumer).poll(any());
		ConsumerFactory<String, String> cf = mock();
		given(cf.createConsumer(any(), any(), any(), any())).willReturn(consumer);
		given(cf.getConfigurationProperties())
				.willReturn(Collections.singletonMap(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest"));
		ContainerProperties containerProperties = new ContainerProperties(retryTopic);
		containerProperties.setGroupId("grp");
		containerProperties.setAckMode(AckMode.MANUAL);
		containerProperties.setAsyncAcks(true);
		containerProperties.setMessageListener((MessageListener) rec -> {
			throw new RuntimeException("test");
		});
		ConcurrentMessageListenerContainer<String, String> container = new ConcurrentMessageListenerContainer<>(cf,
				containerProperties);
		CountDownLatch handleRemainingLatch = new CountDownLatch(1);
		container.setCommonErrorHandler(new CommonErrorHandler() {

			@Override
			public boolean seeksAfterHandling() {
				return true;
			}

			@Override
			public void handleRemaining(Exception thrownException, List<ConsumerRecord<?, ?>> failedRecords,
					Consumer<?, ?> kafkaConsumer, MessageListenerContainer listenerContainer) {

				handleRemainingLatch.countDown();
				throw new RecordInRetryException("retrying", thrownException);
			}

		});
		container.start();
		try {
			assertThat(subscribeLatch.await(10, TimeUnit.SECONDS)).isTrue();
			assertThat(handleRemainingLatch.await(10, TimeUnit.SECONDS)).isTrue();
			KafkaMessageListenerContainer<String, String> child = (KafkaMessageListenerContainer<String, String>) KafkaTestUtils
					.getPropertyValue(container, "containers", List.class).get(0);
			Map<?, ?> offsets = null;
			Map<?, ?> deferred = null;
			for (int i = 0; i < 20; i++) {
				offsets = KafkaTestUtils.getPropertyValue(child, "listenerConsumer.offsetsInThisBatch", Map.class);
				deferred = KafkaTestUtils.getPropertyValue(child, "listenerConsumer.deferredOffsets", Map.class);
				if (CollectionUtils.isEmpty(offsets) && CollectionUtils.isEmpty(deferred)) {
					break;
				}
				Thread.sleep(50);
			}
			assertThat(offsets).isNullOrEmpty();
			assertThat(deferred).isNullOrEmpty();
		}
		finally {
			container.stop();
		}
	}

	@SuppressWarnings("unchecked")
	@Test
	void removeOffsetsInBatchHoldsListenerConsumerMonitor() throws InterruptedException {
		String topic = "remove-offsets-monitor";
		TopicPartition partition = new TopicPartition(topic, 0);
		ConsumerRecords<String, String> polledRecords = new ConsumerRecords<>(Map.of(partition, List.of(
				new ConsumerRecord<>(topic, 0, 0, null, "async"),
				new ConsumerRecord<>(topic, 0, 1, null, "retry"))), Map.of());
		AtomicInteger pollCount = new AtomicInteger();

		Consumer<String, String> consumer = mock();
		given(consumer.assignment()).willReturn(Set.of(partition));
		AtomicReference<ConsumerRebalanceListener> rebal = new AtomicReference<>();
		willAnswer(invocation -> {
			rebal.set(invocation.getArgument(1));
			return null;
		}).given(consumer).subscribe(any(Collection.class), any());
		willAnswer(invocation -> {
			if (pollCount.getAndIncrement() == 0) {
				rebal.get().onPartitionsAssigned(List.of(partition));
				return polledRecords;
			}
			Thread.sleep(10);
			return ConsumerRecords.empty();
		}).given(consumer).poll(any());
		ConsumerFactory<String, String> cf = mock();
		given(cf.createConsumer(any(), any(), any(), any())).willReturn(consumer);
		given(cf.getConfigurationProperties())
				.willReturn(Collections.singletonMap(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest"));
		ContainerProperties containerProperties = new ContainerProperties(topic);
		containerProperties.setGroupId("grp");
		containerProperties.setAckMode(AckMode.MANUAL);
		containerProperties.setAsyncAcks(true);
		AtomicReference<Acknowledgment> pendingAck = new AtomicReference<>();
		AtomicReference<Thread> consumerThread = new AtomicReference<>();
		containerProperties.setMessageListener((AcknowledgingMessageListener<String, String>) (rec, ack) -> {
			consumerThread.set(Thread.currentThread());
			if (rec.offset() == 0) {
				// completed later, on another thread
				pendingAck.set(ack);
			}
			else {
				throw new RuntimeException("retry");
			}
		});
		KafkaMessageListenerContainer<String, String> container =
				new KafkaMessageListenerContainer<>(cf, containerProperties);
		CountDownLatch inErrorHandler = new CountDownLatch(1);
		CountDownLatch monitorHeld = new CountDownLatch(1);
		container.setCommonErrorHandler(new CommonErrorHandler() {

			@Override
			public boolean seeksAfterHandling() {
				return true;
			}

			@Override
			public void handleRemaining(Exception thrownException, List<ConsumerRecord<?, ?>> records,
					Consumer<?, ?> kafkaConsumer, MessageListenerContainer listenerContainer) {

				inErrorHandler.countDown();
				try {
					monitorHeld.await(10, TimeUnit.SECONDS);
				}
				catch (InterruptedException ex) {
					Thread.currentThread().interrupt();
				}
				throw new RecordInRetryException("retrying", thrownException);
			}

		});
		container.start();
		try {
			assertThat(inErrorHandler.await(10, TimeUnit.SECONDS)).isTrue();
			Object listenerConsumer = KafkaTestUtils.getPropertyValue(container, "listenerConsumer");
			Map<TopicPartition, List<Long>> offsets =
					KafkaTestUtils.getPropertyValue(container, "listenerConsumer.offsetsInThisBatch", Map.class);
			// stand in for an ack completing at the same moment: hold the monitor that ackInOrder() uses
			synchronized (listenerConsumer) {
				monitorHeld.countDown();
				for (int i = 0; i < 200 && consumerThread.get().getState() != Thread.State.BLOCKED; i++) {
					Thread.sleep(50);
				}
				assertThat(consumerThread.get().getState()).isEqualTo(Thread.State.BLOCKED);
				assertThat(offsets.get(partition)).containsExactly(0L, 1L);
			}
			for (int i = 0; i < 200 && offsets.get(partition).size() > 1; i++) {
				Thread.sleep(50);
			}
			assertThat(offsets.get(partition)).containsExactly(0L);
			pendingAck.get().acknowledge();
			assertThat(offsets).isEmpty();
			assertThat(KafkaTestUtils.getPropertyValue(container, "listenerConsumer.deferredOffsets", Map.class))
					.isEmpty();
			verify(consumer, timeout(10_000)).commitSync(eq(Map.of(partition, new OffsetAndMetadata(1L))), any());
		}
		finally {
			container.stop();
		}
	}

	@SuppressWarnings("rawtypes")
	private AcknowledgingMessageListener ackOffset1() {
		return new AcknowledgingMessageListener() {

			@Override
			public void onMessage(ConsumerRecord rec, @Nullable Acknowledgment ack) {
				if (rec.offset() == 1 && ack != null) {
					ack.acknowledge();
				}
			}

			@Override
			public void onMessage(Object data) {
			}

		};
	}

	@SuppressWarnings({ "rawtypes", "unchecked" })
	@Test
	void afterRollbackProcessorReceivesFullBatchWhenWantsPollResult() throws InterruptedException {
		// GH-4439
		ConsumerFactory consumerFactory = mock(ConsumerFactory.class);
		Consumer consumer = mock(Consumer.class);
		TopicPartition tp0 = new TopicPartition("foo", 0);
		ConsumerRecord record1 = new ConsumerRecord("foo", 0, 0L, "k", "v1");
		ConsumerRecord record2 = new ConsumerRecord("foo", 0, 1L, "k", "v2");
		ConsumerRecords records = new ConsumerRecords(
				Collections.singletonMap(tp0, List.of(record1, record2)), Map.of());
		ConsumerRecords empty = new ConsumerRecords(Collections.emptyMap(), Map.of());
		AtomicInteger pollCount = new AtomicInteger();
		willAnswer(invocation -> {
			Thread.sleep(10);
			return pollCount.incrementAndGet() < 2 ? records : empty;
		}).given(consumer).poll(any());
		willAnswer(invocation -> {
			((ConsumerRebalanceListener) invocation.getArgument(1))
					.onPartitionsAssigned(List.of(tp0));
			return null;
		}).given(consumer).subscribe(any(Collection.class), any());
		given(consumer.position(any())).willReturn(0L);
		given(consumerFactory.createConsumer("grp", "", "-0", KafkaTestUtils.defaultPropertyOverrides()))
				.willReturn(consumer);

		ContainerProperties containerProperties = new ContainerProperties("foo");
		containerProperties.setGroupId("grp");
		containerProperties.setMissingTopicsFatal(false);
		containerProperties.setMessageListener(new BatchMessageListener<String, String>() {

			@Override
			public boolean wantsPollResult() {
				return true;
			}

			@Override
			public void onMessage(List<ConsumerRecord<String, String>> data) {
				throw new UnsupportedOperationException("expected ConsumerRecords overload");
			}

			@Override
			public void onMessage(ConsumerRecords<String, String> recs, @Nullable Acknowledgment ack,
					Consumer<String, String> consumer) {
				throw new RuntimeException("listener fails on purpose");
			}

		});

		KafkaAwareTransactionManager tm = mock(KafkaAwareTransactionManager.class);
		ProducerFactory pf = mock(ProducerFactory.class);
		given(tm.getProducerFactory()).willReturn(pf);
		Producer producer = mock(Producer.class);
		given(pf.createProducer()).willReturn(producer);
		containerProperties.setKafkaAwareTransactionManager(tm);

		CountDownLatch rollbackLatch = new CountDownLatch(1);
		willAnswer(inv -> {
			TransactionSynchronizationManager.bindResource(pf,
					new KafkaResourceHolder<>(producer, Duration.ofSeconds(5L)));
			return null;
		}).given(tm).getTransaction(any());
		willAnswer(inv -> {
			TransactionSynchronizationManager.unbindResource(pf);
			return null;
		}).given(tm).commit(any());
		willAnswer(inv -> {
			TransactionSynchronizationManager.unbindResource(pf);
			rollbackLatch.countDown();
			return null;
		}).given(tm).rollback(any());

		AfterRollbackProcessor<String, String> afterRollbackProcessor = mock(AfterRollbackProcessor.class);

		ConcurrentMessageListenerContainer container = new ConcurrentMessageListenerContainer(
				consumerFactory, containerProperties);
		container.setAfterRollbackProcessor(afterRollbackProcessor);
		container.setBeanName("test-gh-4439");
		container.start();

		assertThat(rollbackLatch.await(10, TimeUnit.SECONDS)).isTrue();
		container.stop();

		ArgumentCaptor<List<ConsumerRecord<String, String>>> captor = ArgumentCaptor.forClass(List.class);
		verify(afterRollbackProcessor, atLeastOnce()).processBatch(any(ConsumerRecords.class), captor.capture(),
				any(Consumer.class), any(), any(Exception.class), anyBoolean(), any());
		assertThat(captor.getValue()).containsExactly(record1, record2);
	}

	@SuppressWarnings({ "rawtypes", "unchecked" })
	@Test
	@DisplayName("failedSyncForRecord: peekLast + identity across scenarios (GH-4504)")
	void failedSyncForRecordPeekLastIdentityAcrossScenarios() throws Exception {
		// Verifies the peekLast + identity check that replaces the size-delta
		// signal in doInvokeWithRecords. The size delta could grow from an
		// unrelated cross-partition async callback and mistag cRecord's
		// partition, stalling it until rebalance. See GH-4504.

		ConsumerFactory cf = mock(ConsumerFactory.class);
		Consumer consumer = mock(Consumer.class);
		ConsumerRecords empty = new ConsumerRecords<>(Collections.emptyMap(), Map.of());
		given(consumer.poll(any())).willReturn(empty);
		given(cf.createConsumer(any(), any(), any(), any())).willReturn(consumer);
		ContainerProperties props = new ContainerProperties("t");
		props.setGroupId("g");
		props.setMessageListener((MessageListener<String, String>) rec -> { });
		props.setMissingTopicsFatal(false);
		ConcurrentMessageListenerContainer container = new ConcurrentMessageListenerContainer(cf, props);
		container.start();
		try {
			KafkaMessageListenerContainer child = (KafkaMessageListenerContainer) KafkaTestUtils
					.getPropertyValue(container, "containers", List.class).get(0);
			Object listenerConsumer = null;
			long deadline = System.currentTimeMillis() + 10_000;
			while (listenerConsumer == null && System.currentTimeMillis() < deadline) {
				listenerConsumer = KafkaTestUtils.getPropertyValue(child, "listenerConsumer");
				if (listenerConsumer == null) {
					Thread.sleep(20);
				}
			}
			assertThat(listenerConsumer).isNotNull();

			Deque failedRecords = KafkaTestUtils.getPropertyValue(child,
					"listenerConsumer.failedRecords", Deque.class);

			Class<?> tupleClass = Class.forName(
					"org.springframework.kafka.listener.KafkaMessageListenerContainer$FailedRecordTuple");
			Constructor<?> tupleCtor = tupleClass.getDeclaredConstructors()[0];
			tupleCtor.setAccessible(true);

			Method helper = listenerConsumer.getClass().getDeclaredMethod(
					"failedSyncForRecord", ConsumerRecord.class);
			helper.setAccessible(true);

			RuntimeException ex = new RuntimeException("test");
			Observation obs = Observation.NOOP;

			ConsumerRecord<String, String> pA_off10 = new ConsumerRecord<>("t", 0, 10L, "kA", "vA");
			ConsumerRecord<String, String> pA_off10_dup = new ConsumerRecord<>("t", 0, 10L, "kA", "vA");
			ConsumerRecord<String, String> pB_off20 = new ConsumerRecord<>("t", 1, 20L, "kB", "vB");

			// 1. empty deque: peekLast is null → no sync failure
			failedRecords.clear();
			assertThat((Boolean) helper.invoke(listenerConsumer, pA_off10)).isFalse();

			// 2. race case: cross-partition tuple sits at the tail → must not
			//    tag pA_off10's partition
			failedRecords.addLast(tupleCtor.newInstance(pB_off20, ex, obs));
			assertThat((Boolean) helper.invoke(listenerConsumer, pA_off10)).isFalse();

			// 3. sync-failure case: cRecord's own tuple at the tail → detected
			failedRecords.clear();
			failedRecords.addLast(tupleCtor.newInstance(pA_off10, ex, obs));
			assertThat((Boolean) helper.invoke(listenerConsumer, pA_off10)).isTrue();

			// 4. acknowledged false negative: cRecord failed sync but an
			//    unrelated callback appended after it, so peekLast is no longer
			//    cRecord's tuple. handleAsyncFailure catches this next poll.
			failedRecords.addLast(tupleCtor.newInstance(pB_off20, ex, obs));
			assertThat((Boolean) helper.invoke(listenerConsumer, pA_off10)).isFalse();

			// 5. identity, not equality: a distinct ConsumerRecord instance
			//    with the same topic + partition + offset does not match
			failedRecords.clear();
			failedRecords.addLast(tupleCtor.newInstance(pA_off10_dup, ex, obs));
			assertThat((Boolean) helper.invoke(listenerConsumer, pA_off10)).isFalse();
		}
		finally {
			container.stop();
		}
	}

	public static class TestMessageListener1 implements MessageListener<String, String>, ConsumerSeekAware {

		private static final ThreadLocal<ConsumerSeekCallback> callbacks = new ThreadLocal<>();

		CountDownLatch latch = new CountDownLatch(1);

		@Override
		public void onMessage(ConsumerRecord<String, String> data) {
			ConsumerSeekCallback callback = callbacks.get();
			if (latch.getCount() > 0) {
				callback.seekRelative("foo", 0, -30, true);
				callback.seekRelative("foo", 1, 30, true);
				callback.seekRelative("foo", 2, -30, false);
				callback.seekRelative("foo", 3, 30, false);
			}
			this.latch.countDown();
		}

		@Override
		public void registerSeekCallback(ConsumerSeekCallback callback) {
			callbacks.set(callback);
		}

		@Override
		public void onIdleContainer(Map<TopicPartition, Long> assignments, ConsumerSeekCallback callback) {
			if (latch.getCount() > 0) {
				callback.seekRelative("foo", 0, -40, true);
				callback.seekRelative("foo", 1, 40, true);
				callback.seekRelative("foo", 2, -40, false);
				callback.seekRelative("foo", 3, 40, false);
			}
			this.latch.countDown();
		}

	}

	public static class TestMessageListener2 implements MessageListener<String, String>, ConsumerSeekAware {

		private static final ThreadLocal<ConsumerSeekCallback> callbacks = new ThreadLocal<>();

		CountDownLatch latch = new CountDownLatch(1);

		private Collection<TopicPartition> partitions;

		@Override
		public void onMessage(ConsumerRecord<String, String> data) {
			ConsumerSeekCallback callback = callbacks.get();
			if (latch.getCount() > 0) {
				callback.seekToTimestamp("foo", 0, 42L);
				callback.seekToTimestamp(this.partitions.stream()
						.filter(tp -> !tp.equals(new TopicPartition("foo", 0)))
						.collect(Collectors.toList()), 43L);
			}
			this.latch.countDown();
		}

		@Override
		public void registerSeekCallback(ConsumerSeekCallback callback) {
			callbacks.set(callback);
		}

		@Override
		public void onPartitionsAssigned(Map<TopicPartition, Long> assignments, ConsumerSeekCallback callback) {
			this.partitions = assignments.keySet();
		}

		@Override
		public void onIdleContainer(Map<TopicPartition, Long> assignments, ConsumerSeekCallback callback) {
			if (latch.getCount() > 0) {
				callback.seekToTimestamp("foo", 0, 42L);
				callback.seekToTimestamp(assignments.keySet(), 43L);
			}
			this.latch.countDown();
		}

	}

	public static class TestMessageListener3 implements MessageListener<String, String>, ConsumerSeekAware {

		private static final ThreadLocal<ConsumerSeekCallback> callbacks = new ThreadLocal<>();

		CountDownLatch latch = new CountDownLatch(2);

		@Override
		public void onMessage(ConsumerRecord<String, String> data) {

		}

		@Override
		public void registerSeekCallback(ConsumerSeekCallback callback) {
			callbacks.set(callback);
		}

		@Override
		public void onPartitionsAssigned(Map<TopicPartition, Long> assignments, ConsumerSeekCallback callback) {
			if (latch.getCount() > 0) {
				int absoluteTarget1 = 20;
				int absoluteTarget2 = 21;
				int absoluteTarget3 = 22;
				int absoluteTarget4 = 23;
				callback.seek("test-topic", 0, current -> current > absoluteTarget1 ? absoluteTarget1 : current);
				callback.seek("test-topic", 1, current -> current > absoluteTarget2 ? absoluteTarget2 : current);
				callback.seek("test-topic", 2, current -> current > absoluteTarget3 ? absoluteTarget3 : current);
				callback.seek("test-topic", 3, current -> current > absoluteTarget4 ? absoluteTarget4 : current);
			}
			this.latch.countDown();
		}

		@Override
		public void onIdleContainer(Map<TopicPartition, Long> assignments, ConsumerSeekCallback callback) {
			if (latch.getCount() > 0) {
				int absoluteTarget = 31;
				callback.seek("test-topic", 0, current -> current > absoluteTarget ? absoluteTarget : current);
				callback.seek("test-topic", 1, current -> current > absoluteTarget ? absoluteTarget : current);
				callback.seek("test-topic", 2, current -> current > absoluteTarget ? absoluteTarget : current);
				callback.seek("test-topic", 3, current -> current > absoluteTarget ? absoluteTarget : current);
			}
			this.latch.countDown();
		}

	}

	public static class TestMessageListener4 implements MessageListener<String, String>, ConsumerSeekAware {

		private static final ThreadLocal<ConsumerSeekCallback> callbacks = new ThreadLocal<>();

		CountDownLatch latch = new CountDownLatch(1);

		@Override
		public void onMessage(ConsumerRecord<String, String> data) {
			ConsumerSeekCallback callback = callbacks.get();
			if (latch.getCount() > 0) {

				int absoluteTarget1 = 20;
				int absoluteTarget2 = 21;
				int absoluteTarget3 = 22;
				int absoluteTarget4 = 23;

				callback.seek("test-topic", 0, current -> current > absoluteTarget1 ? absoluteTarget1 : current);
				callback.seek("test-topic", 1, current -> current > absoluteTarget2 ? absoluteTarget2 : current);
				callback.seek("test-topic", 2, current -> current > absoluteTarget3 ? absoluteTarget3 : current);
				callback.seek("test-topic", 3, current -> current > absoluteTarget4 ? absoluteTarget4 : current);
			}
			this.latch.countDown();
		}

		@Override
		public void registerSeekCallback(ConsumerSeekCallback callback) {
			callbacks.set(callback);
		}

	}

}
