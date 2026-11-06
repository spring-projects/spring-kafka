/*
 * Copyright 2019-present the original author or authors.
 */

package org.springframework.kafka.listener;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerGroupMetadata;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.common.record.TimestampType;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.ArgumentCaptor;
import org.mockito.InOrder;

import org.springframework.kafka.core.KafkaOperations;
import org.springframework.kafka.listener.ContainerProperties.EOSMode;
import org.springframework.kafka.support.serializer.DeserializationException;
import org.springframework.util.backoff.BackOff;
import org.springframework.util.backoff.BackOffExecution;
import org.springframework.util.backoff.FixedBackOff;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyMap;
import static org.mockito.BDDMockito.given;
import static org.mockito.BDDMockito.willAnswer;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;

/**
 * @author Gary Russell
 * @author Francois Rosiere
 * @author Wang Zhiyang
 * @author Soby Chacko
 *
 * @since 2.3.1
 *
 */
public class DefaultAfterRollbackProcessorTests {

	@ParameterizedTest
	@ValueSource(booleans = { false, true })
	void singleRecoveryPreservesLeaderEpoch(boolean nullContainer) {
		KafkaOperations<String, String> template = mock();
		given(template.isTransactional()).willReturn(true);
		ConsumerRecordRecoverer recoverer = mock();
		DefaultAfterRollbackProcessor<String, String> processor = new DefaultAfterRollbackProcessor<>(recoverer,
				new FixedBackOff(0, 0), template, true);
		Consumer<String, String> consumer = mock();
		ConsumerGroupMetadata group = new ConsumerGroupMetadata("group");
		given(consumer.groupMetadata()).willReturn(group);
		MessageListenerContainer container = mock();
		given(container.getContainerProperties()).willReturn(new ContainerProperties("topic"));
		ConsumerRecord<String, String> record = new ConsumerRecord<>("topic", 0, 41L, 0L,
				TimestampType.NO_TIMESTAMP_TYPE, 0, 0, "key", "value", new RecordHeaders(), Optional.of(7));
		IllegalStateException failure = new IllegalStateException("listener failed");

		processor.process(List.of(record), consumer, nullContainer ? null : container, failure, true, EOSMode.V2);

		InOrder order = inOrder(recoverer, consumer, template);
		order.verify(recoverer).accept(record, failure);
		order.verify(template).isTransactional();
		order.verify(consumer).groupMetadata();
		order.verify(template).sendOffsetsToTransaction(
				Map.of(new TopicPartition("topic", 0), new OffsetAndMetadata(42L, Optional.of(7), "")), group);
		order.verifyNoMoreInteractions();
	}

	@Test
	void batchRecoveryPreservesLeaderEpoch() {
		KafkaOperations<String, String> template = mock();
		given(template.isTransactional()).willReturn(true);
		ConsumerRecordRecoverer recoverer = mock();
		DefaultAfterRollbackProcessor<String, String> processor = new DefaultAfterRollbackProcessor<>(recoverer,
				new FixedBackOff(0, 0), template, true);
		Consumer<String, String> consumer = mock();
		ConsumerGroupMetadata group = new ConsumerGroupMetadata("group");
		given(consumer.groupMetadata()).willReturn(group);
		MessageListenerContainer container = mock();
		given(container.getContainerProperties()).willReturn(new ContainerProperties("topic"));
		TopicPartition first = new TopicPartition("topic", 0);
		TopicPartition second = new TopicPartition("topic", 1);
		ConsumerRecord<String, String> record1 = epochRecord(first, 41L, Optional.of(7));
		ConsumerRecord<String, String> record2 = epochRecord(first, 43L, Optional.of(8));
		ConsumerRecord<String, String> record3 = epochRecord(second, 99L, Optional.of(2));
		ConsumerRecords<String, String> records = new ConsumerRecords<>(
				Map.of(first, List.of(record1, record2), second, List.of(record3)), Map.of());
		IllegalStateException failure = new IllegalStateException("listener failed");

		processor.processBatch(records, List.of(record1, record2, record3), consumer, container,
				failure, true, EOSMode.V2);

		verify(recoverer).accept(record1, failure);
		verify(recoverer).accept(record2, failure);
		verify(recoverer).accept(record3, failure);
		verify(template).isTransactional();
		verify(template).sendOffsetsToTransaction(Map.of(
				first, new OffsetAndMetadata(44L, Optional.of(8), ""),
				second, new OffsetAndMetadata(100L, Optional.of(2), "")), group);
		verifyNoMoreInteractions(template, recoverer);
	}

	@ParameterizedTest
	@CsvSource({ "false,7,-1,0,7", "false,7,9,100,9", "true,7,-1,0,7", "true,7,9,100,9" })
	void recoveryProviderPolicy(boolean batch, int sourceEpoch, int providerEpoch, long delta, int expectedEpoch) {
		KafkaOperations<String, String> template = mock();
		given(template.isTransactional()).willReturn(true);
		AtomicInteger recoveries = new AtomicInteger();
		DefaultAfterRollbackProcessor<String, String> processor = new DefaultAfterRollbackProcessor<>(
				(record, failure) -> recoveries.incrementAndGet(), new FixedBackOff(0, 0), template, true);
		Consumer<String, String> consumer = mock();
		ConsumerGroupMetadata group = new ConsumerGroupMetadata("group");
		given(consumer.groupMetadata()).willReturn(group);
		MessageListenerContainer container = mock();
		ContainerProperties properties = new ContainerProperties("topic");
		given(container.getContainerProperties()).willReturn(properties);
		List<Long> calls = new ArrayList<>();
		properties.setOffsetAndMetadataProvider((metadata, offset) -> {
			assertThat(recoveries.get()).isEqualTo(batch ? 2 : 1);
			calls.add(offset);
			return new OffsetAndMetadata(offset + delta,
					providerEpoch < 0 ? Optional.empty() : Optional.of(providerEpoch), "custom-" + offset);
		});
		Optional<Integer> epoch = sourceEpoch < 0 ? Optional.empty() : Optional.of(sourceEpoch);
		TopicPartition partition = new TopicPartition("topic", 0);
		List<ConsumerRecord<String, String>> list = batch
				? List.of(epochRecord(partition, 41L, Optional.of(5)), epochRecord(partition, 43L, epoch))
				: List.of(epochRecord(partition, 43L, epoch));
		if (batch) {
			processor.processBatch(new ConsumerRecords<>(Map.of(partition, list), Map.of()), list,
					consumer, container, new IllegalStateException(), true, EOSMode.V2);
		}
		else {
			processor.process(list, consumer, container, new IllegalStateException(), true, EOSMode.V2);
		}
		verify(template).sendOffsetsToTransaction(Map.of(partition, new OffsetAndMetadata(44L + delta,
				expectedEpoch < 0 ? Optional.empty() : Optional.of(expectedEpoch), "custom-44")), group);
		assertThat(calls).containsExactlyElementsOf(batch ? List.of(42L, 44L) : List.of(44L));
	}

	@ParameterizedTest
	@ValueSource(booleans = { false, true })
	void unacceptedRecoverySendsNothing(boolean batch) {
		KafkaOperations<String, String> template = mock();
		RuntimeException failure = new IllegalStateException("recovery failed");
		DefaultAfterRollbackProcessor<String, String> processor = new DefaultAfterRollbackProcessor<>((record, ex) -> {
			throw failure;
		}, new FixedBackOff(0, 0), template, true);
		Consumer<String, String> consumer = mock();
		MessageListenerContainer container = mock();
		ContainerProperties properties = new ContainerProperties("topic");
		given(container.getContainerProperties()).willReturn(properties);
		OffsetAndMetadataProvider provider = mock();
		properties.setOffsetAndMetadataProvider(provider);
		TopicPartition partition = new TopicPartition("topic", 0);
		List<ConsumerRecord<String, String>> list = List.of(epochRecord(partition, 41L, Optional.of(7)));
		if (batch) {
			assertThatExceptionOfType(IllegalStateException.class).isThrownBy(() -> processor.processBatch(
					new ConsumerRecords<>(Map.of(partition, list), Map.of()), list,
					consumer, container, new IllegalStateException(), true, EOSMode.V2)).isSameAs(failure);
		}
		else {
			processor.process(list, consumer, container, new IllegalStateException(), true, EOSMode.V2);
		}
		verify(consumer).seek(partition, 41L);
		verifyNoMoreInteractions(consumer, template, provider);
	}

	@Test
	void recoveryReadsCurrentProviderPerRecord() {
		KafkaOperations<String, String> template = mock();
		given(template.isTransactional()).willReturn(true);
		DefaultAfterRollbackProcessor<String, String> processor = new DefaultAfterRollbackProcessor<>(
				(record, failure) -> { }, new FixedBackOff(0, 0), template, true);
		Consumer<String, String> consumer = mock();
		ConsumerGroupMetadata group = new ConsumerGroupMetadata("group");
		given(consumer.groupMetadata()).willReturn(group);
		MessageListenerContainer container = mock();
		ContainerProperties properties = new ContainerProperties("topic");
		given(container.getContainerProperties()).willReturn(properties);
		List<Long> calls = new ArrayList<>();
		properties.setOffsetAndMetadataProvider((metadata, offset) -> {
			calls.add(offset);
			properties.setOffsetAndMetadataProvider((nextMetadata, nextOffset) -> {
				calls.add(nextOffset);
				return new OffsetAndMetadata(nextOffset, Optional.of(9), "replacement");
			});
			return new OffsetAndMetadata(offset, "first");
		});
		TopicPartition partition = new TopicPartition("topic", 0);
		List<ConsumerRecord<String, String>> list = List.of(
				epochRecord(partition, 41L, Optional.of(7)), epochRecord(partition, 43L, Optional.of(8)));
		processor.processBatch(new ConsumerRecords<>(Map.of(partition, list), Map.of()), list,
				consumer, container, new IllegalStateException(), true, EOSMode.V2);
		verify(template).sendOffsetsToTransaction(Map.of(partition,
				new OffsetAndMetadata(44L, Optional.of(9), "replacement")), group);
		assertThat(calls).containsExactly(42L, 44L);
	}

	private static ConsumerRecord<String, String> epochRecord(TopicPartition partition, long offset,
			Optional<Integer> epoch) {

		return new ConsumerRecord<>(partition.topic(), partition.partition(), offset, 0L,
				TimestampType.NO_TIMESTAMP_TYPE, 0, 0, "key", "value", new RecordHeaders(), epoch);
	}

	@Test
	void testClassifier() {
		AtomicReference<ConsumerRecord<?, ?>> recovered = new AtomicReference<>();
		AtomicBoolean recovererShouldFail = new AtomicBoolean(false);
		@SuppressWarnings("unchecked")
		KafkaOperations<String, String> template = mock(KafkaOperations.class);
		given(template.isTransactional()).willReturn(true);
		DefaultAfterRollbackProcessor<String, String> processor = new DefaultAfterRollbackProcessor<>((r, t) -> {
			if (recovererShouldFail.getAndSet(false)) {
				throw new RuntimeException("test recoverer failure");
			}
			recovered.set(r);
		}, SeekUtils.DEFAULT_BACK_OFF, template, true);
		ConsumerRecord<String, String> record1 = new ConsumerRecord<>("foo", 0, 0L, "foo", "bar");
		ConsumerRecord<String, String> record2 = new ConsumerRecord<>("foo", 1, 1L, "foo", "bar");
		List<ConsumerRecord<String, String>> records = Arrays.asList(record1, record2);
		IllegalStateException illegalState = new IllegalStateException();
		@SuppressWarnings("unchecked")
		Consumer<String, String> consumer = mock(Consumer.class);
		given(consumer.groupMetadata()).willReturn(mock(ConsumerGroupMetadata.class));
		MessageListenerContainer container = mock(MessageListenerContainer.class);
		given(container.getContainerProperties()).willReturn(new ContainerProperties("foo"));
		processor.process(records, consumer, container, illegalState, true, EOSMode.V2);
		processor.process(records, consumer, container,
				new DeserializationException("intended", null, false, illegalState), true, EOSMode.V2);
		verify(template).sendOffsetsToTransaction(anyMap(), any(ConsumerGroupMetadata.class));
		assertThat(recovered.get()).isSameAs(record1);
		processor.addNotRetryableExceptions(IllegalStateException.class);
		recovered.set(null);
		recovererShouldFail.set(true);
		processor.process(records, consumer, container, illegalState, true, EOSMode.V2);
		verify(template, times(1)).sendOffsetsToTransaction(anyMap(), any(ConsumerGroupMetadata.class)); // recovery failed
		processor.process(records, consumer, container, illegalState, true, EOSMode.V2);
		verify(template, times(2)).sendOffsetsToTransaction(anyMap(), any(ConsumerGroupMetadata.class));
		assertThat(recovered.get()).isSameAs(record1);
		InOrder inOrder = inOrder(consumer);
		inOrder.verify(consumer).seek(new TopicPartition("foo", 0), 0L); // not recovered so seek
		inOrder.verify(consumer, times(2)).seek(new TopicPartition("foo", 1), 1L);
		inOrder.verify(consumer).seek(new TopicPartition("foo", 0), 0L); // recovery failed
		inOrder.verify(consumer, times(2)).seek(new TopicPartition("foo", 1), 1L);
		inOrder.verify(consumer).groupMetadata();
		inOrder.verifyNoMoreInteractions();
	}

	@Test
	void skipAfterMaxRecoveryFailures() {
		KafkaOperations<String, String> template = mock();
		given(template.isTransactional()).willReturn(true);
		AtomicInteger recoveryAttempts = new AtomicInteger();
		DefaultAfterRollbackProcessor<String, String> processor = new DefaultAfterRollbackProcessor<>((r, t) -> {
			recoveryAttempts.incrementAndGet();
			throw new RuntimeException("test recoverer failure");
		}, new FixedBackOff(0L, 0L), template, true);
		processor.setMaxRecoveryFailures(2);
		ConsumerRecord<String, String> record1 = epochRecord(new TopicPartition("foo", 0), 0L, Optional.of(7));
		ConsumerRecord<String, String> record2 = new ConsumerRecord<>("foo", 1, 1L, "foo", "bar");
		List<ConsumerRecord<String, String>> records = Arrays.asList(record1, record2);
		Consumer<String, String> consumer = mock();
		ConsumerGroupMetadata group = new ConsumerGroupMetadata("group");
		given(consumer.groupMetadata()).willReturn(group);
		MessageListenerContainer container = mock();
		given(container.getContainerProperties()).willReturn(new ContainerProperties("foo"));
		IllegalStateException illegalState = new IllegalStateException();
		processor.process(records, consumer, container, illegalState, true, EOSMode.V2);
		// the first recovery failure is below the limit, so the record is not yet skipped
		verify(template, never()).sendOffsetsToTransaction(anyMap(), any(ConsumerGroupMetadata.class));
		processor.process(records, consumer, container, illegalState, true, EOSMode.V2);
		// the second failure hits the limit, so the record is treated as recovered
		verify(template).sendOffsetsToTransaction(
				Map.of(new TopicPartition("foo", 0), new OffsetAndMetadata(1L, Optional.of(7), "")), group);
		assertThat(recoveryAttempts.get()).isEqualTo(2);
		InOrder inOrder = inOrder(consumer);
		inOrder.verify(consumer).seek(new TopicPartition("foo", 0), 0L); // recovery failed, still seeked
		inOrder.verify(consumer, times(2)).seek(new TopicPartition("foo", 1), 1L);
		inOrder.verify(consumer).groupMetadata();
		inOrder.verifyNoMoreInteractions();
	}

	@Test
	void testBackOffNoBatchRecover() {

		@SuppressWarnings("unchecked")
		KafkaOperations<String, String> template = mock(KafkaOperations.class);
		given(template.isTransactional()).willReturn(true);
		BackOff backOff = spy(new FixedBackOff(0, 1));
		AtomicReference<BackOffExecution> execution = new AtomicReference<>();
		willAnswer(inv -> {
			BackOffExecution exec = spy((BackOffExecution) inv.callRealMethod());
			execution.set(exec);
			return exec;
		}).given(backOff).start();
		ConsumerRecordRecoverer recoverer = mock(ConsumerRecordRecoverer.class);
		DefaultAfterRollbackProcessor<String, String> processor = new DefaultAfterRollbackProcessor<>(recoverer,
				backOff, template, false);
		ConsumerRecord<String, String> record1 = new ConsumerRecord<>("foo", 0, 0L, "foo", "bar");
		ConsumerRecord<String, String> record2 = new ConsumerRecord<>("foo", 1, 1L, "foo", "bar");
		List<ConsumerRecord<String, String>> records = Arrays.asList(record1, record2);
		Map<TopicPartition, List<ConsumerRecord<String, String>>> map = new HashMap<>();
		records.forEach(rec -> map.computeIfAbsent(new TopicPartition(rec.topic(), rec.partition()),
				tp -> new ArrayList<>()).add(rec));
		ConsumerRecords<String, String> consumerRecords = new ConsumerRecords<>(map, Map.of());
		IllegalStateException illegalState = new IllegalStateException();
		@SuppressWarnings("unchecked")
		Consumer<String, String> consumer = mock(Consumer.class);
		given(consumer.groupMetadata()).willReturn(mock(ConsumerGroupMetadata.class));
		MessageListenerContainer container = mock(MessageListenerContainer.class);
		given(container.isRunning()).willReturn(true);
		processor.processBatch(consumerRecords, records, consumer, container, illegalState, false, EOSMode.V2);
		processor.processBatch(consumerRecords, records, consumer, container, illegalState, false, EOSMode.V2);
		verify(backOff, times(2)).start();
		verify(execution.get(), times(2)).nextBackOff();
		processor.clearThreadState();
		processor.processBatch(consumerRecords, records, consumer, container, illegalState, false, EOSMode.V2);
		verify(backOff, times(3)).start();
	}

	@Test
	void testEarlyExitBackOff() {
		DefaultAfterRollbackProcessor<String, String> processor = new DefaultAfterRollbackProcessor<>(
				new FixedBackOff(10_000, 1));
		@SuppressWarnings("unchecked")
		Consumer<String, String> consumer = mock(Consumer.class);
		ConsumerRecord<String, String> record1 = new ConsumerRecord<>("foo", 0, 0L, "foo", "bar");
		ConsumerRecord<String, String> record2 = new ConsumerRecord<>("foo", 1, 1L, "foo", "bar");
		List<ConsumerRecord<String, String>> records = Arrays.asList(record1, record2);
		IllegalStateException illegalState = new IllegalStateException();
		MessageListenerContainer container = mock(MessageListenerContainer.class);
		given(container.isRunning()).willReturn(false);
		long t1 = System.currentTimeMillis();
		processor.process(records, consumer, container, illegalState, true, EOSMode.V2);
		assertThat(System.currentTimeMillis() < t1 + 5_000).isTrue();
	}

	@Test
	void testNoEarlyExitBackOff() {
		DefaultAfterRollbackProcessor<String, String> processor = new DefaultAfterRollbackProcessor<>(
				new FixedBackOff(200, 1));
		@SuppressWarnings("unchecked")
		Consumer<String, String> consumer = mock(Consumer.class);
		ConsumerRecord<String, String> record1 = new ConsumerRecord<>("foo", 0, 0L, "foo", "bar");
		ConsumerRecord<String, String> record2 = new ConsumerRecord<>("foo", 1, 1L, "foo", "bar");
		List<ConsumerRecord<String, String>> records = Arrays.asList(record1, record2);
		IllegalStateException illegalState = new IllegalStateException();
		MessageListenerContainer container = mock(MessageListenerContainer.class);
		given(container.isRunning()).willReturn(true);
		long t1 = System.currentTimeMillis();
		processor.process(records, consumer, container, illegalState, true, EOSMode.V2);
		assertThat(System.currentTimeMillis() >= t1 + 200).isTrue();
	}

	@Test
	void batchRecoverWithConsumerToVerifyDeadLetterPartition() {
		KafkaOperations<Object, Object> template = mock();
		given(template.isTransactional()).willReturn(true);
		given(template.inTransaction()).willReturn(true);
		given(template.send(any(ProducerRecord.class))).willReturn(CompletableFuture.completedFuture(null));
		Consumer<String, String> consumer = mock();
		given(consumer.groupMetadata()).willReturn(mock());
		given(consumer.partitionsFor("orders-dlt", Duration.ofSeconds(5)))
				.willReturn(List.of(new PartitionInfo("orders-dlt", 0, null, null, null)));
		DefaultAfterRollbackProcessor<String, String> processor = new DefaultAfterRollbackProcessor<>(
				new DeadLetterPublishingRecoverer(template), new FixedBackOff(0L, 0L), template, true);
		ConsumerRecord<String, String> record = new ConsumerRecord<>("orders", 1, 0L, "key", "value");
		ConsumerRecords<String, String> records = new ConsumerRecords<>(
				Map.of(new TopicPartition("orders", 1), List.of(record)), Map.of());
		MessageListenerContainer container = mock();
		given(container.getContainerProperties()).willReturn(new ContainerProperties("orders"));
		processor.processBatch(records, List.of(record), consumer, container, new RuntimeException(), true,
				EOSMode.V2);
		ArgumentCaptor<ProducerRecord<Object, Object>> captor = ArgumentCaptor.captor();
		verify(template).send(captor.capture());
		assertThat(captor.getValue().topic()).isEqualTo("orders-dlt");
		assertThat(captor.getValue().partition()).isNull();
	}

}
