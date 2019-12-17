/*
 * Copyright 2019 the original author or authors.
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

package org.springframework.kafka.requestreply;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.BiPredicate;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.internals.RecordHeader;

import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.listener.BatchConsumerAwareMessageListener;
import org.springframework.kafka.listener.ContainerProperties.AckMode;
import org.springframework.kafka.listener.GenericMessageListenerContainer;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.util.Assert;

/**
 * A replying template that aggregates multiple replies with the same correlation id.
 *
 * @param <K> the key type.
 * @param <V> the outbound data type.
 * @param <R> the reply data type.
 *
 * @author Gary Russell
 * @since 2.3
 *
 */
public class AggregatingReplyingKafkaTemplate<K, V, R>
		extends ReplyingKafkaTemplate<K, V, Collection<ConsumerRecord<K, R>>>
		implements BatchConsumerAwareMessageListener<K, Collection<ConsumerRecord<K, R>>> {

	/**
	 * Pseudo topic name for the "outer" {@link ConsumerRecord} that has the aggregated
	 * results in its value after a normal release by the release strategy.
	 */
	public static final String AGGREGATED_RESULTS_TOPIC = "aggregatedResults";

	/**
	 * Pseudo topic name for the "outer" {@link ConsumerRecord} that has the aggregated
	 * results in its value after a timeout.
	 */
	public static final String PARTIAL_RESULTS_AFTER_TIMEOUT_TOPIC = "partialResultsAfterTimeout";

	private static final int DEFAULT_COMMIT_TIMEOUT = 30;

	private final Map<CorrelationKey, Set<RecordHolder<K, R>>> pending = new HashMap<>();

	private final Map<TopicPartition, Long> offsets = new HashMap<>();

	private final BiPredicate<List<ConsumerRecord<K, R>>, Boolean> releaseStrategy;

	private Duration commitTimeout = Duration.ofSeconds(DEFAULT_COMMIT_TIMEOUT);

	private boolean returnPartialOnTimeout;

	/**
	 * Construct an instance using the provided parameter arguments. The releaseStrategy
	 * is consulted to determine when a collection is "complete".
	 * @param producerFactory the producer factory.
	 * @param replyContainer the reply container.
	 * @param releaseStrategy the release strategy.
	 * @deprecated in favor of
	 * {@link #AggregatingReplyingKafkaTemplate(ProducerFactory, GenericMessageListenerContainer, BiPredicate)}
	 */
	@Deprecated
	public AggregatingReplyingKafkaTemplate(ProducerFactory<K, V> producerFactory,
			GenericMessageListenerContainer<K, Collection<ConsumerRecord<K, R>>> replyContainer,
			Predicate<Collection<ConsumerRecord<K, R>>> releaseStrategy) {

		this(producerFactory, replyContainer, (records, timeout) -> timeout || releaseStrategy.test(records));
	}

	/**
	 * Construct an instance using the provided parameter arguments. The releaseStrategy
	 * is consulted to determine when a collection is "complete".
	 * @param producerFactory the producer factory.
	 * @param replyContainer the reply container.
	 * @param releaseStrategy the release strategy which is a {@link BiPredicate} which is
	 * passed the current list and a boolean to indicate if this is for a normal delivery
	 * or a timeout (when {@link #setReturnPartialOnTimeout(boolean)} is true. The
	 * predicate may modify the list of records.
	 * @since 2.3.5
	 */
	public AggregatingReplyingKafkaTemplate(ProducerFactory<K, V> producerFactory,
			GenericMessageListenerContainer<K, Collection<ConsumerRecord<K, R>>> replyContainer,
			BiPredicate<List<ConsumerRecord<K, R>>, Boolean> releaseStrategy) {

		super(producerFactory, replyContainer);
		Assert.notNull(releaseStrategy, "'releaseStrategy' cannot be null");
		AckMode ackMode = replyContainer.getContainerProperties().getAckMode();
		Assert.isTrue(ackMode.equals(AckMode.MANUAL) || ackMode.equals(AckMode.MANUAL_IMMEDIATE),
				"The reply container must have a MANUAL or MANUAL_IMMEDIATE AckMode");
		this.releaseStrategy = releaseStrategy;
	}

	/**
	 * Set the timeout to use when committing offsets.
	 * @param commitTimeout the timeout.
	 */
	public void setCommitTimeout(Duration commitTimeout) {
		Assert.notNull(commitTimeout, "'commitTimeout' cannot be null");
		this.commitTimeout = commitTimeout;
	}

	/**
	 * Set to true to return a partial result when a request times out.
	 * @param returnPartialOnTimeout true to return a partial result.
	 */
	public synchronized void setReturnPartialOnTimeout(boolean returnPartialOnTimeout) {
		this.returnPartialOnTimeout = returnPartialOnTimeout;
	}

	@Override
	public void onMessage(List<ConsumerRecord<K, Collection<ConsumerRecord<K, R>>>> data, Consumer<?, ?> consumer) {
		List<ConsumerRecord<K, Collection<ConsumerRecord<K, R>>>> completed = new ArrayList<>();
		data.forEach(record -> {
			Header correlation = record.headers().lastHeader(KafkaHeaders.CORRELATION_ID);
			if (correlation == null) {
				this.logger.error(() -> "No correlationId found in reply: " + record
						+ " - to use request/reply semantics, the responding server must return the correlation id "
						+ " in the '" + KafkaHeaders.CORRELATION_ID + "' header");
			}
			else {
				CorrelationKey correlationId = new CorrelationKey(correlation.value());
				synchronized (this) {
					if (isPending(correlationId)) {
						List<ConsumerRecord<K, R>> list = addToCollection(record, correlationId).stream()
								.map(RecordHolder::getRecord)
								.collect(Collectors.toList());
						if (this.releaseStrategy.test(list, false)) {
							ConsumerRecord<K, Collection<ConsumerRecord<K, R>>> done =
									new ConsumerRecord<>(AGGREGATED_RESULTS_TOPIC, 0, 0L, null, list);
							done.headers()
									.add(new RecordHeader(KafkaHeaders.CORRELATION_ID, correlationId
											.getCorrelationId()));
							this.pending.remove(correlationId);
							checkOffsetsAndCommitIfNecessary(list, consumer);
							completed.add(done);
						}
					}
					else {
						logLateArrival(record, correlationId);
					}
				}
			}
		});
		if (completed.size() > 0) {
			super.onMessage(completed);
		}
	}

	@Override
	protected synchronized boolean handleTimeout(CorrelationKey correlationId,
			RequestReplyFuture<K, V, Collection<ConsumerRecord<K, R>>> future) {

		Set<RecordHolder<K, R>> removed = this.pending.remove(correlationId);
		if (removed != null && this.returnPartialOnTimeout) {
			List<ConsumerRecord<K, R>> list = removed.stream()
					.map(RecordHolder::getRecord)
					.collect(Collectors.toList());
			if (this.releaseStrategy.test(list, true)) {
				future.set(new ConsumerRecord<>(PARTIAL_RESULTS_AFTER_TIMEOUT_TOPIC, 0, 0L, null, list));
				return true;
			}
		}
		return false;
	}

	private void checkOffsetsAndCommitIfNecessary(List<ConsumerRecord<K, R>> list, Consumer<?, ?> consumer) {
		list.forEach(record -> this.offsets.compute(
				new TopicPartition(record.topic(), record.partition()),
				(k, v) -> v == null ? record.offset() + 1 : Math.max(v, record.offset() + 1)));
		if (this.pending.isEmpty() && !this.offsets.isEmpty()) {
			consumer.commitSync(this.offsets.entrySet().stream()
							.collect(Collectors.toMap(Map.Entry::getKey,
									entry -> new OffsetAndMetadata(entry.getValue()))),
					this.commitTimeout);
			this.offsets.clear();
		}
	}

	@SuppressWarnings({ "rawtypes", "unchecked" })
	private Set<RecordHolder<K, R>> addToCollection(ConsumerRecord record, CorrelationKey correlationId) {
		Set<RecordHolder<K, R>> set = this.pending.computeIfAbsent(correlationId, id -> new LinkedHashSet<>());
		set.add(new RecordHolder<>(record));
		return set;
	}

	private static final class RecordHolder<K, R> {

		private final ConsumerRecord<K, R> record;

		RecordHolder(ConsumerRecord<K, R> record) {
			this.record = record;
		}

		ConsumerRecord<K, R> getRecord() {
			return this.record;
		}

		@Override
		public int hashCode() {
			final int prime = 31;
			int result = 1;
			result = prime * result
					+ this.record.topic().hashCode()
					+ this.record.partition()
					+ (int) this.record.offset();
			return result;
		}

		@Override
		public boolean equals(Object obj) {
			if (this == obj) {
				return true;
			}
			if (obj == null) {
				return false;
			}
			if (getClass() != obj.getClass()) {
				return false;
			}
			@SuppressWarnings("rawtypes")
			RecordHolder other = (RecordHolder) obj;
			if (this.record == null) {
				if (other.record != null) {
					return false;
				}
			}
			else {
				return this.record.topic().equals(other.record.topic())
						&& this.record.partition() == other.record.partition()
						&& this.record.offset() == other.record.offset();
			}

			return false;
		}

	}

}
