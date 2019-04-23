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
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
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
	 * Pseudo topic name for the "outer" {@link ConsumerRecords} that has the aggregated
	 * results in its value after a normal release by the release strategy.
	 */
	public static final String AGGREGATED_RESULTS_TOPIC = "aggregatedResults";

	/**
	 * Pseudo topic name for the "outer" {@link ConsumerRecords} that has the aggregated
	 * results in its value after a timeout.
	 */
	public static final String PARTIAL_RESULTS_AFTER_TIMEOUT_TOPIC = "partialResultsAfterTimeout";

	private final ConcurrentMap<CorrelationKey, Set<RecordHolder<K, R>>> pending = new ConcurrentHashMap<>();

	private final Map<TopicPartition, Long> offsets = new HashMap<>();

	private final Predicate<Collection<ConsumerRecord<K, R>>> releaseStrategy;

	private Duration commitTimeout = Duration.ofSeconds(30);

	private boolean returnPartialOnTimeout;

	private volatile long lastOrphanCheck = System.currentTimeMillis();

	/**
	 * Construct an instance using the provided parameter arguments. The releaseStrategy
	 * is consulted to determine when a collection is "complete".
	 * @param producerFactory the producer factory.
	 * @param replyContainer the reply container.
	 * @param releaseStrategy the release strategy.
	 */
	public AggregatingReplyingKafkaTemplate(ProducerFactory<K, V> producerFactory,
			GenericMessageListenerContainer<K, Collection<ConsumerRecord<K, R>>> replyContainer,
			Predicate<Collection<ConsumerRecord<K, R>>> releaseStrategy) {

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
	public void setReturnPartialOnTimeout(boolean returnPartialOnTimeout) {
		this.returnPartialOnTimeout = returnPartialOnTimeout;
	}

	@Override
	public void onMessage(List<ConsumerRecord<K, Collection<ConsumerRecord<K, R>>>> data, Consumer<?, ?> consumer) {
		long now = System.currentTimeMillis();
		if (now - this.lastOrphanCheck > getReplyTimeout() * 10) { // NOSONAR magic #
			weedOrphans(consumer);
		}
		List<ConsumerRecord<K, Collection<ConsumerRecord<K, R>>>> completed = new ArrayList<>();
		data.forEach(record -> {
			Header correlation = record.headers().lastHeader(KafkaHeaders.CORRELATION_ID);
			if (correlation == null) {
				this.logger.error("No correlationId found in reply: " + record
						+ " - to use request/reply semantics, the responding server must return the correlation id "
						+ " in the '" + KafkaHeaders.CORRELATION_ID + "' header");
			}
			else {
				CorrelationKey correlationId = new CorrelationKey(correlation.value());
				List<ConsumerRecord<K, R>> list = addToCollection(record, correlationId, now).stream()
						.map(entry -> entry.getRecord())
						.collect(Collectors.toList());
				if (isPending(correlationId) && this.releaseStrategy.test(list)) {
					ConsumerRecord<K, Collection<ConsumerRecord<K, R>>> done =
							new ConsumerRecord<>(AGGREGATED_RESULTS_TOPIC, 0, 0L, null, list);
					done.headers().add(new RecordHeader(KafkaHeaders.CORRELATION_ID, correlationId.getCorrelationId()));
					this.pending.remove(correlationId);
					checkOffsets(list);
					commitIfNecessary(consumer);
					completed.add(done);
				}
			}
		});
		if (completed.size() > 0) {
			super.onMessage(completed);
		}
	}

	/**
	 * We may receive a partial delivery of a previously completed group of replies
	 * e.g. after a rebalance. To avoid a memory leak, check for such conditions and
	 * discard from time-to-time.
	 * @param consumer the consumer.
	 */
	private void weedOrphans(Consumer<?, ?> consumer) {
		Map<CorrelationKey, List<ConsumerRecord<K, R>>> orphaned = this.pending.entrySet()
			.stream()
			.filter(entry -> entry.getValue()
					.stream()
					.allMatch(holder -> holder.getTimestamp() > this.lastOrphanCheck))
			.collect(Collectors.toMap(entry -> entry.getKey(), entry -> entry.getValue()
					.stream()
					.map(holder -> holder.getRecord())
					.collect(Collectors.toList())));
		logger.debug("Discarding " + orphaned + " most likely a partial redelivery of an already released group");
		orphaned.values()
			.forEach(list -> checkOffsets(list));
		orphaned.keySet().forEach(corr -> this.pending.remove(corr));
		commitIfNecessary(consumer);
	}

	@Override
	protected boolean handleTimeout(CorrelationKey correlationId,
			RequestReplyFuture<K, V, Collection<ConsumerRecord<K, R>>> future) {

		Set<RecordHolder<K, R>> removed = this.pending.remove(correlationId);
		if (removed != null && this.returnPartialOnTimeout) {
			List<ConsumerRecord<K, R>> list = removed.stream()
				.map(entry -> entry.getRecord())
				.collect(Collectors.toList());
			future.set(new ConsumerRecord<>(PARTIAL_RESULTS_AFTER_TIMEOUT_TOPIC, 0, 0L, null, list));
			return true;
		}
		else {
			return false;
		}
	}

	private synchronized void checkOffsets(List<ConsumerRecord<K, R>> list) {
		list.forEach(record -> this.offsets.compute(
			new TopicPartition(record.topic(), record.partition()),
				(k, v) -> v == null ? record.offset() + 1 : Math.max(v, record.offset() + 1)));
	}

	private synchronized void commitIfNecessary(Consumer<?, ?> consumer) {
		if (this.pending.isEmpty() && !this.offsets.isEmpty()) {
			consumer.commitSync(this.offsets.entrySet().stream()
					.collect(Collectors.toMap(
											Map.Entry::getKey,
											entry -> new OffsetAndMetadata(entry.getValue()))),
								this.commitTimeout);
			this.offsets.clear();
		}
	}

	@SuppressWarnings({ "rawtypes", "unchecked" })
	private Set<RecordHolder<K, R>> addToCollection(ConsumerRecord record, CorrelationKey correlationId, long now) {
		Set<RecordHolder<K, R>> set = this.pending.computeIfAbsent(correlationId, id -> new LinkedHashSet<>());
		set.add(new RecordHolder<>(record, now));
		return set;
	}

	private static final class RecordHolder<K, R> {

		private final ConsumerRecord<K, R> record;

		private final long timestamp;

		RecordHolder(ConsumerRecord<K, R> record, long timestamp) {
			this.record = record;
			this.timestamp = timestamp;
		}

		ConsumerRecord<K, R> getRecord() {
			return this.record;
		}

		long getTimestamp() {
			return this.timestamp;
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
			else if (this.record.topic().equals(other.record.topic())
					&& this.record.partition() == other.record.partition()
					&& this.record.offset() == other.record.offset()) {
				return true;
			}
			return false;
		}

	}

}
