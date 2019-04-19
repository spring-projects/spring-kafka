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
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.function.Function;
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

	private final ConcurrentMap<CorrelationKey, List<ConsumerRecord<K, R>>> pending = new ConcurrentHashMap<>();

	private final Map<TopicPartition, Long> offsets = new HashMap<>();

	private final Function<Collection<ConsumerRecord<K, R>>, Boolean> releaseStrategy;

	private Duration commitTimeout = Duration.ofSeconds(30);

	private boolean returnPartialOnTimeout;

	/**
	 * Construct an instance using the provided parameter arguments. The releaseStrategy
	 * is consulted to determine when a collection is "complete".
	 * @param producerFactory the producer factory.
	 * @param replyContainer the reply container.
	 * @param releaseStrategy the release strategy.
	 */
	public AggregatingReplyingKafkaTemplate(ProducerFactory<K, V> producerFactory,
			GenericMessageListenerContainer<K, Collection<ConsumerRecord<K, R>>> replyContainer,
			Function<Collection<ConsumerRecord<K, R>>, Boolean> releaseStrategy) {

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
				List<ConsumerRecord<K, R>> list = addToCollection(record, correlationId);
				if (isPending(correlationId) && this.releaseStrategy.apply(list)) {
					ConsumerRecord<K, Collection<ConsumerRecord<K, R>>> done =
							new ConsumerRecord<>("aggregatedResults", 0, 0L, null, list);
					done.headers().add(new RecordHeader(KafkaHeaders.CORRELATION_ID, correlationId.getCorrelationId()));
					this.pending.remove(correlationId);
					checkOffsetCommits(list, consumer);
					completed.add(done);
				}
			}
		});
		if (completed.size() > 0) {
			super.onMessage(completed);
		}
	}

	private synchronized void checkOffsetCommits(List<ConsumerRecord<K, R>> list, Consumer<?, ?> consumer) {
		list.forEach(record -> this.offsets.compute(
			new TopicPartition(record.topic(), record.partition()), (k, v) -> record.offset()));
		if (this.pending.isEmpty()) {
			consumer.commitSync(this.offsets.entrySet().stream()
					.collect(Collectors.toMap(
											entry -> entry.getKey(),
											entry -> new OffsetAndMetadata(entry.getValue()))),
								this.commitTimeout);
			this.offsets.clear();
		}
	}

	@SuppressWarnings({ "rawtypes", "unchecked" })
	private List<ConsumerRecord<K, R>> addToCollection(ConsumerRecord record, CorrelationKey correlationId) {
		List<ConsumerRecord<K, R>> list = this.pending.computeIfAbsent(correlationId, id -> new ArrayList<>());
		list.add(record);
		return list;
	}

	@Override
	protected boolean handleTimeout(CorrelationKey correlationId,
			RequestReplyFuture<K, V, Collection<ConsumerRecord<K, R>>> future) {

		List<ConsumerRecord<K, R>> removed = this.pending.remove(correlationId);
		if (removed != null && this.returnPartialOnTimeout) {
			future.set(new ConsumerRecord<>("partialResultsAfterTimeout", 0, 0L, null, removed));
			return true;
		}
		else {
			return false;
		}
	}

}
