/*
 * Copyright 2018-2020 the original author or authors.
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

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiConsumer;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;

import org.springframework.core.log.LogAccessor;
import org.springframework.kafka.support.TopicPartitionOffset;
import org.springframework.lang.Nullable;
import org.springframework.util.Assert;
import org.springframework.util.backoff.BackOff;
import org.springframework.util.backoff.BackOffExecution;

/**
 * Track record processing failure counts.
 *
 * @author Gary Russell
 * @since 2.2
 *
 */
class FailedRecordTracker {

	private final ThreadLocal<Map<TopicPartition, FailedRecord>> failures = new ThreadLocal<>(); // intentionally not static

	private final BiConsumer<ConsumerRecord<?, ?>, Exception> recoverer;

	private final boolean noRetries;

	private final BackOff backOff;

	private boolean resetStateOnRecoveryFailure = true;

	FailedRecordTracker(@Nullable BiConsumer<ConsumerRecord<?, ?>, Exception> recoverer, BackOff backOff,
			LogAccessor logger) {

		Assert.notNull(backOff, "'backOff' cannot be null");
		if (recoverer == null) {
			this.recoverer = (rec, thr) -> {
				Map<TopicPartition, FailedRecord> map = this.failures.get();
				FailedRecord failedRecord = null;
				if (map != null) {
					failedRecord = map.get(new TopicPartition(rec.topic(), rec.partition()));
				}
				logger.error(thr, "Backoff "
					+ (failedRecord == null
						? "none"
						: failedRecord.getBackOffExecution())
					+ " exhausted for " + ListenerUtils.recordToString(rec));
			};
		}
		else {
			this.recoverer = recoverer;
		}
		this.noRetries = backOff.start().nextBackOff() == BackOffExecution.STOP;
		this.backOff = backOff;
	}

	/**
	 * Set to false to immediately attempt to recover on the next attempt instead
	 * of repeating the BackOff cycle when recovery fails.
	 * @param resetStateOnRecoveryFailure false to retain state.
	 * @since 3.5.5
	 */
	public void setResetStateOnRecoveryFailure(boolean resetStateOnRecoveryFailure) {
		this.resetStateOnRecoveryFailure = resetStateOnRecoveryFailure;
	}

	boolean skip(ConsumerRecord<?, ?> record, Exception exception) {
		if (this.noRetries) {
			attemptRecovery(record, exception, null);
			return true;
		}
		Map<TopicPartition, FailedRecord> map = this.failures.get();
		if (map == null) {
			this.failures.set(new HashMap<>());
			map = this.failures.get();
		}
		TopicPartition topicPartition = new TopicPartition(record.topic(), record.partition());
		FailedRecord failedRecord = map.get(topicPartition);
		if (failedRecord == null || failedRecord.getOffset() != record.offset()) {
			failedRecord = new FailedRecord(record.offset(), this.backOff.start());
			map.put(topicPartition, failedRecord);
		}
		else {
			failedRecord.getDeliveryAttempts().incrementAndGet();
		}
		long nextBackOff = failedRecord.getBackOffExecution().nextBackOff();
		if (nextBackOff != BackOffExecution.STOP) {
			try {
				Thread.sleep(nextBackOff);
			}
			catch (@SuppressWarnings("unused") InterruptedException e) {
				Thread.currentThread().interrupt();
			}
			return false;
		}
		else {
			attemptRecovery(record, exception, topicPartition);
			map.remove(topicPartition);
			if (map.isEmpty()) {
				this.failures.remove();
			}
			return true;
		}
	}

	private void attemptRecovery(ConsumerRecord<?, ?> record, Exception exception, @Nullable TopicPartition tp) {
		try {
			this.recoverer.accept(record, exception);
		}
		catch (RuntimeException e) {
			if (tp != null && this.resetStateOnRecoveryFailure) {
				this.failures.get().remove(tp);
			}
			throw e;
		}
	}

	void clearThreadState() {
		this.failures.remove();
	}

	BiConsumer<ConsumerRecord<?, ?>, Exception> getRecoverer() {
		return this.recoverer;
	}

	/**
	 * Return the number of the next delivery attempt for this topic/partition/offsete.
	 * @param topicPartitionOffset the topic/partition/offset.
	 * @return the delivery attempt.
	 * @since 2.5
	 */
	int deliveryAttempt(TopicPartitionOffset topicPartitionOffset) {
		Map<TopicPartition, FailedRecord> map = this.failures.get();
		if (map == null) {
			return 1;
		}
		FailedRecord failedRecord = map.get(topicPartitionOffset.getTopicPartition());
		if (failedRecord == null || failedRecord.getOffset() != topicPartitionOffset.getOffset()) {
			return 1;
		}
		return failedRecord.getDeliveryAttempts().get() + 1;
	}

	private static final class FailedRecord {

		private final long offset;

		private final BackOffExecution backOffExecution;

		private final AtomicInteger deliveryAttempts = new AtomicInteger(1);

		FailedRecord(long offset, BackOffExecution backOffExecution) {
			this.offset = offset;
			this.backOffExecution = backOffExecution;
		}

		long getOffset() {
			return this.offset;
		}

		BackOffExecution getBackOffExecution() {
			return this.backOffExecution;
		}

		AtomicInteger getDeliveryAttempts() {
			return this.deliveryAttempts;
		}

	}

}
