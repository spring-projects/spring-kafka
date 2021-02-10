/*
 * Copyright 2018-2021 the original author or authors.
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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;

import org.apache.kafka.clients.consumer.Consumer;
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
class FailedRecordTracker implements RecoveryStrategy {

	private final ThreadLocal<Map<TopicPartition, FailedRecord>> failures = new ThreadLocal<>(); // intentionally not static

	private final ConsumerAwareRecordRecoverer recoverer;

	private final boolean noRetries;

	private final List<RetryListener> retryListeners = new ArrayList<>();

	private final BackOff backOff;

	private BiFunction<ConsumerRecord<?, ?>, Exception, BackOff> backOffFunction;

	private boolean resetStateOnRecoveryFailure = true;

	private boolean resetStateOnExceptionChange;

	FailedRecordTracker(@Nullable BiConsumer<ConsumerRecord<?, ?>, Exception> recoverer, BackOff backOff,
			LogAccessor logger) {

		Assert.notNull(backOff, "'backOff' cannot be null");
		if (recoverer == null) {
			this.recoverer = (rec, consumer, thr) -> {
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
			if (recoverer instanceof ConsumerAwareRecordRecoverer) {
				this.recoverer = (ConsumerAwareRecordRecoverer) recoverer;
			}
			else {
				this.recoverer = (rec, consumer, ex) -> recoverer.accept(rec, ex);
			}
		}
		this.noRetries = backOff.start().nextBackOff() == BackOffExecution.STOP;
		this.backOff = backOff;
	}

	/**
	 * Set a function to dynamically determine the {@link BackOff} to use, based on the
	 * consumer record and/or exception. If null is returned, the default BackOff will be
	 * used.
	 * @param backOffFunction the function.
	 * @since 2.6
	 */
	public void setBackOffFunction(BiFunction<ConsumerRecord<?, ?>, Exception, BackOff> backOffFunction) {
		this.backOffFunction = backOffFunction;
	}

	/**
	 * Set to false to immediately attempt to recover on the next attempt instead
	 * of repeating the BackOff cycle when recovery fails.
	 * @param resetStateOnRecoveryFailure false to retain state.
	 * @since 2.5.5
	 */
	public void setResetStateOnRecoveryFailure(boolean resetStateOnRecoveryFailure) {
		this.resetStateOnRecoveryFailure = resetStateOnRecoveryFailure;
	}

	/**
	 * Set to true to reset the retry {@link BackOff} if the exception is a different type
	 * to the previous failure for the same record. The
	 * {@link #setBackOffFunction(BiFunction) backOffFunction}, if provided, will be
	 * called to get the {@link BackOff} to use for the new exception; otherwise, the
	 * configured {@link BackOff} will be used.
	 * @param resetStateOnExceptionChange true to reset.
	 * @since 2.6.3
	 */
	public void setResetStateOnExceptionChange(boolean resetStateOnExceptionChange) {
		this.resetStateOnExceptionChange = resetStateOnExceptionChange;
	}

	/**
	 * Set one or more {@link RetryListener} to receive notifications of retries and
	 * recovery.
	 * @param listeners the listeners.
	 * @since 2.7
	 */
	public void setRetryListeners(RetryListener... listeners) {
		this.retryListeners.clear();
		this.retryListeners.addAll(Arrays.asList(listeners));
	}

	List<RetryListener> getRetryListeners() {
		return this.retryListeners;
	}

	boolean skip(ConsumerRecord<?, ?> record, Exception exception) {
		try {
			return recovered(record, exception, null, null);
		}
		catch (InterruptedException e) {
			Thread.currentThread().interrupt();
			return false;
		}
	}

	@Override
	public boolean recovered(ConsumerRecord<?, ?> record, Exception exception,
			@Nullable MessageListenerContainer container,
			@Nullable Consumer<?, ?> consumer) throws InterruptedException {

		if (this.noRetries) {
			attemptRecovery(record, exception, null, consumer);
			return true;
		}
		Map<TopicPartition, FailedRecord> map = this.failures.get();
		if (map == null) {
			this.failures.set(new HashMap<>());
			map = this.failures.get();
		}
		TopicPartition topicPartition = new TopicPartition(record.topic(), record.partition());
		FailedRecord failedRecord = getFailedRecordInstance(record, exception, map, topicPartition);
		this.retryListeners.forEach(rl ->
				rl.failedDelivery(record, exception, failedRecord.getDeliveryAttempts().get()));
		long nextBackOff = failedRecord.getBackOffExecution().nextBackOff();
		if (nextBackOff != BackOffExecution.STOP) {
			if (container == null) {
				Thread.sleep(nextBackOff);
			}
			else {
				ListenerUtils.stoppableSleep(container, nextBackOff);
			}
			return false;
		}
		else {
			attemptRecovery(record, exception, topicPartition, consumer);
			map.remove(topicPartition);
			if (map.isEmpty()) {
				this.failures.remove();
			}
			return true;
		}
	}

	private FailedRecord getFailedRecordInstance(ConsumerRecord<?, ?> record, Exception exception,
			Map<TopicPartition, FailedRecord> map, TopicPartition topicPartition) {

		Exception realException = exception;
		if (realException  instanceof ListenerExecutionFailedException
				&& realException.getCause() instanceof Exception) {

			realException = (Exception) realException.getCause();
		}
		FailedRecord failedRecord = map.get(topicPartition);
		if (failedRecord == null || failedRecord.getOffset() != record.offset()
				|| (this.resetStateOnExceptionChange
						&& !realException.getClass().isInstance(failedRecord.getLastException()))) {

			failedRecord = new FailedRecord(record.offset(), determineBackOff(record, realException).start());
			map.put(topicPartition, failedRecord);
		}
		else {
			failedRecord.getDeliveryAttempts().incrementAndGet();
		}
		failedRecord.setLastException(realException);
		return failedRecord;
	}

	private BackOff determineBackOff(ConsumerRecord<?, ?> record, Exception exception) {
		if (this.backOffFunction == null) {
			return this.backOff;
		}
		BackOff backOffToUse = this.backOffFunction.apply(record, exception);
		return backOffToUse != null ? backOffToUse : this.backOff;
	}

	private void attemptRecovery(ConsumerRecord<?, ?> record, Exception exception, @Nullable TopicPartition tp,
			Consumer<?, ?> consumer) {

		try {
			this.recoverer.accept(record, consumer, exception);
			this.retryListeners.forEach(rl -> rl.recovered(record, exception));
		}
		catch (RuntimeException e) {
			this.retryListeners.forEach(rl -> rl.recoveryFailed(record, exception, e));
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

	static final class FailedRecord {

		private final long offset;

		private final BackOffExecution backOffExecution;

		private final AtomicInteger deliveryAttempts = new AtomicInteger(1);

		private Exception lastException;

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

		Exception getLastException() {
			return this.lastException;
		}

		void setLastException(Exception lastException) {
			this.lastException = lastException;
		}

	}

}
