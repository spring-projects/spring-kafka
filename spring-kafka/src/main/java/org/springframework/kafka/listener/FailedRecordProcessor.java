/*
 * Copyright 2019-2022 the original author or authors.
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

import java.util.List;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.BiPredicate;

import org.apache.commons.logging.LogFactory;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import org.springframework.core.log.LogAccessor;
import org.springframework.kafka.support.KafkaUtils;
import org.springframework.kafka.support.TopicPartitionOffset;
import org.springframework.lang.Nullable;
import org.springframework.util.backoff.BackOff;

/**
 * Common super class for classes that deal with failing to consume a consumer record.
 *
 * @author Gary Russell
 * @since 2.3.1
 *
 */
public abstract class FailedRecordProcessor extends ExceptionClassifier implements DeliveryAttemptAware {

	private static final BiPredicate<ConsumerRecord<?, ?>, Exception> ALWAYS_SKIP_PREDICATE = (r, e) -> true;

	private static final BiPredicate<ConsumerRecord<?, ?>, Exception> NEVER_SKIP_PREDICATE = (r, e) -> false;

	protected final LogAccessor logger = new LogAccessor(LogFactory.getLog(getClass())); // NOSONAR

	private final FailedRecordTracker failureTracker;

	private boolean commitRecovered;

	protected FailedRecordProcessor(@Nullable BiConsumer<ConsumerRecord<?, ?>, Exception> recoverer, BackOff backOff) {
		this.failureTracker = new FailedRecordTracker(recoverer, backOff, this.logger);
	}

	/**
	 * Whether the offset for a recovered record should be committed.
	 * @return true to commit recovered record offsets.
	 */
	protected boolean isCommitRecovered() {
		return this.commitRecovered;
	}

	/**
	 * Set to true to commit the offset for a recovered record.
	 * @param commitRecovered true to commit.
	 */
	public void setCommitRecovered(boolean commitRecovered) {
		this.commitRecovered = commitRecovered;
	}

	/**
	 * Set a function to dynamically determine the {@link BackOff} to use, based on the
	 * consumer record and/or exception. If null is returned, the default BackOff will be
	 * used.
	 * @param backOffFunction the function.
	 * @since 2.6
	 */
	public void setBackOffFunction(BiFunction<ConsumerRecord<?, ?>, Exception, BackOff> backOffFunction) {
		this.failureTracker.setBackOffFunction(backOffFunction);
	}

	/**
	 * Set to false to immediately attempt to recover on the next attempt instead
	 * of repeating the BackOff cycle when recovery fails.
	 * @param resetStateOnRecoveryFailure false to retain state.
	 * @since 2.5.5
	 */
	public void setResetStateOnRecoveryFailure(boolean resetStateOnRecoveryFailure) {
		this.failureTracker.setResetStateOnRecoveryFailure(resetStateOnRecoveryFailure);
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
		this.failureTracker.setResetStateOnExceptionChange(resetStateOnExceptionChange);
	}

	/**
	 * Set one or more {@link RetryListener} to receive notifications of retries and
	 * recovery.
	 * @param listeners the listeners.
	 * @since 2.7
	 */
	public void setRetryListeners(RetryListener... listeners) {
		this.failureTracker.setRetryListeners(listeners);
	}

	@Override
	public int deliveryAttempt(TopicPartitionOffset topicPartitionOffset) {
		return this.failureTracker.deliveryAttempt(topicPartitionOffset);
	}

	/**
	 * Return a {@link RecoveryStrategy} to call to determine whether the first record in the
	 * list should be skipped.
	 * @param records the records.
	 * @param thrownException the exception.
	 * @return the {@link RecoveryStrategy}.
	 * @since 2.7
	 */
	protected RecoveryStrategy getRecoveryStrategy(List<ConsumerRecord<?, ?>> records, Exception thrownException) {
		return getRecoveryStrategy(records, null, thrownException);
	}

	/**
	 * Return a {@link RecoveryStrategy} to call to determine whether the first record in the
	 * list should be skipped.
	 * @param records the records.
	 * @param recoveryConsumer the consumer.
	 * @param thrownException the exception.
	 * @return the {@link RecoveryStrategy}.
	 * @since 2.8.4
	 */
	protected RecoveryStrategy getRecoveryStrategy(List<ConsumerRecord<?, ?>> records,
												@Nullable Consumer<?, ?> recoveryConsumer, Exception thrownException) {
		if (getClassifier().classify(thrownException)) {
			return this.failureTracker::recovered;
		}
		else {
			try {
				this.failureTracker.getRecoverer().accept(records.get(0), recoveryConsumer, thrownException);
				this.failureTracker.getRetryListeners().forEach(rl -> rl.recovered(records.get(0), thrownException));
			}
			catch (Exception ex) {
				if (records.size() > 0) {
					this.logger.error(ex, () -> "Recovery of record ("
							+ KafkaUtils.format(records.get(0)) + ") failed");
					this.failureTracker.getRetryListeners().forEach(rl ->
							rl.recoveryFailed(records.get(0), thrownException, ex));
				}
				return (rec, excep, cont, consumer) -> NEVER_SKIP_PREDICATE.test(rec, excep);
			}
			return (rec, excep, cont, consumer) -> ALWAYS_SKIP_PREDICATE.test(rec, excep);
		}
	}

	public void clearThreadState() {
		this.failureTracker.clearThreadState();
	}

}
