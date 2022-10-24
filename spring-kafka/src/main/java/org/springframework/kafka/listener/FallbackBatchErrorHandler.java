/*
 * Copyright 2020-2022 the original author or authors.
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
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.function.BiConsumer;
import java.util.stream.Collectors;

import org.apache.commons.logging.LogFactory;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.TopicPartition;

import org.springframework.core.log.LogAccessor;
import org.springframework.kafka.KafkaException;
import org.springframework.lang.Nullable;
import org.springframework.util.Assert;
import org.springframework.util.backoff.BackOff;
import org.springframework.util.backoff.FixedBackOff;

/**
 * A batch error handler that invokes the listener according to the supplied
 * {@link BackOff}. The consumer is paused/polled/resumed before each retry in order to
 * avoid a rebalance. If/when retries are exhausted, the provided
 * {@link ConsumerRecordRecoverer} is invoked for each record in the batch. If the
 * recoverer throws an exception, or the thread is interrupted while sleeping, seeks are
 * performed so that the batch will be redelivered on the next poll.
 *
 * @author Gary Russell
 * @since 2.3.7
 *
 */
class FallbackBatchErrorHandler extends ExceptionClassifier implements CommonErrorHandler {

	private final LogAccessor logger = new LogAccessor(LogFactory.getLog(getClass()));

	private final BackOff backOff;

	private final BiConsumer<ConsumerRecords<?, ?>, Exception> recoverer;

	private final CommonErrorHandler seeker = new SeekAfterRecoverFailsOrInterrupted();

	private final ThreadLocal<Boolean> retrying = ThreadLocal.withInitial(() -> false);

	private final List<RetryListener> retryListeners = new ArrayList<>();

	private boolean ackAfterHandle = true;

	/**
	 * Construct an instance with a default {@link FixedBackOff} (unlimited attempts with
	 * a 5 second back off).
	 */
	FallbackBatchErrorHandler() {
		this(new FixedBackOff(), null);
	}

	/**
	 * Construct an instance with the provided {@link BackOff} and
	 * {@link ConsumerRecordRecoverer}. If the recoverer is {@code null}, the discarded
	 * records (topic-partition{@literal @}offset) will be logged.
	 * @param backOff the back off.
	 * @param recoverer the recoverer.
	 */
	FallbackBatchErrorHandler(BackOff backOff, @Nullable ConsumerRecordRecoverer recoverer) {
		this.backOff = backOff;
		this.recoverer = (crs, ex) -> {
			if (recoverer == null) {
				this.logger.error(ex, () -> "Records discarded: " + ErrorHandlingUtils.recordsToString(crs));
			}
			else {
				crs.spliterator().forEachRemaining(rec -> recoverer.accept(rec, ex));
			}
		};
	}

	/**
	 * Set one or more {@link RetryListener} to receive notifications of retries and
	 * recovery.
	 * @param listeners the listeners.
	 * @since 3.0
	 */
	public void setRetryListeners(RetryListener... listeners) {
		Assert.noNullElements(listeners, "'listeners' cannot have null elements");
		this.retryListeners.clear();
		this.retryListeners.addAll(Arrays.asList(listeners));
	}

	@Override
	public boolean isAckAfterHandle() {
		return this.ackAfterHandle;
	}

	@Override
	public void setAckAfterHandle(boolean ackAfterHandle) {
		this.ackAfterHandle = ackAfterHandle;
	}

	@Override
	public void handleBatch(Exception thrownException, @Nullable ConsumerRecords<?, ?> records,
			Consumer<?, ?> consumer, MessageListenerContainer container, Runnable invokeListener) {

		if (records == null || records.count() == 0) {
			this.logger.error(thrownException, "Called with no records; consumer exception");
			return;
		}
		this.retrying.set(true);
		try {
			ErrorHandlingUtils.retryBatch(thrownException, records, consumer, container, invokeListener, this.backOff,
					this.seeker, this.recoverer, this.logger, getLogLevel(), this.retryListeners, getClassifier());
		}
		finally {
			this.retrying.set(false);
		}
	}

	@Override
	public void onPartitionsAssigned(Consumer<?, ?> consumer, Collection<TopicPartition> partitions,
			Runnable publishPause) {

		if (this.retrying.get()) {
			consumer.pause(consumer.assignment());
			publishPause.run();
		}
	}

	private final class SeekAfterRecoverFailsOrInterrupted implements CommonErrorHandler {

		SeekAfterRecoverFailsOrInterrupted() {
		}

		@Override
		public void handleBatch(Exception thrownException, ConsumerRecords<?, ?> data, Consumer<?, ?> consumer,
				MessageListenerContainer container, Runnable invokeListener) {

			data.partitions()
					.stream()
					.collect(
							Collectors.toMap(tp -> tp,
									tp -> data.records(tp).get(0).offset(), (u, v) -> (long) v, LinkedHashMap::new))
					.forEach(consumer::seek);

			throw new KafkaException("Seek to current after exception", getLogLevel(), thrownException);

		}

	}

}
