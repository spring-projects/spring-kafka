/*
 * Copyright 2020-2021 the original author or authors.
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

import java.util.function.BiConsumer;

import org.apache.commons.logging.LogFactory;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecords;

import org.springframework.core.log.LogAccessor;
import org.springframework.lang.Nullable;
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
public class RetryingBatchErrorHandler extends KafkaExceptionLogLevelAware
		implements ListenerInvokingBatchErrorHandler {

	private static final LogAccessor LOGGER = new LogAccessor(LogFactory.getLog(RetryingBatchErrorHandler.class));

	private final BackOff backOff;

	private final BiConsumer<ConsumerRecords<?, ?>, Exception> recoverer;

	private final CommonErrorHandler seeker = new ErrorHandlerAdapter(new SeekToCurrentBatchErrorHandler());

	private boolean ackAfterHandle = true;

	/**
	 * Construct an instance with a default {@link FixedBackOff} (unlimited attempts with
	 * a 5 second back off).
	 */
	public RetryingBatchErrorHandler() {
		this(new FixedBackOff(), null);
	}

	/**
	 * Construct an instance with the provided {@link BackOff} and
	 * {@link ConsumerRecordRecoverer}. If the recoverer is {@code null}, the discarded
	 * records (topic-partition{@literal @}offset) will be logged.
	 * @param backOff the back off.
	 * @param recoverer the recoverer.
	 */
	public RetryingBatchErrorHandler(BackOff backOff, @Nullable ConsumerRecordRecoverer recoverer) {
		this.backOff = backOff;
		this.recoverer = (crs, ex) -> {
			if (recoverer == null) {
				LOGGER.error(ex, () -> "Records discarded: " + ErrorHandlingUtils.recordsToString(crs));
			}
			else {
				crs.spliterator().forEachRemaining(rec -> recoverer.accept(rec, ex));
			}
		};
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
	public void handle(Exception thrownException, @Nullable ConsumerRecords<?, ?> records,
			Consumer<?, ?> consumer, MessageListenerContainer container, Runnable invokeListener) {

		if (records == null || records.count() == 0) {
			LOGGER.error(thrownException, "Called with no records; consumer exception");
			return;
		}
		ErrorHandlingUtils.retryBatch(thrownException, records, consumer, container, invokeListener, this.backOff,
				this.seeker, this.recoverer, LOGGER, getLogLevel());
	}

}
