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

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;

import org.springframework.lang.Nullable;
import org.springframework.util.backoff.BackOff;

/**
 * An error handler that seeks to the current offset for each topic in a batch of records.
 * Used to rewind partitions after a message failure so that the batch can be replayed. If
 * the listener throws a {@link BatchListenerFailedException}, with the failed record. The
 * records before the record will have their offsets committed and the partitions for the
 * remaining records will be repositioned and/or the failed record can be recovered and
 * skipped. If some other exception is thrown, or a valid record is not provided in the
 * exception, error handling is delegated to a {@link SeekToCurrentBatchErrorHandler} with
 * this handler's {@link BackOff}. If the record is recovered, its offset is committed.
 *
 * @deprecated in favor of {@link DefaultErrorHandler}.
 *
 * @author Gary Russell
 * @author Myeonghyeon Lee
 * @since 2.5
 *
 */
@Deprecated
public class RecoveringBatchErrorHandler extends FailedBatchProcessor
		implements ContainerAwareBatchErrorHandler {

	private boolean ackAfterHandle = true;

	/**
	 * Construct an instance with the default recoverer which simply logs the record after
	 * {@value SeekUtils#DEFAULT_MAX_FAILURES} (maxFailures) have occurred for a
	 * topic/partition/offset.
	 */
	public RecoveringBatchErrorHandler() {
		this(null, SeekUtils.DEFAULT_BACK_OFF);
	}

	/**
	 * Construct an instance with the default recoverer which simply logs the record after
	 * the backOff returns STOP for a topic/partition/offset.
	 * @param backOff the {@link BackOff}.
	 */
	public RecoveringBatchErrorHandler(BackOff backOff) {
		this(null, backOff);
	}

	/**
	 * Construct an instance with the provided recoverer which will be called after
	 * {@value SeekUtils#DEFAULT_MAX_FAILURES} (maxFailures) have occurred for a
	 * topic/partition/offset.
	 * @param recoverer the recoverer.
	 */
	public RecoveringBatchErrorHandler(BiConsumer<ConsumerRecord<?, ?>, Exception> recoverer) {
		this(recoverer, SeekUtils.DEFAULT_BACK_OFF);
	}

	/**
	 * Construct an instance with the provided recoverer which will be called after the
	 * backOff returns STOP for a topic/partition/offset.
	 * @param recoverer the recoverer; if null, the default (logging) recoverer is used.
	 * @param backOff the {@link BackOff}.
	 * @since 2.3
	 */
	public RecoveringBatchErrorHandler(@Nullable BiConsumer<ConsumerRecord<?, ?>, Exception> recoverer,
			BackOff backOff) {

		super(recoverer, backOff, createFallback(backOff));
	}

	private static CommonErrorHandler createFallback(BackOff backOff) {
		SeekToCurrentBatchErrorHandler eh = new SeekToCurrentBatchErrorHandler();
		eh.setBackOff(backOff);
		return new ErrorHandlerAdapter(eh);
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
	public void handle(Exception thrownException, @Nullable ConsumerRecords<?, ?> data, Consumer<?, ?> consumer,
			MessageListenerContainer container) {

		doHandle(thrownException, data, consumer, container, () -> { });
	}

}
