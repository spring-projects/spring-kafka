/*
 * Copyright 2021 the original author or authors.
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

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.errors.SerializationException;

import org.springframework.lang.Nullable;
import org.springframework.util.backoff.BackOff;

/**
 * An error handler that, for record listeners, seeks to the current offset for each topic
 * in the remaining records. Used to rewind partitions after a message failure so that it
 * can be replayed. For batch listeners, seeks to the current offset for each topic in a
 * batch of records. Used to rewind partitions after a message failure so that the batch
 * can be replayed. If the listener throws a {@link BatchListenerFailedException}, with
 * the failed record. The records before the record will have their offsets committed and
 * the partitions for the remaining records will be repositioned and/or the failed record
 * can be recovered and skipped. If some other exception is thrown, or a valid record is
 * not provided in the exception, error handling is delegated to a
 * {@link RetryingBatchErrorHandler} with this handler's {@link BackOff}. If the record is
 * recovered, its offset is committed. This is a replacement for the legacy
 * {@link SeekToCurrentErrorHandler} and {@link SeekToCurrentBatchErrorHandler} (but the
 * fallback now can send the messages to a recoverer after retries are completed instead
 * of retring indefinitely).
 *
 * @author Gary Russell
 *
 * @since 2.8
 *
 */
public class DefaultErrorHandler extends FailedBatchProcessor implements CommonErrorHandler {

	private boolean ackAfterHandle = true;

	/**
	 * Construct an instance with the default recoverer which simply logs the record after
	 * {@value SeekUtils#DEFAULT_MAX_FAILURES} (maxFailures) have occurred for a
	 * topic/partition/offset, with the default back off (9 retries, no delay).
	 */
	public DefaultErrorHandler() {
		this(null, SeekUtils.DEFAULT_BACK_OFF);
	}

	/**
	 * Construct an instance with the default recoverer which simply logs the record after
	 * the backOff returns STOP for a topic/partition/offset.
	 * @param backOff the {@link BackOff}.
	 */
	public DefaultErrorHandler(BackOff backOff) {
		this(null, backOff);
	}

	/**
	 * Construct an instance with the provided recoverer which will be called after
	 * {@value SeekUtils#DEFAULT_MAX_FAILURES} (maxFailures) have occurred for a
	 * topic/partition/offset.
	 * @param recoverer the recoverer.
	 */
	public DefaultErrorHandler(ConsumerRecordRecoverer recoverer) {
		this(recoverer, SeekUtils.DEFAULT_BACK_OFF);
	}

	/**
	 * Construct an instance with the provided recoverer which will be called after
	 * the backOff returns STOP for a topic/partition/offset.
	 * @param recoverer the recoverer; if null, the default (logging) recoverer is used.
	 * @param backOff the {@link BackOff}.
	 */
	public DefaultErrorHandler(@Nullable ConsumerRecordRecoverer recoverer, BackOff backOff) {
		super(recoverer, backOff, createFallback(backOff, recoverer));
	}

	private static CommonErrorHandler createFallback(BackOff backOff, @Nullable ConsumerRecordRecoverer recoverer) {
		return new ErrorHandlerAdapter(new RetryingBatchErrorHandler(backOff, recoverer));
	}

	/**
	 * {@inheritDoc}
	 * The container must be configured with
	 * {@link org.springframework.kafka.listener.ContainerProperties.AckMode#MANUAL_IMMEDIATE}.
	 * Whether or not the commit is sync or async depends on the container's syncCommits
	 * property.
	 * @param commitRecovered true to commit.
	 */
	@Override
	public void setCommitRecovered(boolean commitRecovered) { // NOSONAR enhanced javadoc
		super.setCommitRecovered(commitRecovered);
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
	public void handleRemaining(Exception thrownException, List<ConsumerRecord<?, ?>> records,
			Consumer<?, ?> consumer, MessageListenerContainer container) {

		SeekUtils.seekOrRecover(thrownException, records, consumer, container, isCommitRecovered(), // NOSONAR
				getRecoveryStrategy(records, thrownException), this.logger, getLogLevel());
	}

	@Override
	public void handleBatch(Exception thrownException, ConsumerRecords<?, ?> data, Consumer<?, ?> consumer,
			MessageListenerContainer container, Runnable invokeListener) {

		doHandle(thrownException, data, consumer, container, invokeListener);
	}

	@Override
	public void handleOtherException(Exception thrownException, Consumer<?, ?> consumer,
			MessageListenerContainer container, boolean batchListener) {

		if (thrownException instanceof SerializationException) {
			throw new IllegalStateException("This error handler cannot process 'SerializationException's directly; "
					+ "please consider configuring an 'ErrorHandlingDeserializer' in the value and/or key "
					+ "deserializer", thrownException);
		}
		else {
			throw new IllegalStateException("This error handler cannot process '"
					+ thrownException.getClass().getName()
					+ "'s; no record information is available", thrownException);
		}
	}

}
