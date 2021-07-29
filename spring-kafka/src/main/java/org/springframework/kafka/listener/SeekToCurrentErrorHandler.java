/*
 * Copyright 2017-2021 the original author or authors.
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

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import org.springframework.lang.Nullable;
import org.springframework.util.backoff.BackOff;

/**
 * An error handler that seeks to the current offset for each topic in the remaining
 * records. Used to rewind partitions after a message failure so that it can be
 * replayed.
 *
 * @deprecated in favor of {@link DefaultErrorHandler}.
 *
 * @author Gary Russell
 * @author Artem Bilan
 *
 * @since 2.0.1
 *
 */
@Deprecated
public class SeekToCurrentErrorHandler extends FailedRecordProcessor implements ContainerAwareErrorHandler {

	private boolean ackAfterHandle = true;

	/**
	 * Construct an instance with the default recoverer which simply logs the record after
	 * {@value SeekUtils#DEFAULT_MAX_FAILURES} (maxFailures) have occurred for a
	 * topic/partition/offset, with the default back off (9 retries, no delay).
	 * @since 2.2
	 */
	public SeekToCurrentErrorHandler() {
		this(null, SeekUtils.DEFAULT_BACK_OFF);
	}

	/**
	 * Construct an instance with the default recoverer which simply logs the record after
	 * the backOff returns STOP for a topic/partition/offset.
	 * @param backOff the {@link BackOff}.
	 * @since 2.3
	 */
	public SeekToCurrentErrorHandler(BackOff backOff) {
		this(null, backOff);
	}

	/**
	 * Construct an instance with the provided recoverer which will be called after
	 * {@value SeekUtils#DEFAULT_MAX_FAILURES} (maxFailures) have occurred for a
	 * topic/partition/offset.
	 * @param recoverer the recoverer.
	 * @since 2.2
	 */
	public SeekToCurrentErrorHandler(BiConsumer<ConsumerRecord<?, ?>, Exception> recoverer) {
		this(recoverer, SeekUtils.DEFAULT_BACK_OFF);
	}

	/**
	 * Construct an instance with the provided recoverer which will be called after
	 * the backOff returns STOP for a topic/partition/offset.
	 * @param recoverer the recoverer; if null, the default (logging) recoverer is used.
	 * @param backOff the {@link BackOff}.
	 * @since 2.3
	 */
	public SeekToCurrentErrorHandler(@Nullable BiConsumer<ConsumerRecord<?, ?>, Exception> recoverer, BackOff backOff) {
		super(recoverer, backOff);
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
	public void handle(Exception thrownException, @Nullable List<ConsumerRecord<?, ?>> records,
			Consumer<?, ?> consumer, MessageListenerContainer container) {

		SeekUtils.seekOrRecover(thrownException, records, consumer, container, isCommitRecovered(), // NOSONAR
				getRecoveryStrategy(records, thrownException), this.logger, getLogLevel());
	}

}
