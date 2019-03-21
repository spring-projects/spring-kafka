/*
 * Copyright 2017-2019 the original author or authors.
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

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.function.BiConsumer;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.consumer.OffsetCommitCallback;
import org.apache.kafka.common.TopicPartition;

import org.springframework.kafka.KafkaException;
import org.springframework.kafka.listener.ContainerProperties.AckMode;
import org.springframework.kafka.support.SeekUtils;
import org.springframework.lang.Nullable;

/**
 * An error handler that seeks to the current offset for each topic in the remaining
 * records. Used to rewind partitions after a message failure so that it can be
 * replayed.
 *
 * @author Gary Russell
 * @since 2.0.1
 *
 */
public class SeekToCurrentErrorHandler implements ContainerAwareErrorHandler {

	protected static final Log LOGGER = LogFactory.getLog(SeekToCurrentErrorHandler.class); // NOSONAR visibility

	private static final LoggingCommitCallback LOGGING_COMMIT_CALLBACK = new LoggingCommitCallback();

	private final FailedRecordTracker failureTracker;

	private boolean commitRecovered;

	/**
	 * Construct an instance with the default recoverer which simply logs the record after
	 * {@value SeekUtils#DEFAULT_MAX_FAILURES} (maxFailures) have occurred for a
	 * topic/partition/offset.
	 * @since 2.2
	 */
	public SeekToCurrentErrorHandler() {
		this(null, SeekUtils.DEFAULT_MAX_FAILURES);
	}

	/**
	 * Construct an instance with the default recoverer which simply logs the record after
	 * 'maxFailures' have occurred for a topic/partition/offset.
	 * @param maxFailures the maxFailures; a negative value is treated as infinity.
	 * @since 2.2.1
	 */
	public SeekToCurrentErrorHandler(int maxFailures) {
		this(null, maxFailures);
	}

	/**
	 * Construct an instance with the provided recoverer which will be called after
	 * {@value SeekUtils#DEFAULT_MAX_FAILURES} (maxFailures) have occurred for a
	 * topic/partition/offset.
	 * @param recoverer the recoverer.
	 * @since 2.2
	 */
	public SeekToCurrentErrorHandler(BiConsumer<ConsumerRecord<?, ?>, Exception> recoverer) {
		this(recoverer, SeekUtils.DEFAULT_MAX_FAILURES);
	}

	/**
	 * Construct an instance with the provided recoverer which will be called after
	 * maxFailures have occurred for a topic/partition/offset.
	 * @param recoverer the recoverer; if null, the default (logging) recoverer is used.
	 * @param maxFailures the maxFailures; a negative value is treated as infinity.
	 * @since 2.2
	 */
	public SeekToCurrentErrorHandler(@Nullable BiConsumer<ConsumerRecord<?, ?>, Exception> recoverer, int maxFailures) {
		this.failureTracker = new FailedRecordTracker(recoverer, maxFailures, LOGGER);
	}

	/**
	 * Whether the offset for a recovered record should be committed.
	 * @return true to commit recovered record offsets.
	 * @since 2.2.4
	 */
	protected boolean isCommitRecovered() {
		return this.commitRecovered;
	}

	/**
	 * Set to true to commit the offset for a recovered record. The container
	 * must be configured with {@link AckMode#MANUAL_IMMEDIATE}. Whether or not
	 * the commit is sync or async depends on the container's syncCommits
	 * property.
	 * @param commitRecovered true to commit.
	 * @since 2.2.4
	 */
	public void setCommitRecovered(boolean commitRecovered) {
		this.commitRecovered = commitRecovered;
	}

	@Override
	public void handle(Exception thrownException, List<ConsumerRecord<?, ?>> records,
			Consumer<?, ?> consumer, MessageListenerContainer container) {

		if (!SeekUtils.doSeeks(records, consumer, thrownException, true, this.failureTracker::skip, LOGGER)) {
			throw new KafkaException("Seek to current after exception", thrownException);
		}
		else if (this.commitRecovered) {
			if (container.getContainerProperties().getAckMode().equals(AckMode.MANUAL_IMMEDIATE)) {
				ConsumerRecord<?, ?> record = records.get(0);
				Map<TopicPartition, OffsetAndMetadata> offsetToCommit = Collections.singletonMap(
						new TopicPartition(record.topic(), record.partition()),
						new OffsetAndMetadata(record.offset() + 1));
				if (container.getContainerProperties().isSyncCommits()) {
					consumer.commitSync(offsetToCommit);
				}
				else {
					OffsetCommitCallback commitCallback = container.getContainerProperties().getCommitCallback();
					if (commitCallback == null) {
						commitCallback = LOGGING_COMMIT_CALLBACK;
					}
					consumer.commitAsync(offsetToCommit, commitCallback);
				}
			}
			else {
				LOGGER.warn("'commitRecovered' ignored, container AckMode must be MANUAL_IMMEDIATE");
			}
		}
	}

	@Override
	public void clearThreadState() {
		this.failureTracker.clearThreadState();
	}

}
