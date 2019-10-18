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

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.consumer.OffsetCommitCallback;
import org.apache.kafka.common.TopicPartition;

import org.springframework.classify.BinaryExceptionClassifier;
import org.springframework.kafka.KafkaException;
import org.springframework.kafka.listener.ContainerProperties.AckMode;
import org.springframework.kafka.support.SeekUtils;
import org.springframework.kafka.support.serializer.DeserializationException;
import org.springframework.lang.Nullable;
import org.springframework.messaging.converter.MessageConversionException;
import org.springframework.messaging.handler.invocation.MethodArgumentResolutionException;
import org.springframework.util.Assert;
import org.springframework.util.backoff.BackOff;
import org.springframework.util.backoff.FixedBackOff;

/**
 * An error handler that seeks to the current offset for each topic in the remaining
 * records. Used to rewind partitions after a message failure so that it can be
 * replayed.
 *
 * @author Gary Russell
 * @author Artem Bilan
 *
 * @since 2.0.1
 *
 */
public class SeekToCurrentErrorHandler extends FailedRecordProcessor implements ContainerAwareErrorHandler {

	private static final LoggingCommitCallback LOGGING_COMMIT_CALLBACK = new LoggingCommitCallback();

	private boolean ackAfterHandle = true;

	/**
	 * Construct an instance with the default recoverer which simply logs the record after
	 * {@value SeekUtils#DEFAULT_MAX_FAILURES} (maxFailures) have occurred for a
	 * topic/partition/offset.
	 * @since 2.2
	 */
	public SeekToCurrentErrorHandler() {
		this(null, SeekUtils.DEFAULT_BACK_OFF);
	}

	/**
	 * Construct an instance with the default recoverer which simply logs the record after
	 * 'maxFailures' have occurred for a topic/partition/offset.
	 * @param maxFailures the maxFailures; a negative value is treated as infinity.
	 * @deprecated in favor of {@link #SeekToCurrentErrorHandler(BackOff)}.
	 * <b>IMPORTANT</b> When using a {@link FixedBackOff}, the maxAttempts property
	 * represents retries (one less than maxFailures). To retry indefinitely, use a
	 * fixed or exponential {@link BackOff} configured appropriately.
	 * To use the other constructor with the semantics of this one, with maxFailures
	 * equal to 3, use {@code new SeekToCurrentErrorHandler(new FixedBackOff(0L, 2L)}.
	 * @since 2.2.1
	 */
	@Deprecated
	public SeekToCurrentErrorHandler(int maxFailures) {
		this(null, maxFailures);
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
	 * maxFailures have occurred for a topic/partition/offset.
	 * @param recoverer the recoverer; if null, the default (logging) recoverer is used.
	 * @param maxFailures the maxFailures; a negative value is treated as infinity.
	 * @deprecated in favor of {@link #SeekToCurrentErrorHandler(BiConsumer, BackOff)}.
	 * <b>IMPORTANT</b> When using a {@link FixedBackOff}, the maxAttempts property
	 * represents retries (one less than maxFailures). To retry indefinitely, use a
	 * fixed or exponential {@link BackOff} configured appropriately.
	 * To use the other constructor with the semantics of this one, with maxFailures
	 * equal to 3, use
	 * {@code new SeekToCurrentErrorHandler(recoverer, new FixedBackOff(0L, 2L)}.
	 * @since 2.2
	 */
	@Deprecated
	public SeekToCurrentErrorHandler(@Nullable BiConsumer<ConsumerRecord<?, ?>, Exception> recoverer, int maxFailures) {
		// Remove super CTOR when this is removed.
		super(recoverer, maxFailures);
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
	 * The container
	 * must be configured with {@link AckMode#MANUAL_IMMEDIATE}. Whether or not
	 * the commit is sync or async depends on the container's syncCommits
	 * property.
	 * @param commitRecovered true to commit.
	 */
	@Override
	public void setCommitRecovered(boolean commitRecovered) { // NOSONAR enhanced javadoc
		super.setCommitRecovered(commitRecovered);
	}

	/**
	 * Set an exception classifier to determine whether the exception should cause a retry
	 * (until exhaustion) or not. If not, we go straight to the recoverer. By default,
	 * the following exceptions will not be retried:
	 * <ul>
	 * <li>{@link DeserializationException}</li>
	 * <li>{@link MessageConversionException}</li>
	 * <li>{@link MethodArgumentResolutionException}</li>
	 * <li>{@link NoSuchMethodException}</li>
	 * <li>{@link ClassCastException}</li>
	 * </ul>
	 * All others will be retried.
	 * The classifier's {@link BinaryExceptionClassifier#setTraverseCauses(boolean) traverseCauses}
	 * will be set to true because the container always wraps exceptions in a
	 * {@link ListenerExecutionFailedException}.
	 * This replaces the default classifier.
	 * @param classifier the classifier.
	 * @since 2.3
	 * @deprecated in favor of {@link #setClassifications(Map, boolean)}.
	 */
	@Override
	@Deprecated
	public void setClassifier(BinaryExceptionClassifier classifier) {
		Assert.notNull(classifier, "'classifier' + cannot be null");
		classifier.setTraverseCauses(true);
		super.setClassifier(classifier);
	}

	@Override
	public void handle(Exception thrownException, List<ConsumerRecord<?, ?>> records,
			Consumer<?, ?> consumer, MessageListenerContainer container) {

		if (!SeekUtils.doSeeks(records, consumer, thrownException, true, getSkipPredicate(records, thrownException),
				LOGGER)) {
			throw new KafkaException("Seek to current after exception", thrownException);
		}
		if (isCommitRecovered()) {
			if (container.getContainerProperties().getAckMode().equals(AckMode.MANUAL_IMMEDIATE)) {
				ConsumerRecord<?, ?> record = records.get(0);
				Map<TopicPartition, OffsetAndMetadata> offsetToCommit = Collections.singletonMap(
						new TopicPartition(record.topic(), record.partition()),
						new OffsetAndMetadata(record.offset() + 1));
				if (container.getContainerProperties().isSyncCommits()) {
					consumer.commitSync(offsetToCommit, container.getContainerProperties().getSyncCommitTimeout());
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
				LOGGER.warn(() -> "'commitRecovered' ignored, container AckMode must be MANUAL_IMMEDIATE, not "
						+ container.getContainerProperties().getAckMode());
			}
		}
	}

	@Override
	public boolean isAckAfterHandle() {
		return this.ackAfterHandle;
	}

	/**
	 * Set to false to tell the container to NOT commit the offset for a recovered record.
	 * @param ackAfterHandle false to suppress committing the offset.
	 * @since 2.3.2
	 */
	public void setAckAfterHandle(boolean ackAfterHandle) {
		this.ackAfterHandle = ackAfterHandle;
	}

}
