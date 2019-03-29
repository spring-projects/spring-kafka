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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.BiConsumer;
import java.util.function.BiPredicate;

import org.apache.commons.logging.LogFactory;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.consumer.OffsetCommitCallback;
import org.apache.kafka.common.TopicPartition;

import org.springframework.classify.BinaryExceptionClassifier;
import org.springframework.core.log.LogAccessor;
import org.springframework.kafka.KafkaException;
import org.springframework.kafka.listener.ContainerProperties.AckMode;
import org.springframework.kafka.support.SeekUtils;
import org.springframework.kafka.support.serializer.DeserializationException;
import org.springframework.lang.Nullable;
import org.springframework.messaging.converter.MessageConversionException;
import org.springframework.messaging.handler.invocation.MethodArgumentResolutionException;
import org.springframework.util.Assert;

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
public class SeekToCurrentErrorHandler implements ContainerAwareErrorHandler {

	private static final BiPredicate<ConsumerRecord<?, ?>, Exception> ALWAYS_SKIP_PREDICATE = (r, e) -> true;

	protected static final LogAccessor LOGGER =
			new LogAccessor(LogFactory.getLog(SeekToCurrentErrorHandler.class)); // NOSONAR visibility

	private static final LoggingCommitCallback LOGGING_COMMIT_CALLBACK = new LoggingCommitCallback();

	private final FailedRecordTracker failureTracker;

	private boolean commitRecovered;

	private BinaryExceptionClassifier classifier;

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
		this.classifier = configureDefaultClassifier();
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

	/**
	 * Return the exception classifier.
	 * @return the classifier.
	 * @since 2.3
	 */
	protected BinaryExceptionClassifier getClassifier() {
		return this.classifier;
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
	 */
	public void setClassifier(BinaryExceptionClassifier classifier) {
		Assert.notNull(classifier, "'classifier' + cannot be null");
		classifier.setTraverseCauses(true);
		this.classifier = classifier;
	}

	/**
	 * Add an exception type to the default list; if and only if an external classifier
	 * has not been provided. By default, the following exceptions will not be retried:
	 * <ul>
	 * <li>{@link DeserializationException}</li>
	 * <li>{@link MessageConversionException}</li>
	 * <li>{@link MethodArgumentResolutionException}</li>
	 * <li>{@link NoSuchMethodException}</li>
	 * <li>{@link ClassCastException}</li>
	 * </ul>
	 * All others will be retried.
	 * @param exceptionType the exception type.
	 * @since 2.3
	 * @see #removeNotRetryableException(Class)
	 * @see #setClassifier(BinaryExceptionClassifier)
	 */
	public void addNotRetryableException(Class<? extends Exception> exceptionType) {
		Assert.isTrue(this.classifier instanceof ExtendedBinaryExceptionClassifier,
				"Cannot add exception types to a supplied classifier");
		((ExtendedBinaryExceptionClassifier) this.classifier).getClassified().put(exceptionType, false);
	}

	/**
	 * Remove an exception type from the configured list; if and only if an external
	 * classifier has not been provided. By default, the following exceptions will not be
	 * retried:
	 * <ul>
	 * <li>{@link DeserializationException}</li>
	 * <li>{@link MessageConversionException}</li>
	 * <li>{@link MethodArgumentResolutionException}</li>
	 * <li>{@link NoSuchMethodException}</li>
	 * <li>{@link ClassCastException}</li>
	 * </ul>
	 * All others will be retried.
	 * @param exceptionType the exception type.
	 * @return true if the removal was successful.
	 * @since 2.3
	 * @see #addNotRetryableException(Class)
	 * @see #setClassifier(BinaryExceptionClassifier)
	 */
	public boolean removeNotRetryableException(Class<? extends Exception> exceptionType) {
		Assert.isTrue(this.classifier instanceof ExtendedBinaryExceptionClassifier,
				"Cannot remove exception types from a supplied classifier");
		return ((ExtendedBinaryExceptionClassifier) this.classifier).getClassified().remove(exceptionType);
	}

	@Override
	public void handle(Exception thrownException, List<ConsumerRecord<?, ?>> records,
			Consumer<?, ?> consumer, MessageListenerContainer container) {

		if (!SeekUtils.doSeeks(records, consumer, thrownException, true, getSkipPredicate(records, thrownException),
				LOGGER)) {
			throw new KafkaException("Seek to current after exception", thrownException);
		}
		if (this.commitRecovered) {
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

	private BiPredicate<ConsumerRecord<?, ?>, Exception> getSkipPredicate(List<ConsumerRecord<?, ?>> records,
			Exception thrownException) {

		if (this.classifier.classify(thrownException)) {
			return this.failureTracker::skip;
		}
		else {
			this.failureTracker.getRecoverer().accept(records.get(0), thrownException);
			return ALWAYS_SKIP_PREDICATE;
		}
	}

	@Override
	public void clearThreadState() {
		this.failureTracker.clearThreadState();
	}

	private static BinaryExceptionClassifier configureDefaultClassifier() {
		Map<Class<? extends Throwable>, Boolean> classified = new HashMap<>();
		classified.put(DeserializationException.class, false);
		classified.put(MessageConversionException.class, false);
		classified.put(MethodArgumentResolutionException.class, false);
		classified.put(NoSuchMethodException.class, false);
		classified.put(ClassCastException.class, false);
		ExtendedBinaryExceptionClassifier defaultClassifier = new ExtendedBinaryExceptionClassifier(classified, true);
		defaultClassifier.setTraverseCauses(true);
		return defaultClassifier;
	}

	/**
	 * Extended to provide visibility to the current classified exceptions.
	 *
	 * @author Gary Russell
	 *
	 * @since 2.3
	 *
	 */
	@SuppressWarnings("serial")
	private static class ExtendedBinaryExceptionClassifier extends BinaryExceptionClassifier {


		ExtendedBinaryExceptionClassifier(Map<Class<? extends Throwable>, Boolean> typeMap, boolean defaultValue) {
			super(typeMap, defaultValue);
		}

		@Override
		protected Map<Class<? extends Throwable>, Boolean> getClassified() { // NOSONAR worthless override
			return super.getClassified();
		}

	}

}
