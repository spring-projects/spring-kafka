/*
 * Copyright 2021-present the original author or authors.
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
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.BiConsumer;
import java.util.stream.Stream;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.consumer.OffsetCommitCallback;
import org.apache.kafka.common.TopicPartition;
import org.jspecify.annotations.Nullable;

import org.springframework.kafka.KafkaException;
import org.springframework.kafka.KafkaException.Level;
import org.springframework.kafka.support.KafkaUtils;
import org.springframework.util.backoff.BackOff;

/**
 * Subclass of {@link FailedRecordProcessor} that can process (and recover) a batch. If
 * the listener throws a {@link BatchListenerFailedException}, the offsets prior to the
 * failed record are committed and the remaining records have seeks performed. When the
 * retries are exhausted, the failed record is sent to the recoverer instead of being
 * included in the seeks. If other exceptions are thrown, the fallback handler takes the processing.
 *
 * @author Gary Russell
 * @author Francois Rosiere
 * @author Wang Zhiyang
 * @author Artem Bilan
 * @since 2.8
 *
 */
public abstract class FailedBatchProcessor extends FailedRecordProcessor {

	private static final LoggingCommitCallback LOGGING_COMMIT_CALLBACK = new LoggingCommitCallback();

	private final CommonErrorHandler fallbackBatchHandler;

	/**
	 * Construct an instance with the provided properties.
	 * @param recoverer the recoverer.
	 * @param backOff the back off.
	 * @param fallbackHandler the fallback handler.
	 */
	public FailedBatchProcessor(@Nullable BiConsumer<ConsumerRecord<?, ?>, Exception> recoverer, BackOff backOff,
			CommonErrorHandler fallbackHandler) {

		this(recoverer, backOff, null, fallbackHandler);
	}

	/**
	 * Construct an instance with the provided properties.
	 * @param recoverer the recoverer.
	 * @param backOff the back off.
	 * @param backOffHandler the {@link BackOffHandler}
	 * @param fallbackHandler the fallback handler.
	 * @since 2.9
	 */
	public FailedBatchProcessor(@Nullable BiConsumer<ConsumerRecord<?, ?>, Exception> recoverer, BackOff backOff,
			@Nullable BackOffHandler backOffHandler, CommonErrorHandler fallbackHandler) {

		super(recoverer, backOff, backOffHandler);
		this.fallbackBatchHandler = fallbackHandler;
	}

	@Override
	public void setRetryListeners(RetryListener... listeners) {
		super.setRetryListeners(listeners);
		if (this.fallbackBatchHandler instanceof FallbackBatchErrorHandler handler) {
			handler.setRetryListeners(listeners);
		}
	}

	@Override
	public void setLogLevel(Level logLevel) {
		super.setLogLevel(logLevel);
		if (this.fallbackBatchHandler instanceof KafkaExceptionLogLevelAware handler) {
			handler.setLogLevel(logLevel);
		}
	}

	/**
	 * Set to {@code false} to not reclassify the exception if different from the previous
	 * failure. If the changed exception is classified as retryable, the existing back off
	 * sequence is used; a new sequence is not started. Default true. Only applies when
	 * the fallback batch error handler (for exceptions other than
	 * {@link BatchListenerFailedException}) is the default.
	 * @param reclassifyOnExceptionChange false to not reclassify.
	 * @since 2.9.7
	 */
	public void setReclassifyOnExceptionChange(boolean reclassifyOnExceptionChange) {
		if (this.fallbackBatchHandler instanceof FallbackBatchErrorHandler handler) {
			handler.setReclassifyOnExceptionChange(reclassifyOnExceptionChange);
		}
	}

	@Override
	protected void notRetryable(Stream<Class<? extends Exception>> notRetryable) {
		if (this.fallbackBatchHandler instanceof ExceptionClassifier handler) {
			notRetryable.forEach(handler::addNotRetryableExceptions);
		}
	}

	@Override
	public void setClassifications(Map<Class<? extends Throwable>, Boolean> classifications, boolean defaultValue) {
		super.setClassifications(classifications, defaultValue);
		if (this.fallbackBatchHandler instanceof ExceptionClassifier handler) {
			handler.setClassifications(classifications, defaultValue);
		}
	}

	@Override
	@Nullable
	public Boolean removeClassification(Class<? extends Exception> exceptionType) {
		Boolean removed = super.removeClassification(exceptionType);
		if (this.fallbackBatchHandler instanceof ExceptionClassifier handler) {
			handler.removeClassification(exceptionType);
		}
		return removed;
	}

	/**
	 * Return the fallback batch error handler.
	 * @return the handler.
	 * @since 2.8.8
	 */
	protected CommonErrorHandler getFallbackBatchHandler() {
		return this.fallbackBatchHandler;
	}

	protected void doHandle(Exception thrownException, ConsumerRecords<?, ?> data, Consumer<?, ?> consumer,
			MessageListenerContainer container, Runnable invokeListener) {

		handle(thrownException, data, consumer, container, invokeListener);
	}

	protected <K, V> ConsumerRecords<K, V> handle(Exception thrownException, ConsumerRecords<?, ?> data,
			Consumer<?, ?> consumer, MessageListenerContainer container, Runnable invokeListener) {

		BatchListenerFailedException batchListenerFailedException = getBatchListenerFailedException(thrownException);
		if (batchListenerFailedException == null) {
			this.logger.debug(thrownException, "Expected a BatchListenerFailedException; re-delivering full batch");
			fallback(thrownException, data, consumer, container, invokeListener);
		}
		else {
			getRetryListeners().forEach(listener -> listener.failedDelivery(data, thrownException, 1));
			ConsumerRecord<?, ?> record = batchListenerFailedException.getRecord();
			int index = record != null ? findIndex(data, record) : batchListenerFailedException.getIndex();
			if (index < 0 || index >= data.count()) {
				this.logger.warn(batchListenerFailedException, () -> {
					if (record != null) {
						return String.format("Record not found in batch: %s-%d@%d; re-seeking batch",
								record.topic(), record.partition(), record.offset());
					}
					else {
						return String.format("Record not found in batch, index %d out of bounds (0, %d); "
								+ "re-seeking batch", index, data.count() - 1);
					}
				});
				fallback(thrownException, data, consumer, container, invokeListener);
			}
			else {
				return seekOrRecover(thrownException, data, consumer, container, index);
			}
		}
		return ConsumerRecords.empty();
	}

	private void fallback(Exception thrownException, ConsumerRecords<?, ?> data, Consumer<?, ?> consumer,
			MessageListenerContainer container, Runnable invokeListener) {

		this.fallbackBatchHandler.handleBatch(thrownException, data, consumer, container, invokeListener);
	}

	private int findIndex(ConsumerRecords<?, ?> data, @Nullable ConsumerRecord<?, ?> record) {
		if (record == null) {
			return -1;
		}
		int i = 0;
		for (ConsumerRecord<?, ?> datum : data) {
			if (datum.topic().equals(record.topic()) && datum.partition() == record.partition()
					&& datum.offset() == record.offset()) {
				break;
			}
			i++;
		}
		return i;
	}

	@SuppressWarnings("unchecked")
	private <K, V> ConsumerRecords<K, V> seekOrRecover(Exception thrownException, @Nullable ConsumerRecords<?, ?> data,
			Consumer<?, ?> consumer, MessageListenerContainer container, int indexArg) {

		if (data == null) {
			return ConsumerRecords.empty();
		}
		List<ConsumerRecord<?, ?>> remaining = new ArrayList<>();
		int index = indexArg;
		Map<TopicPartition, OffsetAndMetadata> offsets = new HashMap<>();
		for (ConsumerRecord<?, ?> datum : data) {
			if (index-- > 0) {
				offsets.compute(new TopicPartition(datum.topic(), datum.partition()),
						(key, val) -> ListenerUtils.createOffsetAndMetadata(container, datum.offset() + 1));
			}
			else {
				remaining.add(datum);
			}
		}

		try {
			if (!offsets.isEmpty()) {
				commit(consumer, container, offsets);
			}
		}
		catch (Exception ex) {
			// Ignore and follow with seek below
		}

		if (isSeekAfterError()) {
			if (!remaining.isEmpty()) {
				SeekUtils.seekOrRecover(thrownException, remaining, consumer, container, false,
						getFailureTracker(), this.logger, getLogLevel());
				ConsumerRecord<?, ?> recovered = remaining.get(0);
				commit(consumer, container,
						Collections.singletonMap(new TopicPartition(recovered.topic(), recovered.partition()),
								ListenerUtils.createOffsetAndMetadata(container, recovered.offset() + 1)));
				if (remaining.size() > 1) {
					throw new KafkaException("Seek to current after exception", getLogLevel(), thrownException);
				}
			}
			return ConsumerRecords.empty();
		}
		else {
			if (!remaining.isEmpty()) {
				try {
					if (getFailureTracker().recovered(remaining.get(0), thrownException, container,
							consumer)) {
						remaining.remove(0);
					}
				}
				catch (Exception e) {
					if (SeekUtils.isBackoffException(thrownException)) {
						this.logger.debug(e, () -> KafkaUtils.format(remaining.get(0))
								+ " included in remaining due to retry back off " + thrownException);
					}
					else {
						this.logger.error(e, KafkaUtils.format(remaining.get(0))
								+ " included in remaining due to " + thrownException);
					}
				}
			}
			if (remaining.isEmpty()) {
				return ConsumerRecords.empty();
			}
			Map<TopicPartition, List<ConsumerRecord<K, V>>> remains = new HashMap<>();
			remaining.forEach(rec -> remains.computeIfAbsent(new TopicPartition(rec.topic(), rec.partition()),
					tp -> new ArrayList<>()).add((ConsumerRecord<K, V>) rec));
			return new ConsumerRecords<>(remains, Map.of());
		}
	}

	private static void commit(Consumer<?, ?> consumer, MessageListenerContainer container,
			Map<TopicPartition, OffsetAndMetadata> offsets) {

		ContainerProperties properties = container.getContainerProperties();
		if (properties.isSyncCommits()) {
			consumer.commitSync(offsets, properties.getSyncCommitTimeout());
		}
		else {
			OffsetCommitCallback commitCallback = properties.getCommitCallback();
			if (commitCallback == null) {
				commitCallback = LOGGING_COMMIT_CALLBACK;
			}
			consumer.commitAsync(offsets, commitCallback);
		}
	}

	@Nullable
	private static BatchListenerFailedException getBatchListenerFailedException(@Nullable Throwable throwableArg) {
		if (throwableArg == null || throwableArg instanceof BatchListenerFailedException) {
			return (BatchListenerFailedException) throwableArg;
		}

		BatchListenerFailedException target = null;

		Throwable throwable = throwableArg;
		Set<Throwable> checked = new HashSet<>();
		while (throwable.getCause() != null && !checked.contains(throwable.getCause())) {
			throwable = throwable.getCause();
			checked.add(throwable);

			if (throwable instanceof BatchListenerFailedException batchListenerFailedException) {
				target = batchListenerFailedException;
				break;
			}
		}

		return target;
	}

}
