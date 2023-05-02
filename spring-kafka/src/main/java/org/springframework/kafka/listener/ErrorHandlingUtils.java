/*
 * Copyright 2021-2023 the original author or authors.
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

import java.time.Duration;
import java.util.List;
import java.util.Set;
import java.util.function.BiConsumer;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.TopicPartition;

import org.springframework.classify.BinaryExceptionClassifier;
import org.springframework.core.log.LogAccessor;
import org.springframework.kafka.KafkaException;
import org.springframework.kafka.support.KafkaUtils;
import org.springframework.util.backoff.BackOff;
import org.springframework.util.backoff.BackOffExecution;

/**
 * Utilities for error handling.
 *
 * @author Gary Russell
 * @author Andrii Pelesh
 * @since 2.8
 *
 */
public final class ErrorHandlingUtils {

	private ErrorHandlingUtils() {
	}

	/**
	 * Retry a complete batch by pausing the consumer and then, in a loop, poll the
	 * consumer, wait for the next back off, then call the listener. When retries are
	 * exhausted, call the recoverer with the {@link ConsumerRecords}.
	 * @param thrownException the exception.
	 * @param records the records.
	 * @param consumer the consumer.
	 * @param container the container.
	 * @param invokeListener the {@link Runnable} to run (call the listener).
	 * @param backOff the backOff.
	 * @param seeker the common error handler that re-seeks the entire batch.
	 * @param recoverer the recoverer.
	 * @param logger the logger.
	 * @param logLevel the log level.
	 * @param retryListeners the retry listeners.
	 * @param classifier the exception classifier.
	 * @since 2.8.11
	 * @deprecated in favor of
	 * {@link #retryBatch(Exception, ConsumerRecords, Consumer, MessageListenerContainer, Runnable, BackOff, CommonErrorHandler, BiConsumer, LogAccessor, org.springframework.kafka.KafkaException.Level, List, BinaryExceptionClassifier, boolean)}.
	 */
	@Deprecated
	public static void retryBatch(Exception thrownException, ConsumerRecords<?, ?> records, Consumer<?, ?> consumer,
			MessageListenerContainer container, Runnable invokeListener, BackOff backOff,
			CommonErrorHandler seeker, BiConsumer<ConsumerRecords<?, ?>, Exception> recoverer, LogAccessor logger,
			KafkaException.Level logLevel, List<RetryListener> retryListeners, BinaryExceptionClassifier classifier) {

		retryBatch(thrownException, records, consumer, container, invokeListener, backOff, seeker, recoverer, logger,
				logLevel, retryListeners, classifier, false);
	}

	/**
	 * Retry a complete batch by pausing the consumer and then, in a loop, poll the
	 * consumer, wait for the next back off, then call the listener. When retries are
	 * exhausted, call the recoverer with the {@link ConsumerRecords}.
	 * @param thrownException the exception.
	 * @param records the records.
	 * @param consumer the consumer.
	 * @param container the container.
	 * @param invokeListener the {@link Runnable} to run (call the listener).
	 * @param backOff the backOff.
	 * @param seeker the common error handler that re-seeks the entire batch.
	 * @param recoverer the recoverer.
	 * @param logger the logger.
	 * @param logLevel the log level.
	 * @param retryListeners the retry listeners.
	 * @param classifier the exception classifier.
	 * @param reClassifyOnExceptionChange true to reset the state if a different exception
	 * is thrown during retry.
	 * @since 2.9.7
	 */
	public static void retryBatch(Exception thrownException, ConsumerRecords<?, ?> records, Consumer<?, ?> consumer,
			MessageListenerContainer container, Runnable invokeListener, BackOff backOff,
			CommonErrorHandler seeker, BiConsumer<ConsumerRecords<?, ?>, Exception> recoverer, LogAccessor logger,
			KafkaException.Level logLevel, List<RetryListener> retryListeners, BinaryExceptionClassifier classifier,
			boolean reClassifyOnExceptionChange) {

		BackOffExecution execution = backOff.start();
		long nextBackOff = execution.nextBackOff();
		String failed = null;
		Set<TopicPartition> assignment = consumer.assignment();
		consumer.pause(assignment);
		int attempt = 1;
		listen(retryListeners, records, thrownException, attempt++);
		ConsumerRecord<?, ?> first = records.iterator().next();
		MessageListenerContainer childOrSingle = container.getContainerFor(first.topic(), first.partition());
		if (childOrSingle instanceof ConsumerPauseResumeEventPublisher) {
			((ConsumerPauseResumeEventPublisher) childOrSingle)
					.publishConsumerPausedEvent(assignment, "For batch retry");
		}
		try {
			Exception recoveryException = thrownException;
			Exception lastException = unwrapIfNeeded(thrownException);
			Boolean retryable = classifier.classify(lastException);
			while (Boolean.TRUE.equals(retryable) && nextBackOff != BackOffExecution.STOP) {
				consumer.poll(Duration.ZERO);
				try {
					ListenerUtils.stoppableSleep(container, nextBackOff);
				}
				catch (InterruptedException e1) {
					Thread.currentThread().interrupt();
					seeker.handleBatch(thrownException, records, consumer, container, () -> { });
					throw new KafkaException("Interrupted during retry", logLevel, e1);
				}
				if (!container.isRunning()) {
					throw new KafkaException("Container stopped during retries");
				}
				consumer.poll(Duration.ZERO);
				try {
					invokeListener.run();
					return;
				}
				catch (Exception ex) {
					listen(retryListeners, records, ex, attempt++);
					if (failed == null) {
						failed = recordsToString(records);
					}
					String toLog = failed;
					logger.debug(ex, () -> "Retry failed for: " + toLog);
					recoveryException = ex;
					Exception newException = unwrapIfNeeded(ex);
					if (reClassifyOnExceptionChange && !newException.getClass().equals(lastException.getClass())
							&& !classifier.classify(newException)) {

						break;
					}
				}
				nextBackOff = execution.nextBackOff();
			}
			try {
				recoverer.accept(records, recoveryException);
				final Exception finalRecoveryException = recoveryException;
				retryListeners.forEach(listener -> listener.recovered(records, finalRecoveryException));
			}
			catch (Exception ex) {
				logger.error(ex, () -> "Recoverer threw an exception; re-seeking batch");
				retryListeners.forEach(listener -> listener.recoveryFailed(records, thrownException, ex));
				seeker.handleBatch(thrownException, records, consumer, container, () -> { });
			}
		}
		finally {
			Set<TopicPartition> assignment2 = consumer.assignment();
			consumer.resume(assignment2);
			if (childOrSingle instanceof ConsumerPauseResumeEventPublisher) {
				((ConsumerPauseResumeEventPublisher) childOrSingle).publishConsumerResumedEvent(assignment2);
			}
		}
	} // NOSONAR NCSS line count

	private static void listen(List<RetryListener> listeners, ConsumerRecords<?, ?> records,
			Exception thrownException, int attempt) {

		listeners.forEach(listener -> listener.failedDelivery(records, thrownException, attempt));
	}

	/**
	 * Represent the records as a comma-delimited String of {@code topic-part@offset}.
	 * @param records the records.
	 * @return the String.
	 */
	public static String recordsToString(ConsumerRecords<?, ?> records) {
		StringBuffer sb = new StringBuffer();
		records.spliterator().forEachRemaining(rec -> sb
				.append(KafkaUtils.format(rec))
				.append(','));
		sb.deleteCharAt(sb.length() - 1);
		return sb.toString();
	}

	/**
	 * Remove a {@link TimestampedException}, if present.
	 * Remove a {@link ListenerExecutionFailedException}, if present.
	 * @param exception the exception.
	 * @return the unwrapped cause or cause of cause.
	 * @since 2.8.11
	 */
	public static Exception unwrapIfNeeded(Exception exception) {
		Exception theEx = exception;
		if (theEx instanceof TimestampedException && theEx.getCause() instanceof Exception cause) {
			theEx = cause;
		}
		if (theEx instanceof ListenerExecutionFailedException && theEx.getCause() instanceof Exception cause) {
			theEx = cause;
		}
		return theEx;
	}

	/**
	 * Find the root cause, ignoring any {@link ListenerExecutionFailedException} and
	 * {@link TimestampedException}.
	 * @param exception the exception to examine.
	 * @return the root cause.
	 * @since 3.0.7
	 */
	public static Exception findRootCause(Exception exception) {
		Exception realException = exception;
		while ((realException  instanceof ListenerExecutionFailedException
				|| realException instanceof TimestampedException)
						&& realException.getCause() instanceof Exception cause) {

			realException = cause;
		}
		return realException;
	}

}
