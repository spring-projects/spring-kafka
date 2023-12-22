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
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.function.BiConsumer;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;

import org.springframework.classify.BinaryExceptionClassifier;
import org.springframework.core.log.LogAccessor;
import org.springframework.kafka.KafkaException;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.support.KafkaUtils;
import org.springframework.kafka.support.serializer.ErrorHandlingDeserializer;
import org.springframework.lang.Nullable;
import org.springframework.util.ClassUtils;
import org.springframework.util.backoff.BackOff;
import org.springframework.util.backoff.BackOffExecution;

/**
 * Utilities for error handling.
 *
 * @author Gary Russell
 * @author Andrii Pelesh
 * @author Antonio Tomac
 * @since 2.8
 *
 */
public final class ErrorHandlingUtils {

	static Runnable NO_OP = () -> { };

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
		if (childOrSingle instanceof ConsumerPauseResumeEventPublisher consumerPauseResumeEventPublisher) {
			consumerPauseResumeEventPublisher.publishConsumerPausedEvent(assignment, "For batch retry");
		}
		try {
			Exception recoveryException = thrownException;
			Exception lastException = unwrapIfNeeded(thrownException);
			Boolean retryable = classifier.classify(lastException);
			while (Boolean.TRUE.equals(retryable) && nextBackOff != BackOffExecution.STOP) {
				try {
					consumer.poll(Duration.ZERO);
				}
				catch (WakeupException we) {
					seeker.handleBatch(thrownException, records, consumer, container, NO_OP);
					throw new KafkaException("Woken up during retry", logLevel, we);
				}
				try {
					ListenerUtils.conditionalSleep(
							() -> container.isRunning() &&
									!container.isPauseRequested() &&
									records.partitions().stream().noneMatch(container::isPartitionPauseRequested),
							nextBackOff
					);
				}
				catch (InterruptedException e1) {
					Thread.currentThread().interrupt();
					seeker.handleBatch(thrownException, records, consumer, container, NO_OP);
					throw new KafkaException("Interrupted during retry", logLevel, e1);
				}
				if (!container.isRunning()) {
					throw new KafkaException("Container stopped during retries");
				}
				if (container.isPauseRequested() ||
						records.partitions().stream().anyMatch(container::isPartitionPauseRequested)) {
					seeker.handleBatch(thrownException, records, consumer, container, NO_OP);
					throw new KafkaException("Container paused requested during retries");
				}
				try {
					consumer.poll(Duration.ZERO);
				}
				catch (WakeupException we) {
					seeker.handleBatch(thrownException, records, consumer, container, NO_OP);
					throw new KafkaException("Woken up during retry", logLevel, we);
				}
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
				logger.error(ex, "Recoverer threw an exception; re-seeking batch");
				retryListeners.forEach(listener -> listener.recoveryFailed(records, thrownException, ex));
				seeker.handleBatch(thrownException, records, consumer, container, NO_OP);
			}
		}
		finally {
			Set<TopicPartition> assignment2 = consumer.assignment();
			consumer.resume(assignment2);
			if (childOrSingle instanceof ConsumerPauseResumeEventPublisher consumerPauseResumeEventPublisher) {
				consumerPauseResumeEventPublisher.publishConsumerResumedEvent(assignment2);
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
		return StreamSupport.stream(records.spliterator(), false)
				.map(KafkaUtils::format)
				.collect(Collectors.joining(","));
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

	/**
	 * Determine whether the key or value deserializer is an instance of
	 * {@link ErrorHandlingDeserializer}.
	 * @param <K> the key type.
	 * @param <V> the value type.
	 * @param consumerFactory the consumer factory.
	 * @param consumerOverrides consumer factory property overrides.
	 * @param isValue true to find the value deserializer.
	 * @param classLoader the class loader to load the deserializer class.
	 * @return true if the deserializer is an instance of
	 * {@link ErrorHandlingDeserializer}.
	 * @since 3.0.10
	 */
	public static <K, V> boolean checkDeserializer(ConsumerFactory<K, V> consumerFactory,
			Properties consumerOverrides, boolean isValue, ClassLoader classLoader) {

		Object deser = findDeserializerClass(consumerFactory, consumerOverrides, isValue);
		Class<?> deserializer = null;
		if (deser instanceof Class<?> deserClass) {
			deserializer = deserClass;
		}
		else if (deser instanceof String str) {
			try {
				deserializer = ClassUtils.forName(str, classLoader);
			}
			catch (ClassNotFoundException | LinkageError e) {
				throw new IllegalStateException(e);
			}
		}
		else if (deser != null) {
			throw new IllegalStateException("Deserializer must be a class or class name, not a " + deser.getClass());
		}
		return deserializer != null && ErrorHandlingDeserializer.class.isAssignableFrom(deserializer);
	}

	@Nullable
	private static <K, V> Object findDeserializerClass(ConsumerFactory<K, V> consumerFactory,
			Properties consumerOverrides, boolean isValue) {

		Map<String, Object> props = consumerFactory.getConfigurationProperties();
		Object configuredDeserializer = isValue
				? consumerFactory.getValueDeserializer()
				: consumerFactory.getKeyDeserializer();
		if (configuredDeserializer == null) {
			Object deser = consumerOverrides.get(isValue
					? ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG
					: ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG);
			if (deser == null) {
				deser = props.get(isValue
						? ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG
						: ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG);
			}
			return deser;
		}
		else {
			return configuredDeserializer.getClass();
		}
	}

}
