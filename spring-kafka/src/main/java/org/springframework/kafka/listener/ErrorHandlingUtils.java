/*
 * Copyright 2021-2022 the original author or authors.
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
import java.util.Set;
import java.util.function.BiConsumer;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.TopicPartition;

import org.springframework.core.log.LogAccessor;
import org.springframework.kafka.KafkaException;
import org.springframework.util.backoff.BackOff;
import org.springframework.util.backoff.BackOffExecution;

/**
 * Utilities for error handling.
 *
 * @author Gary Russell
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
	 */
	public static void retryBatch(Exception thrownException, ConsumerRecords<?, ?> records, Consumer<?, ?> consumer,
			MessageListenerContainer container, Runnable invokeListener, BackOff backOff,
			CommonErrorHandler seeker, BiConsumer<ConsumerRecords<?, ?>, Exception> recoverer, LogAccessor logger,
			KafkaException.Level logLevel) {

		BackOffExecution execution = backOff.start();
		long nextBackOff = execution.nextBackOff();
		String failed = null;
		Set<TopicPartition> assignment = consumer.assignment();
		consumer.pause(assignment);
		if (container instanceof KafkaMessageListenerContainer) {
			((KafkaMessageListenerContainer<?, ?>) container).publishConsumerPausedEvent(assignment, "For batch retry");
		}
		try {
			while (nextBackOff != BackOffExecution.STOP) {
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
				try {
					invokeListener.run();
					return;
				}
				catch (Exception e) {
					if (failed == null) {
						failed = recordsToString(records);
					}
					String toLog = failed;
					logger.debug(e, () -> "Retry failed for: " + toLog);
				}
				nextBackOff = execution.nextBackOff();
			}
			try {
				recoverer.accept(records, thrownException);
			}
			catch (Exception e) {
				logger.error(e, () -> "Recoverer threw an exception; re-seeking batch");
				seeker.handleBatch(thrownException, records, consumer, container, () -> { });
			}
		}
		finally {
			Set<TopicPartition> assignment2 = consumer.assignment();
			consumer.resume(assignment2);
			if (container instanceof KafkaMessageListenerContainer) {
				((KafkaMessageListenerContainer<?, ?>) container).publishConsumerResumedEvent(assignment2);
			}
		}
	}

	/**
	 * Represent the records as a comma-delimited String of {@code topic-part@offset}.
	 * @param records the records.
	 * @return the String.
	 */
	@SuppressWarnings("deprecation")
	public static String recordsToString(ConsumerRecords<?, ?> records) {
		StringBuffer sb = new StringBuffer();
		records.spliterator().forEachRemaining(rec -> sb
				.append(ListenerUtils.recordToString(rec, true))
				.append(','));
		sb.deleteCharAt(sb.length() - 1);
		return sb.toString();
	}

}
