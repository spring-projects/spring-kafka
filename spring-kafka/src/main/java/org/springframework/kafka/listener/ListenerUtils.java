/*
 * Copyright 2017-2023 the original author or authors.
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

import java.util.Map;
import java.util.function.Supplier;

import org.apache.kafka.clients.consumer.OffsetAndMetadata;

import org.springframework.util.Assert;
import org.springframework.util.backoff.BackOff;
import org.springframework.util.backoff.BackOffExecution;

/**
 * Listener utilities.
 *
 * @author Gary Russell
 * @author Francois Rosiere
 * @author Antonio Tomac
 * @since 2.0
 *
 */
public final class ListenerUtils {

	private ListenerUtils() {
	}

	private static final int DEFAULT_SLEEP_INTERVAL = 100;

	private static final int SMALL_SLEEP_INTERVAL = 10;

	private static final long SMALL_INTERVAL_THRESHOLD = 500;

	/**
	 * Determine the type of the listener.
	 * @param listener the listener.
	 * @return the {@link ListenerType}.
	 */
	public static ListenerType determineListenerType(Object listener) {
		Assert.notNull(listener, "Listener cannot be null");
		ListenerType listenerType;
		if (listener instanceof AcknowledgingConsumerAwareMessageListener
				|| listener instanceof BatchAcknowledgingConsumerAwareMessageListener) {
			listenerType = ListenerType.ACKNOWLEDGING_CONSUMER_AWARE;
		}
		else if (listener instanceof ConsumerAwareMessageListener
				|| listener instanceof BatchConsumerAwareMessageListener) {
			listenerType = ListenerType.CONSUMER_AWARE;
		}
		else if (listener instanceof AcknowledgingMessageListener
				|| listener instanceof BatchAcknowledgingMessageListener) {
			listenerType = ListenerType.ACKNOWLEDGING;
		}
		else if (listener instanceof GenericMessageListener) {
			listenerType = ListenerType.SIMPLE;
		}
		else {
			throw new IllegalArgumentException("Unsupported listener type: " + listener.getClass().getName());
		}
		return listenerType;
	}

	/**
	 * Sleep according to the {@link BackOff}; when the {@link BackOffExecution} returns
	 * {@link BackOffExecution#STOP} sleep for the previous backOff.
	 * @param backOff the {@link BackOff} to create a new {@link BackOffExecution}.
	 * @param executions a thread local containing the {@link BackOffExecution} for this
	 * thread.
	 * @param lastIntervals a thread local containing the previous {@link BackOff}
	 * interval for this thread.
	 * @param container the container or parent container.
	 * @throws InterruptedException if the thread is interrupted.
	 * @since 2.7
	 * @deprecated in favor of
	 * {@link #unrecoverableBackOff(BackOff, Map, Map, MessageListenerContainer)}.
	 */
	@Deprecated(since = "3.1", forRemoval = true) // 3.2
	public static void unrecoverableBackOff(BackOff backOff, ThreadLocal<BackOffExecution> executions,
			ThreadLocal<Long> lastIntervals, MessageListenerContainer container) throws InterruptedException {

		BackOffExecution backOffExecution = executions.get();
		if (backOffExecution == null) {
			backOffExecution = backOff.start();
			executions.set(backOffExecution);
		}
		Long interval = backOffExecution.nextBackOff();
		if (interval == BackOffExecution.STOP) {
			interval = lastIntervals.get();
			if (interval == null) {
				interval = Long.valueOf(0);
			}
		}
		lastIntervals.set(interval);
		if (interval > 0) {
			stoppableSleep(container, interval);
		}
	}

	/**
	 * Sleep according to the {@link BackOff}; when the {@link BackOffExecution} returns
	 * {@link BackOffExecution#STOP} sleep for the previous backOff.
	 * @param backOff the {@link BackOff} to create a new {@link BackOffExecution}.
	 * @param executions a thread local containing the {@link BackOffExecution} for this
	 * thread.
	 * @param lastIntervals a thread local containing the previous {@link BackOff}
	 * interval for this thread.
	 * @param container the container or parent container.
	 * @throws InterruptedException if the thread is interrupted.
	 * @since 3.1
	 */
	public static void unrecoverableBackOff(BackOff backOff, Map<Thread, BackOffExecution> executions,
			Map<Thread, Long> lastIntervals, MessageListenerContainer container) throws InterruptedException {

		Thread currentThread = Thread.currentThread();
		BackOffExecution backOffExecution = executions.get(currentThread);
		if (backOffExecution == null) {
			backOffExecution = backOff.start();
			executions.put(currentThread, backOffExecution);
		}
		Long interval = backOffExecution.nextBackOff();
		if (interval == BackOffExecution.STOP) {
			interval = lastIntervals.get(currentThread);
			if (interval == null) {
				interval = 0L;
			}
		}
		lastIntervals.put(currentThread, interval);
		if (interval > 0) {
			stoppableSleep(container, interval);
		}
	}

	/**
	 * Sleep for the desired timeout, as long as the container continues to run.
	 * @param container the container.
	 * @param interval the timeout.
	 * @throws InterruptedException if the thread is interrupted.
	 * @since 2.7
	 */
	public static void stoppableSleep(MessageListenerContainer container, long interval) throws InterruptedException {
		conditionalSleep(container::isRunning, interval);
	}

	/**
	 * Sleep for the desired timeout, as long as shouldSleepCondition supplies true.
	 * @param shouldSleepCondition to.
	 * @param interval the timeout.
	 * @throws InterruptedException if the thread is interrupted.
	 * @since 3.0.9
	 */
	public static void conditionalSleep(Supplier<Boolean> shouldSleepCondition, long interval) throws InterruptedException {
		long timeout = System.currentTimeMillis() + interval;
		long sleepInterval = interval > SMALL_INTERVAL_THRESHOLD ? DEFAULT_SLEEP_INTERVAL : SMALL_SLEEP_INTERVAL;
		do {
			Thread.sleep(sleepInterval);
			if (!shouldSleepCondition.get()) {
				break;
			}
		}
		while (System.currentTimeMillis() < timeout);
	}

	/**
	 * Create a new {@link  OffsetAndMetadata} using the given container and offset.
	 * @param container a container.
	 * @param offset an offset.
	 * @return an offset and metadata.
	 * @since 2.8.6
	 */
	public static OffsetAndMetadata createOffsetAndMetadata(MessageListenerContainer container,
															long offset) {
		final OffsetAndMetadataProvider metadataProvider = container.getContainerProperties()
				.getOffsetAndMetadataProvider();
		if (metadataProvider != null) {
			return metadataProvider.provide(new DefaultListenerMetadata(container), offset);
		}
		return new OffsetAndMetadata(offset);
	}
}

