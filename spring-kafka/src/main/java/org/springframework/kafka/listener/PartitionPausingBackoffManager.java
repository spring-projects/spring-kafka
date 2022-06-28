/*
 * Copyright 2018-2022 the original author or authors.
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

import java.time.Clock;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.logging.LogFactory;
import org.apache.kafka.common.TopicPartition;

import org.springframework.context.ApplicationListener;
import org.springframework.core.log.LogAccessor;
import org.springframework.kafka.event.ListenerContainerPartitionIdleEvent;
import org.springframework.lang.Nullable;

/**
 *
 * A manager that backs off consumption for a given topic if the timestamp provided is not
 * due. Use with {@link DefaultErrorHandler} to guarantee that the message is read
 * again after partition consumption is resumed (or seek it manually by other means).
 * It's also necessary to set a {@link ContainerProperties#setIdlePartitionEventInterval(Long)}
 * so the Manager can resume the partition consumption.
 *
 * Note that when a record backs off the partition consumption gets paused for
 * approximately that amount of time, so you must have a fixed backoff value per partition.
 * @deprecated in favor of {@link ContainerPartitionPausingBackOffManager}.
 *
 * @author Tomaz Fernandes
 * @author Gary Russell
 * @since 2.7
 * @see DefaultErrorHandler
 */
@Deprecated
public class PartitionPausingBackoffManager implements KafkaConsumerBackoffManager,
		ApplicationListener<ListenerContainerPartitionIdleEvent> {

	private static final LogAccessor LOGGER = new LogAccessor(LogFactory.getLog(KafkaConsumerBackoffManager.class));

	private final ListenerContainerRegistry listenerContainerRegistry;

	private final Map<TopicPartition, Context> backOffContexts;

	private final Clock clock;

	private final KafkaConsumerTimingAdjuster kafkaConsumerTimingAdjuster;

	/**
	 * Constructs an instance with the provided {@link ListenerContainerRegistry} and
	 * {@link KafkaConsumerTimingAdjuster}.
	 *
	 * The ListenerContainerRegistry is used to fetch the {@link MessageListenerContainer}
	 * that will be backed off / resumed.
	 *
	 * The KafkaConsumerTimingAdjuster is used to make timing adjustments
	 * in the message consumption so that it processes the message closer
	 * to its due time rather than later.
	 *
	 * @param listenerContainerRegistry the listenerContainerRegistry to use.
	 * @param kafkaConsumerTimingAdjuster the kafkaConsumerTimingAdjuster to use.
	 */
	public PartitionPausingBackoffManager(ListenerContainerRegistry listenerContainerRegistry,
										KafkaConsumerTimingAdjuster kafkaConsumerTimingAdjuster) {

		this(listenerContainerRegistry, kafkaConsumerTimingAdjuster, Clock.systemUTC());
	}

	/**
	 * Constructs an instance with the provided {@link ListenerContainerRegistry}
	 * and with no timing adjustment capabilities.
	 *
	 * The ListenerContainerRegistry is used to fetch the {@link MessageListenerContainer}
	 * that will be backed off / resumed.
	 *
	 * @param listenerContainerRegistry the listenerContainerRegistry to use.
	 */
	public PartitionPausingBackoffManager(ListenerContainerRegistry listenerContainerRegistry) {
		this(listenerContainerRegistry, null, Clock.systemUTC());
	}

	/**
	 * Creates an instance with the provided {@link ListenerContainerRegistry},
	 * {@link KafkaConsumerTimingAdjuster} and {@link Clock}.
	 *
	 * @param listenerContainerRegistry the listenerContainerRegistry to use.
	 * @param kafkaConsumerTimingAdjuster the kafkaConsumerTimingAdjuster to use.
	 * @param clock the clock to use.
	 */
	public PartitionPausingBackoffManager(ListenerContainerRegistry listenerContainerRegistry,
										@Nullable KafkaConsumerTimingAdjuster kafkaConsumerTimingAdjuster,
										Clock clock) {

		this.listenerContainerRegistry = listenerContainerRegistry;
		this.clock = clock;
		this.kafkaConsumerTimingAdjuster = kafkaConsumerTimingAdjuster;
		this.backOffContexts = new HashMap<>();
	}

	/**
	 * Creates an instance with the provided {@link ListenerContainerRegistry}
	 * and {@link Clock}, with no timing adjustment capabilities.
	 *
	 * @param listenerContainerRegistry the listenerContainerRegistry to use.
	 * @param clock the clock to use.
	 */
	public PartitionPausingBackoffManager(ListenerContainerRegistry listenerContainerRegistry, Clock clock) {
		this(listenerContainerRegistry, null, clock);
	}

	/**
	 * Backs off if the current time is before the dueTimestamp provided
	 * in the {@link Context} object.
	 * @param context the back off context for this execution.
	 */
	@Override
	public void backOffIfNecessary(Context context) {
		long backoffTime = context.getDueTimestamp() - getCurrentMillisFromClock();
		LOGGER.debug(() -> "Back off time: " + backoffTime + " Context: " + context);
		if (backoffTime > 0) {
			pauseConsumptionAndThrow(context, backoffTime);
		}
	}

	private void pauseConsumptionAndThrow(Context context, Long backOffTime) throws KafkaBackoffException {
		TopicPartition topicPartition = context.getTopicPartition();
		getListenerContainerFromContext(context).pausePartition(topicPartition);
		addBackoff(context, topicPartition);
		throw new KafkaBackoffException(String.format("Partition %s from topic %s is not ready for consumption, " +
				"backing off for approx. %s millis.", context.getTopicPartition().partition(),
				context.getTopicPartition().topic(), backOffTime),
				topicPartition, context.getListenerId(), context.getDueTimestamp());
	}

	@Override
	public void onApplicationEvent(ListenerContainerPartitionIdleEvent partitionIdleEvent) {
		LOGGER.debug(() -> String.format("partitionIdleEvent received at %s. Partition: %s",
				getCurrentMillisFromClock(), partitionIdleEvent.getTopicPartition()));

		Context backOffContext = getBackOffContext(partitionIdleEvent.getTopicPartition());
		maybeResumeConsumption(backOffContext);
	}

	private long getCurrentMillisFromClock() {
		return Instant.now(this.clock).toEpochMilli();
	}

	private void maybeResumeConsumption(@Nullable Context context) {
		if (context == null) {
			return;
		}
		long now = getCurrentMillisFromClock();
		long timeUntilDue = context.getDueTimestamp() - now;
		long pollTimeout = getListenerContainerFromContext(context)
				.getContainerProperties()
				.getPollTimeout();
		boolean isDue = timeUntilDue <= pollTimeout;

		long adjustedAmount = applyTimingAdjustment(context, timeUntilDue, pollTimeout);

		if (adjustedAmount != 0L || isDue) {
			resumePartition(context);
		}
		else {
			LOGGER.debug(() -> String.format("TopicPartition %s not due. DueTimestamp: %s Now: %s ",
					context.getTopicPartition(), context.getDueTimestamp(), now));
		}
	}

	private long applyTimingAdjustment(Context context, long timeUntilDue, long pollTimeout) {
		if (this.kafkaConsumerTimingAdjuster == null || context.getConsumerForTimingAdjustment() == null) {
			LOGGER.debug(() -> String.format(
					"Skipping timing adjustment for TopicPartition %s.", context.getTopicPartition()));
			return 0L;
		}
		return this.kafkaConsumerTimingAdjuster.adjustTiming(
						context.getConsumerForTimingAdjustment(),
						context.getTopicPartition(), pollTimeout, timeUntilDue);
	}

	private void resumePartition(Context context) {
		MessageListenerContainer container = getListenerContainerFromContext(context);
		LOGGER.debug(() -> "Resuming partition at " + getCurrentMillisFromClock());
		container.resumePartition(context.getTopicPartition());
		removeBackoff(context.getTopicPartition());
	}

	private MessageListenerContainer getListenerContainerFromContext(Context context) {
		return this.listenerContainerRegistry.getListenerContainer(context.getListenerId()); // NOSONAR
	}

	protected void addBackoff(Context context, TopicPartition topicPartition) {
		synchronized (this.backOffContexts) {
			this.backOffContexts.put(topicPartition, context);
		}
	}

	protected @Nullable Context getBackOffContext(TopicPartition topicPartition) {
		synchronized (this.backOffContexts) {
			return this.backOffContexts.get(topicPartition);
		}
	}

	protected void removeBackoff(TopicPartition topicPartition) {
		synchronized (this.backOffContexts) {
			this.backOffContexts.remove(topicPartition);
		}
	}
}
