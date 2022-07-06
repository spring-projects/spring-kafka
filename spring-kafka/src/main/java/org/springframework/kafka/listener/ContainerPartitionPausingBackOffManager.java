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

import org.apache.commons.logging.LogFactory;
import org.apache.kafka.common.TopicPartition;

import org.springframework.core.log.LogAccessor;
import org.springframework.util.Assert;

/**
 *
 * A manager that backs off consumption for a given topic if the timestamp provided is not
 * due. Use with {@link DefaultErrorHandler} to guarantee that the message is read
 * again after partition consumption is resumed (or seek it manually by other means).
 * Note that when a record backs off the partition consumption gets paused for
 * approximately that amount of time, so you must have a fixed backoff value per partition.
 *
 * @author Tomaz Fernandes
 * @author Gary Russell
 * @since 2.9
 * @see DefaultErrorHandler
 */
public class ContainerPartitionPausingBackOffManager implements KafkaConsumerBackoffManager {

	private static final LogAccessor LOGGER = new LogAccessor(LogFactory.getLog(KafkaConsumerBackoffManager.class));

	private final ListenerContainerRegistry listenerContainerRegistry;

	private final BackOffHandler backOffHandler;

	/**
	 * Construct an instance with the provided registry and back off handler.
	 * @param listenerContainerRegistry the registry.
	 * @param backOffHandler the handler.
	 */
	public ContainerPartitionPausingBackOffManager(ListenerContainerRegistry listenerContainerRegistry,
			BackOffHandler backOffHandler) {

		Assert.notNull(listenerContainerRegistry, "'listenerContainerRegistry' cannot be null");
		Assert.notNull(backOffHandler, "'backOffHandler' cannot be null");
		this.listenerContainerRegistry = listenerContainerRegistry;
		this.backOffHandler = backOffHandler;
	}

	/**
	 * Backs off if the current time is before the dueTimestamp provided
	 * in the {@link Context} object.
	 * @param context the back off context for this execution.
	 */
	@Override
	public void backOffIfNecessary(Context context) {
		long backoffTime = context.getDueTimestamp() - System.currentTimeMillis();
		LOGGER.debug(() -> "Back off time: " + backoffTime + " Context: " + context);
		if (backoffTime > 0) {
			pauseConsumptionAndThrow(context, backoffTime);
		}
	}

	private void pauseConsumptionAndThrow(Context context, Long backOffTime) throws KafkaBackoffException {
		TopicPartition topicPartition = context.getTopicPartition();
		MessageListenerContainer container = getListenerContainerFromContext(context);
		container.pausePartition(topicPartition);
		this.backOffHandler.onNextBackOff(container, topicPartition, backOffTime);
		throw new KafkaBackoffException(String.format("Partition %s from topic %s is not ready for consumption, " +
				"backing off for approx. %s millis.", topicPartition.partition(),
				topicPartition.topic(), backOffTime),
				topicPartition, context.getListenerId(), context.getDueTimestamp());
	}

	private MessageListenerContainer getListenerContainerFromContext(Context context) {
		MessageListenerContainer container = this.listenerContainerRegistry.getListenerContainer(context.getListenerId()); // NOSONAR
		if (container == null) {
			container = this.listenerContainerRegistry.getUnregisteredListenerContainer(context.getListenerId());
		}
		Assert.notNull(container, () -> "No container found with id: " + context.getListenerId());
		return container;
	}

}
