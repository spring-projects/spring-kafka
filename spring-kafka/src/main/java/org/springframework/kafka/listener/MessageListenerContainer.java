/*
 * Copyright 2016-2018 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.springframework.kafka.listener;

import java.util.Collection;
import java.util.Map;

import org.apache.kafka.common.Metric;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.TopicPartition;

import org.springframework.context.SmartLifecycle;

/**
 * Internal abstraction used by the framework representing a message
 * listener container. Not meant to be implemented externally.
 *
 * @author Stephane Nicoll
 * @author Gary Russell
 * @author Vladimir Tsanev
 */
public interface MessageListenerContainer extends SmartLifecycle {

	/**
	 * Setup the message listener to use. Throws an {@link IllegalArgumentException}
	 * if that message listener type is not supported.
	 * @param messageListener the {@code object} to wrapped to the {@code MessageListener}.
	 */
	void setupMessageListener(Object messageListener);

	/**
	 * Return metrics kept by this container's consumer(s), grouped by {@code client-id}.
	 * @return the consumer(s) metrics grouped by {@code client-id}
	 * @since 1.3
	 * @see org.apache.kafka.clients.consumer.Consumer#metrics()
	 */
	Map<String, Map<MetricName, ? extends Metric>> metrics();

	/**
	 * Return the container properties for this container.
	 * @return the properties.
	 * @since 2.1.3
	 */
	default ContainerProperties getContainerProperties() {
		throw new UnsupportedOperationException("This container doesn't support retrieving its properties");
	}

	/**
	 * Return the assigned topics/partitions for this container.
	 * @return the topics/partitions.
	 * @since 2.1.3
	 */
	default Collection<TopicPartition> getAssignedPartitions() {
		throw new UnsupportedOperationException("This container doesn't support retrieving its assigned partitions");
	}

	/**
	 * Pause this container before the next poll().
	 * @since 2.1.3
	 */
	default void pause() {
		throw new UnsupportedOperationException("This container doesn't support pause");
	}

	/**
	 * Resume this container, if paused, after the next poll().
	 * @since 2.1.3
	 */
	default void resume() {
		throw new UnsupportedOperationException("This container doesn't support resume");
	}

	/**
	 * Return true if {@link #pause()} has been called; the container might not have actually
	 * paused yet.
	 * @return true if pause has been requested.
	 * @since 2.1.5
	 */
	default boolean isPauseRequested() {
		throw new UnsupportedOperationException("This container doesn't support pause/resume");
	}

	/**
	 * Return true if {@link #pause()} has been called; and all consumers in this container
	 * have actually paused.
	 * @return true if the container is paused.
	 * @since 2.1.5
	 */
	default boolean isContainerPaused() {
		throw new UnsupportedOperationException("This container doesn't support pause/resume");
	}

	/**
	 * Set the autoStartup.
	 * @param autoStartup the autoStartup to set.
	 * @since 2.2
	 * @see SmartLifecycle
	 */
	default void setAutoStartup(boolean autoStartup) {
		// empty
	}

}
