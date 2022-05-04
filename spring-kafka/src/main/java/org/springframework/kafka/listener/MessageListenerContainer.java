/*
 * Copyright 2016-2022 the original author or authors.
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

import java.util.Collection;
import java.util.Map;

import org.apache.kafka.common.Metric;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.TopicPartition;

import org.springframework.beans.factory.DisposableBean;
import org.springframework.context.SmartLifecycle;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.lang.Nullable;

/**
 * Internal abstraction used by the framework representing a message
 * listener container. Not meant to be implemented externally.
 *
 * @author Stephane Nicoll
 * @author Gary Russell
 * @author Vladimir Tsanev
 * @author Tomaz Fernandes
 * @author Francois Rosiere
 */
public interface MessageListenerContainer extends SmartLifecycle, DisposableBean {

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
	@Nullable
	default Collection<TopicPartition> getAssignedPartitions() {
		throw new UnsupportedOperationException("This container doesn't support retrieving its assigned partitions");
	}

	/**
	 * Return the assigned topics/partitions for this container, by client.id.
	 * @return the topics/partitions.
	 * @since 2.5
	 */
	@Nullable
	default Map<String, Collection<TopicPartition>> getAssignmentsByClientId() {
		throw new UnsupportedOperationException("This container doesn't support retrieving its assigned partitions");
	}

	/**
	 * Pause this container before the next poll(). This is a thread-safe operation, the
	 * actual pause is processed by the consumer thread.
	 * @since 2.1.3
	 * @see org.apache.kafka.clients.consumer.KafkaConsumer#pause(Collection)
	 */
	default void pause() {
		throw new UnsupportedOperationException("This container doesn't support pause");
	}

	/**
	 * Resume this container, if paused, after the next poll(). This is a thread-safe
	 * operation, the actual resume is processed by the consumer thread.
	 * @since 2.1.3
	 * @see org.apache.kafka.clients.consumer.KafkaConsumer#resume(Collection)
	 */
	default void resume() {
		throw new UnsupportedOperationException("This container doesn't support resume");
	}

	/**
	 * Pause this partition before the next poll(). This is a thread-safe operation, the
	 * actual pause is processed by the consumer thread.
	 * @param topicPartition the topicPartition to pause.
	 * @since 2.7
	 */
	default void pausePartition(TopicPartition topicPartition) {
		throw new UnsupportedOperationException("This container doesn't support pausing a partition");
	}

	/**
	 * Resume this partition, if paused, after the next poll(). This is a thread-safe operation, the
	 * actual pause is processed by the consumer thread.
	 * @param topicPartition the topicPartition to resume.
	 * @since 2.7
	 */
	default void resumePartition(TopicPartition topicPartition) {
		throw new UnsupportedOperationException("This container doesn't support resuming a partition");
	}

	/**
	 * Whether or not this topic's partition pause has been requested.
	 * @param topicPartition the topic partition to check
	 * @return true if pause for this TopicPartition has been requested
	 * @since 2.7
	 */
	default boolean isPartitionPauseRequested(TopicPartition topicPartition) {
		throw new UnsupportedOperationException("This container doesn't support pausing a partition");
	}

	/**
	 * Whether or not this topic's partition is currently paused.
	 * @param topicPartition the topic partition to check
	 * @return true if this partition has been paused.
	 * @since 2.7
	 */
	default boolean isPartitionPaused(TopicPartition topicPartition) {
		throw new UnsupportedOperationException("This container doesn't support checking if a partition is paused");
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

	/**
	 * Return the {@code group.id} property for this container whether specifically set on the
	 * container or via a consumer property on the consumer factory.
	 * @return the group id.
	 * @since 2.2.5
	 */
	@Nullable
	default String getGroupId() {
		throw new UnsupportedOperationException("This container does not support retrieving the group id");
	}

	/**
	 * The 'id' attribute of a {@code @KafkaListener} or the bean name for spring-managed
	 * containers.
	 * @return the id or bean name.
	 * @since 2.2.5
	 */
	@Nullable
	default String getListenerId() {
		throw new UnsupportedOperationException("This container does not support retrieving the listener id");
	}

	/**
	 * Get arbitrary static information that will be added to the
	 * {@link KafkaHeaders#LISTENER_INFO} header of all records.
	 * @return the info.
	 * @since 2.8.6
	 */
	@Nullable
	default byte[] getListenerInfo() {
		throw new UnsupportedOperationException("This container does not support retrieving the listener info");
	}

	/**
	 * If this container has child containers, return true if at least one child is running. If there are not
	 * child containers, returns {@link #isRunning()}.
	 * @return true if a child is running.
	 * @since 2.7.3
	 */
	default boolean isChildRunning() {
		return isRunning();
	}

	/**
	 * Return true if the container is running, has never been started, or has been
	 * stopped.
	 * @return true if the state is as expected.
	 * @since 2.8
	 * @see #stopAbnormally(Runnable)
	 */
	default boolean isInExpectedState() {
		return true;
	}

	/**
	 * Stop the container after some exception so that {@link #isInExpectedState()} will
	 * return false.
	 * @param callback the callback.
	 * @since 2.8
	 * @see #isInExpectedState()
 	 */
	default void stopAbnormally(Runnable callback) {
		stop(callback);
	}

	@Override
	default void destroy() {
		stop();
	}

}
