/*
 * Copyright 2026-present the original author or authors.
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
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.common.Metric;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.TopicPartition;
import org.jspecify.annotations.Nullable;

import org.springframework.kafka.event.ConsumerStoppedEvent;

/**
 * A parent container view associated with one generation of child containers. Mutating
 * operations from an old generation are ignored after the parent has restarted.
 *
 * @param <K> the key type.
 * @param <V> the value type.
 * @author Sudhanshu Ratna Thakur
 * @since 4.2
 */
final class ChildAwareMessageListenerContainer<K, V> implements MessageListenerContainer {

	private final ConcurrentMessageListenerContainer<K, V> parent;

	private final long generation;

	ChildAwareMessageListenerContainer(ConcurrentMessageListenerContainer<K, V> parent, long generation) {

		this.parent = parent;
		this.generation = generation;
	}

	@Override
	public void setupMessageListener(Object messageListener) {
		this.parent.invokeIfGenerationCurrent(this.generation, () -> this.parent.setupMessageListener(messageListener));
	}

	@Override
	public Map<String, Map<MetricName, ? extends Metric>> metrics() {
		return this.parent.metrics();
	}

	@Override
	public ContainerProperties getContainerProperties() {
		return this.parent.getContainerProperties();
	}

	@Override
	@Nullable
	public Collection<TopicPartition> getAssignedPartitions() {
		return this.parent.getAssignedPartitions();
	}

	@Override
	@Nullable
	public Map<String, Collection<TopicPartition>> getAssignmentsByClientId() {
		return this.parent.getAssignmentsByClientId();
	}

	@Override
	public void enforceRebalance() {
		this.parent.invokeIfGenerationCurrent(this.generation, this.parent::enforceRebalance);
	}

	@Override
	public void pause() {
		this.parent.invokeIfGenerationCurrent(this.generation, this.parent::pause);
	}

	@Override
	public void resume() {
		this.parent.invokeIfGenerationCurrent(this.generation, this.parent::resume);
	}

	@Override
	public void pausePartition(TopicPartition topicPartition) {
		this.parent.invokeIfGenerationCurrent(this.generation, () -> this.parent.pausePartition(topicPartition));
	}

	@Override
	public void resumePartition(TopicPartition topicPartition) {
		this.parent.invokeIfGenerationCurrent(this.generation, () -> this.parent.resumePartition(topicPartition));
	}

	@Override
	public boolean isPartitionPauseRequested(TopicPartition topicPartition) {
		return this.parent.isPartitionPauseRequested(topicPartition);
	}

	@Override
	public boolean isPartitionPaused(TopicPartition topicPartition) {
		return this.parent.isPartitionPaused(topicPartition);
	}

	@Override
	public boolean isPauseRequested() {
		return this.parent.isPauseRequested();
	}

	@Override
	public boolean isContainerPaused() {
		return this.parent.isContainerPaused();
	}

	@Override
	public boolean isAutoStartup() {
		return this.parent.isAutoStartup();
	}

	@Override
	public void setAutoStartup(boolean autoStartup) {
		this.parent.invokeIfGenerationCurrent(this.generation, () -> this.parent.setAutoStartup(autoStartup));
	}

	@Override
	@Nullable
	public String getGroupId() {
		return this.parent.getGroupId();
	}

	@Override
	public String getListenerId() {
		return this.parent.getListenerId();
	}

	@Override
	@Nullable
	public String getMainListenerId() {
		return this.parent.getMainListenerId();
	}

	@Override
	@Nullable
	public byte[] getListenerInfo() {
		return this.parent.getListenerInfo();
	}

	@Override
	public boolean isChildRunning() {
		return this.parent.isChildRunning();
	}

	@Override
	public boolean isInExpectedState() {
		return this.parent.isInExpectedState();
	}

	@Override
	public void stopAbnormally(Runnable callback) {
		this.parent.stopIfGenerationCurrent(this.generation, callback, false);
	}

	@Override
	public MessageListenerContainer getContainerFor(String topic, int partition) {
		return this.parent.getContainerFor(topic, partition);
	}

	@Override
	public void childStopped(MessageListenerContainer childContainer, ConsumerStoppedEvent.Reason reason) {
		this.parent.childStopped(childContainer, reason);
	}

	@Override
	public void childStarted(MessageListenerContainer childContainer) {
		this.parent.childStarted(childContainer);
	}

	@Override
	public void start() {
		this.parent.invokeIfGenerationCurrent(this.generation, this.parent::start);
	}

	@Override
	public void stop() {
		CountDownLatch latch = new CountDownLatch(1);
		if (this.parent.stopIfGenerationCurrent(this.generation, latch::countDown, true)) {
			try {
				latch.await(getContainerProperties().getShutdownTimeout(), TimeUnit.MILLISECONDS); // NOSONAR
			}
			catch (@SuppressWarnings("unused") InterruptedException ex) {
				Thread.currentThread().interrupt();
			}
		}
	}

	@Override
	public void stop(Runnable callback) {
		this.parent.stopIfGenerationCurrent(this.generation, callback, true);
	}

	@Override
	public boolean isRunning() {
		return this.parent.isRunning();
	}

	@Override
	public int getPhase() {
		return this.parent.getPhase();
	}

	@Override
	public String toString() {
		return this.parent.toString();
	}

}
