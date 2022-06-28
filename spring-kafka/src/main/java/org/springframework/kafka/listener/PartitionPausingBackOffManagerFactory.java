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

import org.springframework.core.task.TaskExecutor;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;
import org.springframework.util.Assert;

/**
 *
 * Creates a {@link KafkaConsumerBackoffManager} instance
 * with or without a {@link KafkaConsumerTimingAdjuster}.
 * IMPORTANT: Since 2.9 this class doesn't create a {@link ThreadPoolTaskExecutor}
 * by default. In order for the factory to create a {@link KafkaConsumerTimingAdjuster},
 * such thread executor must be provided.
 * @deprecated in favor of {@link ContainerPartitionPausingBackOffManager}.
 *
 * @author Tomaz Fernandes
 * @since 2.7
 */
@Deprecated
public class PartitionPausingBackOffManagerFactory extends AbstractKafkaBackOffManagerFactory {

	private boolean timingAdjustmentEnabled = true;

	private KafkaConsumerTimingAdjuster timingAdjustmentManager;

	private TaskExecutor taskExecutor;

	private Clock clock;

	/**
	 * Construct a factory instance that will create the {@link KafkaConsumerBackoffManager}
	 * instances with the provided {@link KafkaConsumerTimingAdjuster}.
	 *
	 * @param timingAdjustmentManager the {@link KafkaConsumerTimingAdjuster} to be used.
	 */
	public PartitionPausingBackOffManagerFactory(KafkaConsumerTimingAdjuster timingAdjustmentManager) {
		this.clock = getDefaultClock();
		doSetTimingAdjustmentManager(timingAdjustmentManager);
	}

	/**
	 * Construct a factory instance that will create the {@link KafkaConsumerBackoffManager}
	 * instances with the provided {@link TaskExecutor} in its {@link KafkaConsumerTimingAdjuster}.
	 *
	 * @param timingAdjustmentManagerTaskExecutor the {@link TaskExecutor} to be used.
	 */
	public PartitionPausingBackOffManagerFactory(TaskExecutor timingAdjustmentManagerTaskExecutor) {
		this.clock = getDefaultClock();
		doSetTaskExecutor(timingAdjustmentManagerTaskExecutor);
	}

	/**
	 * Construct a factory instance specifying whether or not timing adjustment is enabled
	 * for this factories {@link KafkaConsumerBackoffManager}.
	 *
	 * @param timingAdjustmentEnabled the {@link KafkaConsumerTimingAdjuster} to be used.
	 */
	public PartitionPausingBackOffManagerFactory(boolean timingAdjustmentEnabled) {
		this.clock = getDefaultClock();
		this.timingAdjustmentEnabled = timingAdjustmentEnabled;
	}

	/**
	 * Construct a factory instance using the provided {@link ListenerContainerRegistry}.
	 *
	 * @param listenerContainerRegistry the {@link ListenerContainerRegistry} to be used.
	 */
	public PartitionPausingBackOffManagerFactory(ListenerContainerRegistry listenerContainerRegistry) {
		super(listenerContainerRegistry);
		this.clock = getDefaultClock();
	}

	/**
	 * Construct a factory instance with default dependencies.
	 */
	public PartitionPausingBackOffManagerFactory() {
		this.clock = getDefaultClock();
	}

	/**
	 * Construct an factory instance that will create the {@link KafkaConsumerBackoffManager}
	 * with the provided {@link Clock}.
	 * @param clock the clock instance to be used.
	 */
	public PartitionPausingBackOffManagerFactory(Clock clock) {
		this.clock = clock;
	}

	/**
	 * Set this property to false if you don't want the resulting KafkaBackOffManager
	 * to adjust the precision of the topics' consumption timing.
	 *
	 * @param timingAdjustmentEnabled set to false to disable timing adjustment.
	 */
	public void setTimingAdjustmentEnabled(boolean timingAdjustmentEnabled) {
		this.timingAdjustmentEnabled = timingAdjustmentEnabled;
	}

	/**
	 * Set the {@link WakingKafkaConsumerTimingAdjuster} that will be used
	 * with the resulting {@link KafkaConsumerBackoffManager}.
	 *
	 * @param timingAdjustmentManager the adjustmentManager to be used.
	 */
	public void setTimingAdjustmentManager(KafkaConsumerTimingAdjuster timingAdjustmentManager) {
		doSetTimingAdjustmentManager(timingAdjustmentManager);
	}

	private void doSetTimingAdjustmentManager(KafkaConsumerTimingAdjuster timingAdjustmentManager) {
		Assert.isTrue(this.timingAdjustmentEnabled, () -> "TimingAdjustment is disabled for this factory.");
		this.timingAdjustmentManager = timingAdjustmentManager;
	}

	/**
	 * Set the {@link TaskExecutor} that will be used in the {@link KafkaConsumerTimingAdjuster}.
	 * @param taskExecutor the taskExecutor to be used.
	 */
	public void setTaskExecutor(TaskExecutor taskExecutor) {
		doSetTaskExecutor(taskExecutor);
	}

	private void doSetTaskExecutor(TaskExecutor taskExecutor) {
		Assert.isTrue(this.timingAdjustmentEnabled, () -> "TimingAdjustment is disabled for this factory.");
		this.taskExecutor = taskExecutor;
	}

	/**
	 * Set the {@link Clock} instance that will be used in the
	 * {@link KafkaConsumerBackoffManager}.
	 * @param clock the clock instance.
	 * @since 2.9
	 */
	public void setClock(Clock clock) {
		this.clock = clock;
	}

	@Override
	protected KafkaConsumerBackoffManager doCreateManager(ListenerContainerRegistry registry) {
		return getKafkaConsumerBackoffManager(registry);
	}

	protected final Clock getDefaultClock() {
		return Clock.systemUTC();
	}

	private PartitionPausingBackoffManager getKafkaConsumerBackoffManager(ListenerContainerRegistry registry) {
		return this.timingAdjustmentEnabled && this.taskExecutor != null
			? new PartitionPausingBackoffManager(registry, getOrCreateBackOffTimingAdjustmentManager(), this.clock)
			: new PartitionPausingBackoffManager(registry, this.clock);
	}

	private KafkaConsumerTimingAdjuster getOrCreateBackOffTimingAdjustmentManager() {
		if (this.timingAdjustmentManager != null) {
			return this.timingAdjustmentManager;
		}
		return new WakingKafkaConsumerTimingAdjuster(this.taskExecutor);
	}

}
