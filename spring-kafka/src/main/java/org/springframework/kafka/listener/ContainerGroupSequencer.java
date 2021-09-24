/*
 * Copyright 2021 the original author or authors.
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
import java.util.Iterator;
import java.util.LinkedHashSet;

import org.apache.commons.logging.LogFactory;

import org.springframework.beans.BeansException;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.context.ApplicationListener;
import org.springframework.context.SmartLifecycle;
import org.springframework.core.log.LogAccessor;
import org.springframework.core.task.SimpleAsyncTaskExecutor;
import org.springframework.core.task.TaskExecutor;
import org.springframework.kafka.event.ListenerContainerIdleEvent;

/**
 * Sequence the starting of container groups when all containers in the previous group are
 * idle.
 *
 * @author Gary Russell
 * @since 2.7.3
 *
 */
public class ContainerGroupSequencer implements ApplicationContextAware,
		ApplicationListener<ListenerContainerIdleEvent>, SmartLifecycle {

	private static final LogAccessor LOGGER = new LogAccessor(LogFactory.getLog(ContainerGroupSequencer.class));

	private final ListenerContainerRegistry registry;

	private final long defaultIdleEventInterval;

	private final Collection<String> groupNames = new LinkedHashSet<>();

	private final Collection<ContainerGroup> groups = new LinkedHashSet<>();

	private final TaskExecutor executor = new SimpleAsyncTaskExecutor("container-group-sequencer-");

	private ApplicationContext applicationContext;

	private boolean stopLastGroupWhenIdle;

	private Iterator<ContainerGroup> iterator;

	private ContainerGroup currentGroup;

	private boolean autoStartup = true;

	private int phase = AbstractMessageListenerContainer.DEFAULT_PHASE;

	private boolean running;

	/**
	 * Set containers in each group to not auto start. Start the containers in the first
	 * group. Start containers in group[n] when all containers in group[n-1] are idle;
	 * stop the containers in group[n-1].
	 * @param registry the registry.
	 * @param defaultIdleEventInterval the idle event interval if not already set.
	 * @param containerGroups The list of container groups, in order.
	 */
	public ContainerGroupSequencer(ListenerContainerRegistry registry, long defaultIdleEventInterval,
			String... containerGroups) {

		this.registry = registry;
		this.defaultIdleEventInterval = defaultIdleEventInterval;
		for (String groupName : containerGroups) {
			this.groupNames.add(groupName);
		}
	}

	@Override
	public void setApplicationContext(ApplicationContext applicationContext) throws BeansException {
		this.applicationContext = applicationContext;
	}

	/**
	 * Set to true to stop the containers in the final group when they go idle. By
	 * default, the containers in the final group remain running.
	 * @param stopLastGroupWhenIdle true to stop containers in the final group.
	 */
	public synchronized void setStopLastGroupWhenIdle(boolean stopLastGroupWhenIdle) {
		this.stopLastGroupWhenIdle = stopLastGroupWhenIdle;
	}

	@Override
	public boolean isAutoStartup() {
		return this.autoStartup;
	}

	/**
	 * Set to false to not automatically start.
	 * @param autoStartup false to not start;
	 * @since 2.7.6
	 */
	public void setAutoStartup(boolean autoStartup) {
		this.autoStartup = autoStartup;
	}

	@Override
	public int getPhase() {
		return this.phase;
	}

	/**
	 * Set the {@link SmartLifecycle#getPhase()}.
	 * @param phase the phase.
	 * @since 2.7.6
	 */
	public void setPhase(int phase) {
		this.phase = phase;
	}

	@Override
	public synchronized void onApplicationEvent(ListenerContainerIdleEvent event) {
		LOGGER.debug(() -> event.toString());
		MessageListenerContainer parent = event.getContainer(MessageListenerContainer.class);
		MessageListenerContainer container = (MessageListenerContainer) event.getSource();
		boolean inCurrentGroup = this.currentGroup != null && this.currentGroup.contains(parent);
		if (this.running && inCurrentGroup && (this.iterator.hasNext() || this.stopLastGroupWhenIdle)) {
			this.executor.execute(() -> {
				LOGGER.debug(() -> "Stopping: " + container);
				container.stop(() -> {
					synchronized (this) {
						if (!parent.isChildRunning()) {
							this.executor.execute(() -> {
								stopParentAndCheckGroup(parent);
							});
						}
					}
				});
			});
		}
	}

	private synchronized void stopParentAndCheckGroup(MessageListenerContainer parent) {
		if (parent.isRunning()) {
			LOGGER.debug(() -> "Stopping: " + parent);
			parent.stop(() -> {
				if (this.currentGroup != null) {
					LOGGER.debug(() -> "Checking group: " + this.currentGroup.toString());
					if (this.currentGroup.allStopped()) {
						if (this.iterator.hasNext()) {
							this.currentGroup = this.iterator.next();
							LOGGER.debug(() -> "Starting next group: " + this.currentGroup);
							this.currentGroup.start();
						}
						else {
							this.currentGroup = null;
						}
					}
				}
			});
		}
	}

	@Override
	public synchronized void start() {
		if (this.currentGroup != null) {
			LOGGER.debug(() -> "Starting first group: " + this.currentGroup);
			this.currentGroup.start();
		}
		this.running = true;
	}

	public void initialize() {
		this.groups.clear();
		for (String group : this.groupNames) {
			this.groups.add(this.applicationContext.getBean(group + ".group", ContainerGroup.class));
		}
		if (this.groups.size() > 0) {
			this.iterator = this.groups.iterator();
			this.currentGroup = this.iterator.next();
			this.groups.forEach(grp -> {
				Collection<String> ids = grp.getListenerIds();
				ids.stream().forEach(id -> {
					MessageListenerContainer container = this.registry.getListenerContainer(id);
					if (container.getContainerProperties().getIdleEventInterval() == null) {
						container.getContainerProperties().setIdleEventInterval(this.defaultIdleEventInterval);
						container.setAutoStartup(false);
					}
				});
			});
		}
		LOGGER.debug(() -> "Found: " + this.groups);
	}

	@Override
	public synchronized void stop() {
		this.running = false;
		if (this.currentGroup != null) {
			this.currentGroup.stop();
			this.currentGroup = null;
		}
	}

	@Override
	public synchronized boolean isRunning() {
		return this.running;
	}

}
