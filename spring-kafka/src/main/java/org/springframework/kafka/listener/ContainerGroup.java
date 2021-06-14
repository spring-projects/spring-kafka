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
import java.util.LinkedHashSet;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.commons.logging.LogFactory;

import org.springframework.context.Lifecycle;
import org.springframework.core.log.LogAccessor;
import org.springframework.util.Assert;

/**
 * A group of listener containers.
 *
 * @author Gary Russell
 * @since 2.7.3
 *
 */
public class ContainerGroup implements Lifecycle {

	private static final LogAccessor LOGGER = new LogAccessor(LogFactory.getLog(ContainerGroup.class));

	private final String name;

	private final Collection<MessageListenerContainer> containers = new LinkedHashSet<>();

	private boolean running;

	/**
	 * Construct an instance with the provided name.
	 * @param name the group name.
	 */
	public ContainerGroup(String name) {
		this.name = name;
	}

	/**
	 * Construct an instance with the provided name and containers.
	 * @param name the group name.
	 * @param containers the containers.
	 */
	public ContainerGroup(String name, List<MessageListenerContainer> containers) {
		this.name = name;
		this.containers.addAll(containers);
	}

	/**
	 * Construct an instance with the provided name and containers.
	 * @param name the group name.
	 * @param containers the containers.
	 */
	public ContainerGroup(String name, MessageListenerContainer...containers) {
		this.name = name;
		for (MessageListenerContainer container : containers) {
			this.containers.add(container);
		}
	}

	/**
	 * Return the group name.
	 * @return the name.
	 */
	public String getName() {
		return this.name;
	}

	/**
	 * Return the listener ids of the containers in this group.
	 * @return the listener ids.
	 */
	public Collection<String> getListenerIds() {
		return this.containers.stream()
				.map(container -> container.getListenerId())
				.map(id -> {
					Assert.state(id != null, "Containers must have listener ids to be used here");
					return id;
				})
				.collect(Collectors.toList());
	}

	/**
	 * Return true if the provided container is in this group.
	 * @param container the container.
	 * @return true if it is in this group.
	 */
	public boolean contains(MessageListenerContainer container) {
		return this.containers.contains(container);
	}

	/**
	 * Return true if all containers in this group are stopped.
	 * @return true if all are stopped.
	 */
	public boolean allStopped() {
		return this.containers.stream()
				.allMatch(container -> !container.isRunning());
	}

	/**
	 * Add one or more containers to the group.
	 * @param theContainers the container(s).
	 */
	public void addContainers(MessageListenerContainer... theContainers) {
		for (MessageListenerContainer container : theContainers) {
			this.containers.add(container);
		}
	}

	/**
	 * Remove a container from the group.
	 * @param container the container.
	 * @return true if the container was removed.
	 */
	public boolean removeContainer(MessageListenerContainer container) {
		return this.containers.remove(container);
	}

	@Override
	public synchronized void start() {
		if (!this.running) {
			this.containers.forEach(container -> {
				LOGGER.debug(() -> "Starting: " + container);
				container.start();
			});
			this.running = true;
		}
	}

	@Override
	public synchronized void stop() {
		if (this.running) {
			this.containers.forEach(container -> container.stop());
			this.running = false;
		}
	}

	@Override
	public synchronized boolean isRunning() {
		return this.running;
	}

	@Override
	public String toString() {
		return "ContainerGroup [name=" + this.name + ", containers=" + this.containers + "]";
	}

}
