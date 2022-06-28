/*
 * Copyright 2022 the original author or authors.
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
import java.time.Instant;
import java.time.ZoneId;
import java.util.Optional;

import org.apache.commons.logging.LogFactory;
import org.apache.kafka.common.TopicPartition;

import org.springframework.core.log.LogAccessor;
import org.springframework.lang.Nullable;
import org.springframework.scheduling.TaskScheduler;
import org.springframework.util.Assert;

/**
 * Service for pausing and resuming of {@link MessageListenerContainer}.
 *
 * @author Jan Marincek
 * @author Gary Russell
 * @since 2.9
 */
public class ListenerContainerPauseService {

	private static final LogAccessor LOGGER = new LogAccessor(LogFactory.getLog(ListenerContainerPauseService.class));

	@Nullable
	private final ListenerContainerRegistry registry;

	private final TaskScheduler scheduler;

	/**
	 * Create an instance with the provided registry and scheduler.
	 * @param registry the registry or null.
	 * @param scheduler the scheduler.
	 */
	public ListenerContainerPauseService(@Nullable ListenerContainerRegistry registry, TaskScheduler scheduler) {
		Assert.notNull(scheduler, "'scheduler' cannot be null");
		this.registry = registry;
		this.scheduler = scheduler;
	}

	/**
	 * Pause the listener by given id.
	 * Checks if the listener has already been requested to pause.
	 * Sets executor schedule for resuming the same listener after pauseDuration.
	 * @param listenerId    the id of the listener
	 * @param pauseDuration duration between pause() and resume() actions
	 */
	public void pause(String listenerId, Duration pauseDuration) {
		Assert.notNull(this.registry, "Pause by id is only supported when a registry is provided");
		getListenerContainer(listenerId)
				.ifPresent(messageListenerContainer -> pause(messageListenerContainer, pauseDuration));
	}

	/**
	 * Pause the listener by given container instance. Checks if the listener has already
	 * been requested to pause. Sets executor schedule for resuming the same listener
	 * after pauseDuration.
	 * @param messageListenerContainer the listener container
	 * @param pauseDuration duration between pause() and resume() actions
	 */
	public void pause(MessageListenerContainer messageListenerContainer, Duration pauseDuration) {
		if (messageListenerContainer.isPauseRequested()) {
			LOGGER.debug(() -> "Container " + messageListenerContainer + " already has pause requested");
		}
		else {
			Instant resumeAt = Instant.now().plusMillis(pauseDuration.toMillis());
			LOGGER.debug(() -> "Pausing container " + messageListenerContainer + ", resume scheduled for "
					+ resumeAt.atZone(ZoneId.systemDefault()).toLocalDateTime());
			messageListenerContainer.pause();
			this.scheduler.schedule(() -> {
				LOGGER.debug(() -> "Pausing container " + messageListenerContainer);
				resume(messageListenerContainer);
			}, resumeAt);
		}
	}

	/**
	 * Pause consumption from a given partition for the duration.
	 * @param messageListenerContainer the container.
	 * @param partition the partition.
	 * @param pauseDuration the duration.
	 */
	public void pausePartition(MessageListenerContainer messageListenerContainer, TopicPartition partition,
			Duration pauseDuration) {

		Instant resumeAt = Instant.now().plusMillis(pauseDuration.toMillis());
		LOGGER.debug(() -> "Pausing container: " + messageListenerContainer + " partition: " + partition
				+ ", resume scheduled for "
				+ resumeAt.atZone(ZoneId.systemDefault()).toLocalDateTime());
		messageListenerContainer.pausePartition(partition);
		this.scheduler.schedule(() -> {
			LOGGER.debug(() -> "Resuming container: " + messageListenerContainer + " partition: " + partition);
			messageListenerContainer.resumePartition(partition);
		}, resumeAt);

	}

	/**
	 * Resume the listener container by given id.
	 * @param listenerId the id of the listener
	 */
	public void resume(String listenerId) {
		Assert.notNull(this.registry, "Resume by id is only supported when a registry is provided");
		getListenerContainer(listenerId).ifPresent(this::resume);
	}

	/**
	 * Resume the listener container.
	 * @param messageListenerContainer the listener container
	 */
	public void resume(MessageListenerContainer messageListenerContainer) {
		if (messageListenerContainer.isPauseRequested()) {
			LOGGER.debug(() -> "Resuming container " + messageListenerContainer);
			messageListenerContainer.resume();
		}
		else {
			LOGGER.debug(() -> "Container " + messageListenerContainer + " was not paused");
		}
	}

	private Optional<MessageListenerContainer> getListenerContainer(String listenerId) {
		MessageListenerContainer messageListenerContainer = this.registry.getListenerContainer(listenerId);
		if (messageListenerContainer == null) {
			LOGGER.warn(() -> "MessageListenerContainer " + listenerId + " does not exists");
		}

		return Optional.ofNullable(messageListenerContainer);
	}

}
