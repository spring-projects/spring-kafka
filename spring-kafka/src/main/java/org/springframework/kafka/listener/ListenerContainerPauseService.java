/*
 * Copyright 2014-2022 the original author or authors.
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
import java.time.LocalDateTime;
import java.util.Optional;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.apache.commons.logging.LogFactory;

import org.springframework.core.log.LogAccessor;
import org.springframework.lang.NonNull;
import org.springframework.lang.Nullable;

/**
 * Service for pausing and resuming of {@link MessageListenerContainer}.
 *
 * @author Jan Marincek
 * @since 2.9
 */
public class ListenerContainerPauseService {

	private static final LogAccessor LOGGER = new LogAccessor(LogFactory.getLog(ListenerContainerPauseService.class));
	private final ListenerContainerRegistry registry;
	private final ScheduledExecutorService executor = Executors.newSingleThreadScheduledExecutor();

	public ListenerContainerPauseService(ListenerContainerRegistry registry) {
		this.registry = registry;
	}

	/**
	 * Pause the listener by given id.
	 * Checks if the listener has already been requested to pause.
	 *
	 * @param listenerId the id of the listener
	 */
	public void pause(String listenerId) {
		getListenerContainer(listenerId).ifPresent(this::pause);
	}

	/**
	 * Pause the listener by given id.
	 * Checks if the listener has already been requested to pause.
	 * Sets executor schedule for resuming the same listener after pauseDuration.
	 *
	 * @param listenerId    the id of the listener
	 * @param pauseDuration duration between pause() and resume() actions
	 */
	public void pause(String listenerId, Duration pauseDuration) {
		getListenerContainer(listenerId)
				.ifPresent(messageListenerContainer -> pause(messageListenerContainer, pauseDuration));
	}

	/**
	 * Pause the listener by given container instance.
	 * Checks if the listener has already been requested to pause.
	 *
	 * @param messageListenerContainer the listener container
	 */
	public void pause(@NonNull MessageListenerContainer messageListenerContainer) {
		pause(messageListenerContainer, null);
	}

	/**
	 * Pause the listener by given container instance.
	 * Checks if the listener has already been requested to pause.
	 * Sets executor schedule for resuming the same listener after pauseDuration.
	 *
	 * @param messageListenerContainer the listener container
	 * @param pauseDuration            duration between pause() and resume() actions
	 */
	public void pause(@NonNull MessageListenerContainer messageListenerContainer, @Nullable Duration pauseDuration) {
		if (messageListenerContainer.isPauseRequested()) {
			LOGGER.debug(() -> "Container " + messageListenerContainer + " already has pause requested");
		}
		else {
			LOGGER.debug(() -> "Pausing container " + messageListenerContainer);
			messageListenerContainer.pause();
			if (messageListenerContainer.getListenerId() != null && pauseDuration != null) {
				LOGGER.debug(() -> "Resuming of container " + messageListenerContainer + " scheduled for " + LocalDateTime.now().plus(pauseDuration));
				this.executor.schedule(() -> resume(messageListenerContainer.getListenerId()), pauseDuration.toMillis(), TimeUnit.MILLISECONDS);
			}
		}
	}

	/**
	 * Resume the listener by given id.
	 *
	 * @param listenerId the id of the listener
	 */
	public void resume(@NonNull String listenerId) {
		getListenerContainer(listenerId).ifPresent(this::resume);
	}

	/**
	 * Resume the listener.
	 *
	 * @param messageListenerContainer the listener container
	 */
	public void resume(@NonNull MessageListenerContainer messageListenerContainer) {
		if (messageListenerContainer.isContainerPaused()) {
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
