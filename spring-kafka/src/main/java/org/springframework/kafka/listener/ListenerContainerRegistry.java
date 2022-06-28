/*
 * Copyright 2021-2022 the original author or authors.
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
import java.util.Set;

import org.springframework.lang.Nullable;

/**
 * A registry for listener containers.
 *
 * @author Gary Russell
 * @since 2.7
 *
 */
public interface ListenerContainerRegistry {

	/**
	 * Return the {@link MessageListenerContainer} with the specified id or
	 * {@code null} if no such container exists.
	 * @param id the id of the container
	 * @return the container or {@code null} if no container with that id exists
	 * @see org.springframework.kafka.config.KafkaListenerEndpoint#getId()
	 * @see #getListenerContainerIds()
	 */
	@Nullable
	MessageListenerContainer getListenerContainer(String id);

	/**
	 * Return the {@link MessageListenerContainer} with the specified id or {@code null}
	 * if no such container exists. Returns containers that are not registered with the
	 * registry, but exist in the application context.
	 * @param id the id of the container
	 * @return the container or {@code null} if no container with that id exists
	 * @see #getListenerContainerIds()
	 */
	@Nullable
	MessageListenerContainer getUnregisteredListenerContainer(String id);

	/**
	 * Return the ids of the managed {@link MessageListenerContainer} instance(s).
	 * @return the ids.
	 * @see #getListenerContainer(String)
	 */
	Set<String> getListenerContainerIds();

	/**
	 * Return the managed {@link MessageListenerContainer} instance(s).
	 * @return the managed {@link MessageListenerContainer} instance(s).
	 * @see #getAllListenerContainers()
	 */
	Collection<MessageListenerContainer> getListenerContainers();

	/**
	 * Return all {@link MessageListenerContainer} instances including those managed by
	 * this registry and those declared as beans in the application context.
	 * Prototype-scoped containers will be included. Lazy beans that have not yet been
	 * created will not be initialized by a call to this method.
	 * @return the {@link MessageListenerContainer} instance(s).
	 * @see #getListenerContainers()
	 */
	Collection<MessageListenerContainer> getAllListenerContainers();

}
