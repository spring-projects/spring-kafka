/*
 * Copyright 2016-2019 the original author or authors.
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

import org.springframework.kafka.KafkaException;
import org.springframework.lang.Nullable;

/**
 * The listener specific {@link KafkaException} extension.
 *
 * @author Gary Russell
 */
@SuppressWarnings("serial")
public class ListenerExecutionFailedException extends KafkaException {

	private final String groupId;

	/**
	 * Construct an instance with the provided properties.
	 * @param message the exception message.
	 */
	public ListenerExecutionFailedException(String message) {
		this(message, null, null);
	}

	/**
	 * Construct an instance with the provided properties.
	 * @param message the exception message.
	 * @param cause the cause.
	 */
	public ListenerExecutionFailedException(String message, @Nullable Throwable cause) {
		this(message, null, cause);
	}

	/**
	 * Construct an instance with the provided properties.
	 * @param message the exception message.
	 * @param groupId the container's group.id property.
	 * @param cause the cause.
	 * @since 2.2.4
	 */
	public ListenerExecutionFailedException(String message, @Nullable String groupId, @Nullable Throwable cause) {
		super(message, cause);
		this.groupId = groupId;
	}

	/**
	 * Return the consumer group.id property of the container that threw this exception.
	 * @return the group id; may be null, but not when the exception is passed to an error
	 * handler by a listener container.
	 * @since 2.2.4
	 */
	@Nullable
	public String getGroupId() {
		return this.groupId;
	}

}
