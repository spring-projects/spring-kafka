/*
 * Copyright 2025-present the original author or authors.
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

package org.springframework.kafka.event;

import java.io.Serial;

import org.apache.kafka.clients.consumer.ShareConsumer;

/**
 * An event published when a share consumer is stopping. While it is best practice to use
 * stateless listeners, you can consume this event to clean up any thread-based resources
 * (remove ThreadLocals, destroy thread-scoped beans etc), as long as the context event
 * multicaster is not modified to use an async task executor.
 *
 * @author Youngjoo Kim
 * @since 4.1
 *
 * @see ShareConsumer
 */
public class ShareConsumerStoppingEvent extends KafkaEvent {

	@Serial
	private static final long serialVersionUID = 1L;

	private transient final ShareConsumer<?, ?> consumer;

	/**
	 * Construct an instance with the provided source, container, and share consumer.
	 * @param source the container instance that generated the event.
	 * @param container the container or the parent container if the container is a child.
	 * @param consumer the share consumer.
	 */
	public ShareConsumerStoppingEvent(Object source, Object container, ShareConsumer<?, ?> consumer) {
		super(source, container);
		this.consumer = consumer;
	}

	/**
	 * Return the share consumer.
	 * @return the share consumer.
	 */
	public ShareConsumer<?, ?> getConsumer() {
		return this.consumer;
	}

	@Override
	public String toString() {
		return "ShareConsumerStoppingEvent [consumer=" + this.consumer + "]";
	}

}
