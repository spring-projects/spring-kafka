/*
 * Copyright 2018-2020 the original author or authors.
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

/**
 * An event published when a consumer is stopped. While it is best practice to use
 * stateless listeners, you can consume this event to clean up any thread-based resources
 * (remove ThreadLocals, destroy thread-scoped beans etc), as long as the context event
 * multicaster is not modified to use an async task executor. You can also use this event
 * to restart a container that was stopped because a transactional producer was fenced.
 *
 * @author Gary Russell
 * @since 2.2
 *
 */
public class ConsumerStoppedEvent extends KafkaEvent {

	private static final long serialVersionUID = 1L;

	/**
	 * Reasons for stopping a consumer.
	 * @since 2.5.9
	 *
	 */
	public enum Reason {

		/**
		 * The consumer was stopped because the container was stopped.
		 */
		NORMAL,

		/**
		 * The transactional producer was fenced and the container
		 * {@code stopContainerWhenFenced} property is true.
		 */
		FENCED,

		/**
		 * An authorization exception occurred.
		 * @since 2.5.10
		 */
		AUTH,

		/**
		 * No offset found for a partition and no reset policy.
		 * @since 2.5.10
		 */
		NO_OFFSET,

		/**
		 * A {@link java.lang.Error} was thrown.
		 */
		ERROR

	}

	private final Reason reason;

	/**
	 * Construct an instance with the provided source and container.
	 * @param source the container instance that generated the event.
	 * @param container the container or the parent container if the container is a child.
	 * @param reason the reason.
	 * @since 2.5.8
	 */
	public ConsumerStoppedEvent(Object source, Object container, Reason reason) {
		super(source, container);
		this.reason = reason;
	}

	/**
	 * Return the reason why the consumer was stopped.
	 * @return the reason.
	 * @since 2.5.8
	 */
	public Reason getReason() {
		return this.reason;
	}

	@Override
	public String toString() {
		return "ConsumerStoppedEvent [source=" + getSource() + ", reason=" + this.reason + "]";
	}

}
