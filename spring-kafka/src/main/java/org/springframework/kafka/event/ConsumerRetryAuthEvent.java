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

package org.springframework.kafka.event;

/**
 * An event published when authentication or authorization of a consumer fails and
 * is being retried. Contains the reason for this event.
 *
 * @author Daniel Gentes
 * @since 3.0
 *
 */
public class ConsumerRetryAuthEvent extends KafkaEvent {

	private static final long serialVersionUID = 1L;

	/**
	 * Reasons for retrying auth a consumer.
	 */
	public enum Reason {
		/**
		 * An authentication exception occurred.
		 */
		AUTHENTICATION,

		/**
		 * An authorization exception occurred.
		 */
		AUTHORIZATION
	}

	private final Reason reason;

	/**
	 * Construct an instance with the provided source and container.
	 * @param source the container instance that generated the event.
	 * @param container the container or the parent container
	 *                     if the container is a child.
	 * @param reason the reason.
	 */
	public ConsumerRetryAuthEvent(Object source, Object container, Reason reason) {
		super(source, container);
		this.reason = reason;
	}

	/**
	 * Return the reason for the auth failure.
	 * @return the reason.
	 */
	public Reason getReason() {
		return this.reason;
	}

	@Override
	public String toString() {
		return "ConsumerRetryAuthEvent [source=" + getSource() + ", reason=" + this.reason + "]";
	}

}
