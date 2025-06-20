/*
 * Copyright 2018-present the original author or authors.
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

/**
 * An event published when a concurrent container is stopped.
 *
 * @author Lokesh Alamuri
 * @since 3.3
 *
 */
public class ConcurrentContainerStoppedEvent extends KafkaEvent {

	@Serial
	private static final long serialVersionUID = 1L;

	private final ConsumerStoppedEvent.Reason reason;

	/**
	 * Construct an instance with the provided source and container.
	 * @param source the container instance that generated the event.
	 * @param reason the reason.
	 */
	public ConcurrentContainerStoppedEvent(Object source, ConsumerStoppedEvent.Reason reason) {
		super(source, source);
		this.reason = reason;
	}

	/**
	 * Return the reason why the container was stopped.
	 * @return the reason.
	 */
	public ConsumerStoppedEvent.Reason getReason() {
		return this.reason;
	}

	@Override
	public String toString() {
		return "ConcurrentContainerStoppedEvent [source=" + getSource() + ", reason=" + this.reason + "]";
	}

}
