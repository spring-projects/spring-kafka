/*
 * Copyright 2022-present the original author or authors.
 */

package org.springframework.kafka.event;

import java.io.Serial;

/**
 * An event published when authentication or authorization has been retried successfully.
 *
 * @author Daniel Gentes
 * @since 3.0
 *
 */
public class ConsumerRetryAuthSuccessfulEvent extends KafkaEvent {

	@Serial
	private static final long serialVersionUID = 1L;

	/**
	 * Construct an instance with the provided source and container.
	 * @param source the container instance that generated the event.
	 * @param container the container or the parent container
	 *                     if the container is a child.
	 */
	public ConsumerRetryAuthSuccessfulEvent(Object source, Object container) {
		super(source, container);
	}

	@Override
	public String toString() {
		return "ConsumerRetryAuthSuccessfulEvent [source=" + getSource() + "]";
	}

}
