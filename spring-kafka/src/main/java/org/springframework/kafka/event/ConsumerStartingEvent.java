/*
 * Copyright 2018-present the original author or authors.
 */

package org.springframework.kafka.event;

import java.io.Serial;

/**
 * An event published when a consumer is initializing.
 *
 * @author Gary Russell
 * @since 2.3
 *
 */
public class ConsumerStartingEvent extends KafkaEvent {

	@Serial
	private static final long serialVersionUID = 1L;

	/**
	 * Construct an instance with the provided source and container.
	 * @param source the container instance that generated the event.
	 * @param container the container or the parent container if the container is a child.
	 */
	public ConsumerStartingEvent(Object source, Object container) {
		super(source, container);
	}

	@Override
	public String toString() {
		return "ConsumerStartingEvent [source=" + getSource() + "]";
	}

}
