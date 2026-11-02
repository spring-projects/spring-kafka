/*
 * Copyright 2018-present the original author or authors.
 */

package org.springframework.kafka.event;

import java.io.Serial;

import org.jspecify.annotations.Nullable;

/**
 * An event published when a concurrent container is stopped.
 *
 * @author Lokesh Alamuri
 * @author Ngoc Nhan
 * @since 3.3
 *
 */
public class ConcurrentContainerStoppedEvent extends KafkaEvent {

	@Serial
	private static final long serialVersionUID = 1L;

	private final ConsumerStoppedEvent.@Nullable Reason reason;

	/**
	 * Construct an instance with the provided source and container.
	 * @param source the container instance that generated the event.
	 * @param reason the reason.
	 */
	public ConcurrentContainerStoppedEvent(Object source, ConsumerStoppedEvent.@Nullable Reason reason) {
		super(source, source);
		this.reason = reason;
	}

	/**
	 * Return the reason why the container was stopped.
	 * @return the reason.
	 */
	public ConsumerStoppedEvent.@Nullable Reason getReason() {
		return this.reason;
	}

	@Override
	public String toString() {
		return "ConcurrentContainerStoppedEvent [source=" + getSource() + ", reason=" + this.reason + "]";
	}

}
