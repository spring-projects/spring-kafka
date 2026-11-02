/*
 * Copyright 2022-present the original author or authors.
 */

package org.springframework.kafka.event;

import java.io.Serial;

/**
 * An event published when authentication or authorization of a consumer fails and
 * is being retried. Contains the reason for this event.
 *
 * @author Daniel Gentes
 * @since 3.0
 *
 */
public class ConsumerRetryAuthEvent extends KafkaEvent {

	@Serial
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
