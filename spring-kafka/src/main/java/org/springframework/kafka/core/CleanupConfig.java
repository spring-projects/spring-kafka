/*
 * Copyright 2018-present the original author or authors.
 */

package org.springframework.kafka.core;

/**
 * Specifies time of {@link org.apache.kafka.streams.KafkaStreams#cleanUp()} execution.
 *
 * @author Pawel Szymczyk
 */
public class CleanupConfig {

	private final boolean onStart;

	private final boolean onStop;

	public CleanupConfig() {
		this(false, false);
	}

	public CleanupConfig(boolean onStart, boolean onStop) {
		this.onStart = onStart;
		this.onStop = onStop;
	}

	public boolean cleanupOnStart() {
		return this.onStart;
	}

	public boolean cleanupOnStop() {
		return this.onStop;
	}

}
