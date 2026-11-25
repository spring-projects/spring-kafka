/*
 * Copyright and licensing information for this source code can be found in
 * the NOTICE and LICENSE files distributed with this project.
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
