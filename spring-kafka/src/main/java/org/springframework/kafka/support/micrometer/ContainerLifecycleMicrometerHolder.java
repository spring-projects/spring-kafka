/*
 * Copyright 2026-present the original author or authors.
 */

package org.springframework.kafka.support.micrometer;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;

/**
 * Micrometer holder for container lifecycle metrics (start/stop counts).
 *
 * @author Vineeth Yelagandula
 *
 * @since 4.2.0
 */
public final class ContainerLifecycleMicrometerHolder {

	private final Counter startCounter;

	private final Counter stopCounter;

	/**
	 * Create an instance with the provided {@link MeterRegistry} and container name.
	 * @param registry the meter registry.
	 * @param containerName the container bean name.
	 */
	public ContainerLifecycleMicrometerHolder(MeterRegistry registry, String containerName) {
		this.startCounter = Counter.builder("spring.kafka.container.start.count")
				.description("Number of times this listener container has been started")
				.tag("name", containerName)
				.register(registry);
		this.stopCounter = Counter.builder("spring.kafka.container.stop.count")
				.description("Number of times this listener container has been stopped")
				.tag("name", containerName)
				.register(registry);
	}

	/**
	 * Increment the start counter.
	 */
	public void recordStart() {
		this.startCounter.increment();
	}

	/**
	 * Increment the stop counter.
	 */
	public void recordStop() {
		this.stopCounter.increment();
	}

}
