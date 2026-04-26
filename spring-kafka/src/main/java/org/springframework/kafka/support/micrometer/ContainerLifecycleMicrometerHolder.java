/*
 * Copyright 2016-present the original author or authors.
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

package org.springframework.kafka.support.micrometer;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;

/**
 * Micrometer holder for container lifecycle metrics (start/stop counts).
 *
 * @author Vineeth Yelagandula
 * @since 3.4
 */
public class ContainerLifecycleMicrometerHolder {

	private final Counter startCounter;

	private final Counter stopCounter;

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
