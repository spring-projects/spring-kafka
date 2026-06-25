/*
 * Copyright 2024-present the original author or authors.
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
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests for {@link ContainerLifecycleMicrometerHolder}.
 *
 * @author Vineeth Yelagandula
 * @since 4.1.0
 */
public class ContainerLifecycleMicrometerHolderTests {

	@Test
	void startCounterIncrementsOnRecordStart() {
		MeterRegistry registry = new SimpleMeterRegistry();
		ContainerLifecycleMicrometerHolder holder =
				new ContainerLifecycleMicrometerHolder(registry, "testContainer");

		holder.recordStart();
		holder.recordStart();

		Counter startCounter = registry.find("spring.kafka.container.start.count")
				.tag("name", "testContainer")
				.counter();
		assertThat(startCounter).isNotNull();
		assertThat(startCounter.count()).isEqualTo(2.0);
	}

	@Test
	void stopCounterIncrementsOnRecordStop() {
		MeterRegistry registry = new SimpleMeterRegistry();
		ContainerLifecycleMicrometerHolder holder =
				new ContainerLifecycleMicrometerHolder(registry, "testContainer");

		holder.recordStop();
		holder.recordStop();
		holder.recordStop();

		Counter stopCounter = registry.find("spring.kafka.container.stop.count")
				.tag("name", "testContainer")
				.counter();
		assertThat(stopCounter).isNotNull();
		assertThat(stopCounter.count()).isEqualTo(3.0);
	}

	@Test
	void countersAreTaggedWithContainerName() {
		MeterRegistry registry = new SimpleMeterRegistry();
		ContainerLifecycleMicrometerHolder holder =
				new ContainerLifecycleMicrometerHolder(registry, "myContainer");

		holder.recordStart();
		holder.recordStop();

		assertThat(registry.find("spring.kafka.container.start.count")
				.tag("name", "myContainer").counter()).isNotNull();
		assertThat(registry.find("spring.kafka.container.stop.count")
				.tag("name", "myContainer").counter()).isNotNull();
	}

	@Test
	void startAndStopCountersAreIndependent() {
		MeterRegistry registry = new SimpleMeterRegistry();
		ContainerLifecycleMicrometerHolder holder =
				new ContainerLifecycleMicrometerHolder(registry, "testContainer");

		holder.recordStart();
		holder.recordStop();
		holder.recordStop();

		Counter startCounter = registry.find("spring.kafka.container.start.count")
				.tag("name", "testContainer").counter();
		Counter stopCounter = registry.find("spring.kafka.container.stop.count")
				.tag("name", "testContainer").counter();

		assertThat(startCounter).isNotNull();
		assertThat(stopCounter).isNotNull();
		assertThat(startCounter.count()).isEqualTo(1.0);
		assertThat(stopCounter.count()).isEqualTo(2.0);
	}

}
