/*
 * Copyright 2020-present the original author or authors.
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

package org.springframework.kafka.core;

import java.util.Collections;
import java.util.List;

import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Tag;
import io.micrometer.core.instrument.binder.kafka.KafkaClientMetrics;
import org.apache.kafka.clients.consumer.Consumer;
import org.jspecify.annotations.Nullable;

import org.springframework.scheduling.TaskScheduler;

/**
 * A consumer factory listener that manages {@link KafkaClientMetrics}.
 *
 * @param <K> the key type.
 * @param <V> the value type.
 *
 * @author Gary Russell
 * @author Artem Bilan
 *
 * @since 2.5
 */
public class MicrometerConsumerListener<K, V> extends KafkaMetricsSupport<Consumer<K, V>>
		implements ConsumerFactory.Listener<K, V> {

	/**
	 * Construct an instance with the provided registry.
	 * @param meterRegistry the registry.
	 */
	public MicrometerConsumerListener(MeterRegistry meterRegistry) {
		this(meterRegistry, Collections.emptyList());
	}

	/**
	 * Construct an instance with the provided registry and task scheduler.
	 * @param meterRegistry the registry.
	 * @param taskScheduler the task scheduler.
	 * @since 3.3
	 */
	public MicrometerConsumerListener(MeterRegistry meterRegistry, TaskScheduler taskScheduler) {
		this(meterRegistry, Collections.emptyList(), taskScheduler);
	}

	/**
	 * Construct an instance with the provided registry and tags.
	 * @param meterRegistry the registry.
	 * @param tags the tags.
	 */
	public MicrometerConsumerListener(MeterRegistry meterRegistry, List<Tag> tags) {
		super(meterRegistry, tags);
	}

	/**
	 * Construct an instance with the provided registry, tags and task scheduler.
	 * @param meterRegistry the registry.
	 * @param tags the tags.
	 * @param taskScheduler the task scheduler.
	 * @since 3.3
	 */
	public MicrometerConsumerListener(MeterRegistry meterRegistry, List<Tag> tags, TaskScheduler taskScheduler) {
		super(meterRegistry, tags, taskScheduler);
	}

	@Override
	public synchronized void consumerAdded(String id, Consumer<K, V> consumer) {
		bindClient(id, consumer);
	}

	@Override
	public synchronized void consumerRemoved(@Nullable String id, Consumer<K, V> consumer) {
		unbindClient(id, consumer);
	}

}
