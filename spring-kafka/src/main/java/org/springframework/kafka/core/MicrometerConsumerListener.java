/*
 * Copyright 2020-present the original author or authors.
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
