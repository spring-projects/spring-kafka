/*
 * Copyright 2020-present the original author or authors.
 */

package org.springframework.kafka.core;

import java.util.Collections;
import java.util.List;

import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Tag;
import io.micrometer.core.instrument.binder.kafka.KafkaClientMetrics;
import org.apache.kafka.clients.producer.Producer;

import org.springframework.scheduling.TaskScheduler;

/**
 * A producer factory listener that manages {@link KafkaClientMetrics}.
 *
 * @param <K> the key type.
 * @param <V> the value type.
 *
 * @author Gary Russell
 * @author Artem Bilan
 *
 * @since 2.5
 */
public class MicrometerProducerListener<K, V> extends KafkaMetricsSupport<Producer<K, V>>
		implements ProducerFactory.Listener<K, V> {

	/**
	 * Construct an instance with the provided registry.
	 * @param meterRegistry the registry.
	 */
	public MicrometerProducerListener(MeterRegistry meterRegistry) {
		this(meterRegistry, Collections.emptyList());
	}

	/**
	 * Construct an instance with the provided registry and task scheduler.
	 * @param meterRegistry the registry.
	 * @param taskScheduler the task scheduler.
	 * @since 3.3
	 */
	public MicrometerProducerListener(MeterRegistry meterRegistry, TaskScheduler taskScheduler) {
		this(meterRegistry, Collections.emptyList(), taskScheduler);
	}

	/**
	 * Construct an instance with the provided registry and tags.
	 * @param meterRegistry the registry.
	 * @param tags the tags.
	 */
	public MicrometerProducerListener(MeterRegistry meterRegistry, List<Tag> tags) {
		super(meterRegistry, tags);
	}

	/**
	 * Construct an instance with the provided registry, tags and task scheduler.
	 * @param meterRegistry the registry.
	 * @param tags the tags.
	 * @param taskScheduler the task scheduler.
	 * @since 3.3
	 */
	public MicrometerProducerListener(MeterRegistry meterRegistry, List<Tag> tags, TaskScheduler taskScheduler) {
		super(meterRegistry, tags, taskScheduler);
	}

	@Override
	public synchronized void producerAdded(String id, Producer<K, V> producer) {
		bindClient(id, producer);
	}

	@Override
	public synchronized void producerRemoved(String id, Producer<K, V> producer) {
		unbindClient(id, producer);
	}

}
