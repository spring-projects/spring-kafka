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

package org.springframework.kafka.streams;

import java.util.Collections;
import java.util.List;

import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Tag;
import io.micrometer.core.instrument.binder.MeterBinder;
import io.micrometer.core.instrument.binder.kafka.KafkaStreamsMetrics;
import org.apache.kafka.streams.KafkaStreams;

import org.springframework.kafka.config.StreamsBuilderFactoryBean;
import org.springframework.kafka.core.KafkaMetricsSupport;
import org.springframework.scheduling.TaskScheduler;

/**
 * Creates a {@link KafkaStreamsMetrics} for the {@link KafkaStreams}.
 *
 * @author Gary Russell
 * @author Artem Bilan
 *
 * @since 2.5.3
 *
 */
public class KafkaStreamsMicrometerListener extends KafkaMetricsSupport<KafkaStreams>
		implements StreamsBuilderFactoryBean.Listener {

	/**
	 * Construct an instance with the provided registry.
	 * @param meterRegistry the registry.
	 */
	public KafkaStreamsMicrometerListener(MeterRegistry meterRegistry) {
		this(meterRegistry, Collections.emptyList());
	}

	/**
	 * Construct an instance with the provided registry and task scheduler.
	 * @param meterRegistry the registry.
	 * @param taskScheduler the task scheduler.
	 * @since 3.3
	 */
	public KafkaStreamsMicrometerListener(MeterRegistry meterRegistry, TaskScheduler taskScheduler) {
		this(meterRegistry, Collections.emptyList(), taskScheduler);
	}

	/**
	 * Construct an instance with the provided registry and tags.
	 * @param meterRegistry the registry.
	 * @param tags the tags.
	 */
	public KafkaStreamsMicrometerListener(MeterRegistry meterRegistry, List<Tag> tags) {
		super(meterRegistry, tags);
	}

	/**
	 * Construct an instance with the provided registry, tags and task scheduler.
	 * @param meterRegistry the registry.
	 * @param tags the tags.
	 * @param taskScheduler the task scheduler.
	 * @since 3.3
	 */
	public KafkaStreamsMicrometerListener(MeterRegistry meterRegistry, List<Tag> tags, TaskScheduler taskScheduler) {
		super(meterRegistry, tags, taskScheduler);
	}

	@Override
	public synchronized void streamsAdded(String id, KafkaStreams kafkaStreams) {
		bindClient(id, kafkaStreams);
	}

	@Override
	protected MeterBinder createClientMetrics(KafkaStreams client, List<Tag> tags) {
		return this.scheduler != null
				? new KafkaStreamsMetrics(client, tags, this.scheduler)
				: new KafkaStreamsMetrics(client, tags);
	}

	@Override
	public synchronized void streamsRemoved(String id, KafkaStreams streams) {
		unbindClient(id, streams);
	}

}
