/*
 * Copyright 2020-2022 the original author or authors.
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

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.kafka.streams.KafkaStreams;

import org.springframework.kafka.config.StreamsBuilderFactoryBean;

import io.micrometer.core.instrument.ImmutableTag;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Tag;
import io.micrometer.core.instrument.binder.kafka.KafkaStreamsMetrics;

/**
 * Creates a {@link KafkaStreamsMetrics} for the {@link KafkaStreams}.
 *
 * @author Gary Russell
 * @since 2.5.3
 *
 */
public class KafkaStreamsMicrometerListener implements StreamsBuilderFactoryBean.Listener {

	private final MeterRegistry meterRegistry;

	private final List<Tag> tags;

	private final Map<String, KafkaStreamsMetrics> metrics = new HashMap<>();

	/**
	 * Construct an instance with the provided registry.
	 * @param meterRegistry the registry.
	 */
	public KafkaStreamsMicrometerListener(MeterRegistry meterRegistry) {
		this(meterRegistry, Collections.emptyList());
	}

	/**
	 * Construct an instance with the provided registry and tags.
	 * @param meterRegistry the registry.
	 * @param tags the tags.
	 */
	public KafkaStreamsMicrometerListener(MeterRegistry meterRegistry, List<Tag> tags) {
		this.meterRegistry = meterRegistry;
		this.tags = tags;
	}


	@Override
	public synchronized void streamsAdded(String id, KafkaStreams kafkaStreams) {
		if (!this.metrics.containsKey(id)) {
			List<Tag> streamsTags = new ArrayList<>(this.tags);
			streamsTags.add(new ImmutableTag("spring.id", id));
			this.metrics.put(id, new KafkaStreamsMetrics(kafkaStreams, streamsTags));
			this.metrics.get(id).bindTo(this.meterRegistry);
		}
	}

	@Override
	public synchronized void streamsRemoved(String id, KafkaStreams streams) {
		KafkaStreamsMetrics removed = this.metrics.remove(id);
		if (removed != null) {
			removed.close();
		}
	}

}
