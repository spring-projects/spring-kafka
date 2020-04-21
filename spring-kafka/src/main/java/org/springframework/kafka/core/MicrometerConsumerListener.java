/*
 * Copyright 2020 the original author or authors.
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

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.kafka.clients.consumer.Consumer;

import io.micrometer.core.instrument.ImmutableTag;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Tag;
import io.micrometer.core.instrument.binder.kafka.KafkaClientMetrics;

/**
 * A consumer factory listener that manages {@link KafkaClientMetrics}.
 *
 * @param <K> the key type.
 * @param <V> the value type.
 *
 * @author Gary Russell
 * @since 2.5
 *
 */
public class MicrometerConsumerListener<K, V> implements ConsumerFactory.Listener<K, V> {

	private final MeterRegistry meterRegistry;

	private final List<Tag> tags;

	private final Map<String, KafkaClientMetrics> metrics = new HashMap<>();

	/**
	 * Construct an instance with the provided registry.
	 * @param meterRegistry the registry.
	 */
	public MicrometerConsumerListener(MeterRegistry meterRegistry) {
		this(meterRegistry, Collections.emptyList());
	}

	/**
	 * Construct an instance with the provided registry and tags.
	 * @param meterRegistry the registry.
	 * @param tags the tags.
	 */
	public MicrometerConsumerListener(MeterRegistry meterRegistry, List<Tag> tags) {
		this.meterRegistry = meterRegistry;
		this.tags = tags;
	}

	@Override
	public synchronized void consumerAdded(String id, Consumer<K, V> consumer) {
		if (!this.metrics.containsKey(id)) {
			List<Tag> consumerTags = new ArrayList<>(this.tags);
			consumerTags.add(new ImmutableTag("spring.id", id));
			this.metrics.put(id, new KafkaClientMetrics(consumer, consumerTags));
			this.metrics.get(id).bindTo(this.meterRegistry);
		}
	}

	@Override
	public synchronized void consumerRemoved(String id, Consumer<K, V> consumer) {
		KafkaClientMetrics removed = this.metrics.remove(id);
		if (removed != null) {
			removed.close();
		}
	}

}
