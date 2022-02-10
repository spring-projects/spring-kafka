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

package org.springframework.kafka.core;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.regex.Pattern;

import org.apache.kafka.clients.consumer.ConsumerGroupMetadata;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.common.Metric;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.TopicPartition;

import org.springframework.util.Assert;

/**
 * A {@link KafkaTemplate} that routes messages based on the topic name. Does not support
 * transactions, {@link #flush()}, {@link #metrics()}, and
 * {@link #execute(org.springframework.kafka.core.KafkaOperations.ProducerCallback)},
 * only simple send operations.
 *
 * @author Gary Russell
 * @since 2.5
 *
 */
public class RoutingKafkaTemplate extends KafkaTemplate<Object, Object> {

	private static final String THIS_METHOD_IS_NOT_SUPPORTED = "This method is not supported";

	private final Map<Pattern, ProducerFactory<Object, Object>> factoryMatchers;

	private final ConcurrentMap<String, ProducerFactory<Object, Object>> factoryMap = new ConcurrentHashMap<>();

	/**
	 * Construct an instance with the provided properties. The topic patterns will be
	 * traversed in order so an ordered map, such as {@link LinkedHashMap} should be used
	 * with more specific patterns declared first.
	 * @param factories the factories.
	 */
	public RoutingKafkaTemplate(Map<Pattern, ProducerFactory<Object, Object>> factories) {
		super(new ProducerFactory<Object, Object>() {

			@Override
			public Producer<Object, Object> createProducer() {
				throw new UnsupportedOperationException();
			}

		});
		this.factoryMatchers = new LinkedHashMap<>(factories);
		Optional<Boolean> transactional = factories.values().stream()
			.map(fact -> fact.transactionCapable())
			.findFirst();
		Assert.isTrue(!transactional.isPresent() || !transactional.get(), "Transactional factories are not supported");
	}

	@Override
	public ProducerFactory<Object, Object> getProducerFactory() {
		throw new UnsupportedOperationException(THIS_METHOD_IS_NOT_SUPPORTED);
	}

	@Override
	public ProducerFactory<Object, Object> getProducerFactory(String topic) {
		ProducerFactory<Object, Object> producerFactory = this.factoryMap.computeIfAbsent(topic, key -> {
			for (Entry<Pattern, ProducerFactory<Object, Object>> entry : this.factoryMatchers.entrySet()) {
				if (entry.getKey().matcher(topic).matches()) {
					return entry.getValue();
				}
			}
			return null;
		});
		Assert.state(producerFactory != null, "No producer factory found for topic: " + topic);
		return producerFactory;
	}

	@Override
	public <T> T execute(ProducerCallback<Object, Object, T> callback) {
		throw new UnsupportedOperationException(THIS_METHOD_IS_NOT_SUPPORTED);
	}

	@Override
	public <T> T executeInTransaction(OperationsCallback<Object, Object, T> callback) {
		throw new UnsupportedOperationException(THIS_METHOD_IS_NOT_SUPPORTED);
	}

	@Override
	public void sendOffsetsToTransaction(Map<TopicPartition, OffsetAndMetadata> offsets,
			ConsumerGroupMetadata groupMetadata) {

		throw new UnsupportedOperationException(THIS_METHOD_IS_NOT_SUPPORTED);
	}

	@Override
	public Map<MetricName, ? extends Metric> metrics() {
		throw new UnsupportedOperationException(THIS_METHOD_IS_NOT_SUPPORTED);
	}

	@Override
	public void flush() {
		throw new UnsupportedOperationException(THIS_METHOD_IS_NOT_SUPPORTED);
	}

}
