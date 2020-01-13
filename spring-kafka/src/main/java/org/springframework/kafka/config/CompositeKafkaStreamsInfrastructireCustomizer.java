/*
 * Copyright 2018-2019 the original author or authors.
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

package org.springframework.kafka.config;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;

/**
 * Composite {@link KafkaStreamsCustomizer} customizes {@link KafkaStreams} by delegating
 * to a list of provided {@link KafkaStreamsCustomizer}.
 *
 * @author Gary Russell
 *
 * @since 2.4.1
 */
public class CompositeKafkaStreamsInfrastructireCustomizer implements KafkaStreamsInfrastructureCustomizer {

	private final List<KafkaStreamsInfrastructureCustomizer> kafkaStreamsCustomizers = new ArrayList<>();

	/**
	 * Construct an instance with the provided customizers.
	 * @param customizers the customizers;
	 */
	public CompositeKafkaStreamsInfrastructireCustomizer(KafkaStreamsInfrastructureCustomizer... customizers) {

		this.kafkaStreamsCustomizers.addAll(Arrays.asList(customizers));
	}

	/**
	 * Add customizers.
	 * @param customizers the customizers.
	 */
	public void addKafkaStreamsCustomizers(KafkaStreamsInfrastructureCustomizer... customizers) {
		this.kafkaStreamsCustomizers.addAll(Arrays.asList(customizers));
	}

	@Override
	public void configureBuilder(StreamsBuilder builder) {
		this.kafkaStreamsCustomizers.forEach(cust -> cust.configureBuilder(builder));
	}

	@Override
	public void configureTopology(Topology topology) {
		this.kafkaStreamsCustomizers.forEach(cust -> cust.configureTopology(topology));
	}

}
