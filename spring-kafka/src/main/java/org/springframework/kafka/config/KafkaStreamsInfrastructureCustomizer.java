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

package org.springframework.kafka.config;

import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;

/**
 * A customizer for infrastructure components such as the {@code StreamsBuilder} and
 * {@code Topology}. It can be provided to the {@link StreamsBuilderFactoryBean} which
 * will apply the changes before creating the stream.
 *
 * @author Gary Russell
 * @since 2.4.1
 *
 */
public interface KafkaStreamsInfrastructureCustomizer {

	/**
	 * Configure the builder.
	 * @param builder the builder.
	 */
	default void configureBuilder(StreamsBuilder builder) {
		// no-op
	}

	/**
	 * Configure the topology.
	 * @param topology the topology
	 */
	default void configureTopology(Topology topology) {
		// no-op
	}

}
