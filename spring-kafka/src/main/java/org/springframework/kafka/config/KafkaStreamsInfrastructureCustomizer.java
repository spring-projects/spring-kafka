/*
 * Copyright 2020-present the original author or authors.
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
