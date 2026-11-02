/*
 * Copyright 2020-present the original author or authors.
 */

package org.springframework.kafka.config;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;

/**
 * Composite {@link KafkaStreamsInfrastructureCustomizer} customizes
 * {@link org.apache.kafka.streams.KafkaStreams} by delegating to a list of provided
 * {@link KafkaStreamsInfrastructureCustomizer}.
 *
 * @author Gary Russell
 *
 * @since 2.4.1
 */
public class CompositeKafkaStreamsInfrastructureCustomizer implements KafkaStreamsInfrastructureCustomizer {

	private final List<KafkaStreamsInfrastructureCustomizer> infrastructureCustomizers = new ArrayList<>();

	/**
	 * Construct an instance with the provided customizers.
	 * @param customizers the customizers;
	 */
	public CompositeKafkaStreamsInfrastructureCustomizer(KafkaStreamsInfrastructureCustomizer... customizers) {
		this.infrastructureCustomizers.addAll(Arrays.asList(customizers));
	}

	/**
	 * Add customizers.
	 * @param customizers the customizers.
	 */
	public void addKafkaStreamsCustomizers(KafkaStreamsInfrastructureCustomizer... customizers) {
		this.infrastructureCustomizers.addAll(Arrays.asList(customizers));
	}

	@Override
	public void configureBuilder(StreamsBuilder builder) {
		this.infrastructureCustomizers.forEach(cust -> cust.configureBuilder(builder));
	}

	@Override
	public void configureTopology(Topology topology) {
		this.infrastructureCustomizers.forEach(cust -> cust.configureTopology(topology));
	}

}
