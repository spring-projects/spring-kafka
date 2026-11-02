/*
 * Copyright 2018-present the original author or authors.
 */

package org.springframework.kafka.config;

import java.util.Properties;

import org.apache.kafka.streams.KafkaClientSupplier;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.Topology;

/**
 * Callback interface that can be used to configure {@link KafkaStreams} directly.
 *
 * @author Nurettin Yilmaz
 * @author Almog Gavra
 *
 * @since 2.1.5
 *
 * @see StreamsBuilderFactoryBean
 */
@FunctionalInterface
public interface KafkaStreamsCustomizer {

	/**
	 * Customize the instantiation of the {@code KafkaStreams} instance. This
	 * happens before the modifications made by {@link StreamsBuilderFactoryBean}.
	 *
	 * @param topology the full topology
	 * @param properties the configuration properties
	 * @param clientSupplier the client supplier
	 *
	 * @return a new instance of {@link KafkaStreams}
	 *
	 * @since 3.3.0
	 */
	default KafkaStreams initKafkaStreams(
			Topology topology,
			Properties properties,
			KafkaClientSupplier clientSupplier
	) {
		return new KafkaStreams(topology, properties, clientSupplier);
	}

	/**
	 * Customize the instance of {@code KafkaStreams} after {@link StreamsBuilderFactoryBean}
	 * has applied its default configurations.
	 *
	 * @param kafkaStreams the instantiated Kafka Streams instance
	 */
	void customize(KafkaStreams kafkaStreams);

}
