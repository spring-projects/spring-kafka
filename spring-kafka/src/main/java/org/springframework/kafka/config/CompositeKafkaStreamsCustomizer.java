/*
 * Copyright and licensing information for this source code can be found in
 * the NOTICE and LICENSE files distributed with this project.
 */

package org.springframework.kafka.config;

import java.util.ArrayList;
import java.util.List;

import org.apache.kafka.streams.KafkaStreams;

/**
 * Composite {@link KafkaStreamsCustomizer} customizes {@link KafkaStreams} by delegating
 * to a list of provided {@link KafkaStreamsCustomizer}.
 *
 * @author Nurettin Yilmaz
 * @author Artem Bilan
 *
 * @since 2.1.5
 */
public class CompositeKafkaStreamsCustomizer implements KafkaStreamsCustomizer {

	private final List<KafkaStreamsCustomizer> kafkaStreamsCustomizers = new ArrayList<>();

	public CompositeKafkaStreamsCustomizer() {
	}

	public CompositeKafkaStreamsCustomizer(List<KafkaStreamsCustomizer> kafkaStreamsCustomizers) {
		this.kafkaStreamsCustomizers.addAll(kafkaStreamsCustomizers);
	}

	@Override
	public void customize(KafkaStreams kafkaStreams) {
		this.kafkaStreamsCustomizers.forEach(kafkaStreamsCustomizer -> kafkaStreamsCustomizer.customize(kafkaStreams));
	}

	public void addKafkaStreamsCustomizers(List<KafkaStreamsCustomizer> customizers) {
		this.kafkaStreamsCustomizers.addAll(customizers);
	}

}
