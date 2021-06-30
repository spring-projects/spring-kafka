package com.example;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Suppressed;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.KafkaStreamsConfiguration;
import org.springframework.stereotype.Component;

import java.time.Duration;


/**
 * A basic topology that counts records by key and materialises the output into a new topic
 *
 * @author Nacho Munoz
 * @since 2.7.1
 */
@Configuration
@Component
public class Topology {
	private final String inputTopic;
	private final String outputTopic;
	private final KafkaProperties configuredKafkaProperties;

	@Autowired
	public Topology(@Value("${input-topic.name}") final String inputTopic,
					@Value("${output-topic.name}") final String outputTopic,
					final KafkaProperties configuredKafkaProperties) {
		this.inputTopic = inputTopic;
		this.outputTopic = outputTopic;
		this.configuredKafkaProperties = configuredKafkaProperties;
	}

	@Bean
	KafkaStreamsConfiguration defaultKafkaStreamsConfig() {
		return new KafkaStreamsConfiguration(configuredKafkaProperties.buildStreamsProperties());
	}

	@Autowired
	public void defaultTopology(final StreamsBuilder builder) {
		builder.stream(inputTopic, Consumed.with(Serdes.Integer(), Serdes.String()))
				.groupByKey()
				.count()
				.suppress(Suppressed.untilTimeLimit(Duration.ofMillis(5), Suppressed.BufferConfig.unbounded()))
				.toStream()
				.to(outputTopic);

	}
}
