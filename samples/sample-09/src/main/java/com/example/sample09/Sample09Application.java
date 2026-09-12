/*
 * Copyright 2026-present the original author or authors.
 */

package com.example.sample09;

import org.apache.kafka.clients.admin.NewTopic;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.ApplicationRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.core.KafkaTemplate;

/**
 * Kafka Queues sample which purpose is only to demonstrate the record acknowledgment
 * types of a share consumer (KIP-932) in Spring for Apache Kafka.
 * Three records are emitted, one for each acknowledgment type exercised by
 * {@link Sample09ShareListener}.
 *
 * @author Omar Morales Ortega
 */
@SpringBootApplication
public class Sample09Application {

	public static void main(String[] args) {
		SpringApplication.run(Sample09Application.class, args);
	}

	@Bean
	NewTopic shareDemoTopic(@Value("${sample09.topic}") String topic) {
		return TopicBuilder.name(topic).partitions(1).replicas(1).build();
	}

	@Bean
	ApplicationRunner sendDemoRecords(KafkaTemplate<String, String> kafkaTemplate,
			@Value("${sample09.topic}") String topic) {

		return args -> {
			kafkaTemplate.send(topic, "accept", "order-1 processed successfully");
			kafkaTemplate.send(topic, "release", "order-2 hit a transient failure");
			kafkaTemplate.send(topic, "reject", "order-3 is malformed");
		};
	}

}
