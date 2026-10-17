/*
 * Copyright 2026-present the original author or authors.
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
 *
 * @since 4.2
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
