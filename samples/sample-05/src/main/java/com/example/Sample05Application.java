/*
 * Copyright 2022-present the original author or authors.
 */

package com.example;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.kafka.annotation.KafkaListener;

/**
 * Plain Kafka listener sample which purpose is only to be used in the tests assertions.
 * See unit tests for this project for more information.
 *
 * @author Artem Bilan
 *
 * @since 3.0
 *
 */
@SpringBootApplication
public class Sample05Application {

	public static void main(String[] args) {
		SpringApplication.run(Sample05Application.class, args);
	}

	@KafkaListener(id ="sampleListener", topics = "topic1")
	void listenForTopic(String payload) {
		System.out.println("Received: " + payload);
	}

}
