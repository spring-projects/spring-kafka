/*
 * Copyright 2021-present the original author or authors.
 */

package com.example;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.kafka.annotation.DltHandler;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.annotation.RetryableTopic;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.kafka.annotation.BackOff;

/**
 * Sample shows use of topic-based retry.
 *
 * @author Gary Russell
 * @since 2.7.1
 *
 */
@SpringBootApplication
public class Application {

	private final Logger logger = LoggerFactory.getLogger(Application.class);

	public static void main(String[] args) {
		SpringApplication.run(Application.class, args);
	}

	@RetryableTopic(attempts = "5", backOff = @BackOff(delay = 2_000, maxDelay = 10_000, multiplier = 2))
	@KafkaListener(id = "fooGroup", topics = "topic4")
	public void listen(String in, @Header(KafkaHeaders.RECEIVED_TOPIC) String topic,
			@Header(KafkaHeaders.OFFSET) long offset) {

		this.logger.info("Received: {} from {} @ {}", in, topic, offset);
		if (in.startsWith("fail")) {
			throw new RuntimeException("failed");
		}
	}

	@DltHandler
	public void listenDlt(String in, @Header(KafkaHeaders.RECEIVED_TOPIC) String topic,
			@Header(KafkaHeaders.OFFSET) long offset) {

		this.logger.info("DLT Received: {} from {} @ {}", in, topic, offset);
	}

}
