/*
 * Copyright 2022-present the original author or authors.
 */

package com.example.sample07;

import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

/**
 * New consumer rebalance protocol sample which purpose is only to demonstrate the application
 * of the New Consumer Rebalance Protocol in Spring Kafka.
 * Each consumer will subscribe test-topic with different group id.
 * Then, new consumer rebalance protocol will be completed successfully.
 *
 * @author Sanghyeok An.
 *
 * @since 3.2.0
 */

@Component
public class Sample07KafkaListener {

	@KafkaListener(topics = "test-topic", groupId = "sample07-1")
	public void listenWithGroup1(String message) {
		System.out.println("Received message at group sample07-1: " + message);
	}

	@KafkaListener(topics = "test-topic", groupId = "sample07-2")
	public void listenWithGroup2(String message) {
		System.out.println("Received message at group sample07-2: " + message);
	}
}
