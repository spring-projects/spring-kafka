/*
 * Copyright 2022-present the original author or authors.
 */

package com.example;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.system.CapturedOutput;
import org.springframework.boot.test.system.OutputCaptureExtension;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.test.annotation.DirtiesContext;

import static org.awaitility.Awaitility.await;

/**
 * This test is going to fail from IDE since there is no exposed {@code spring.kafka.bootstrap-servers} system property.
 * Use Maven to run tests which enables global embedded Kafka broker via properties provided to Surefire plugin.
 */
@ExtendWith(OutputCaptureExtension.class)
@SpringBootTest
@DirtiesContext
class Sample05Application1Tests {

	@Autowired
	KafkaTemplate<String, String> kafkaTemplate;

	@Test
	void testKafkaListener(CapturedOutput output) {
		this.kafkaTemplate.send("topic1", "testData");

		await().until(() -> output.getOut().contains("Received: testData"));
	}

}
