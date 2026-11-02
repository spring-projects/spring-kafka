/*
 * Copyright 2022-present the original author or authors.
 */

package com.example;

import java.util.concurrent.TimeUnit;

import org.apache.kafka.common.errors.TimeoutException;
import org.junit.jupiter.api.Test;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.KafkaException;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.test.annotation.DirtiesContext;

import static org.assertj.core.api.Assertions.assertThatExceptionOfType;

/**
 * This test is going to fail from IDE since there is no exposed {@code spring.kafka.bootstrap-servers} system property.
 * This test demonstrates that global embedded Kafka broker config for {@code auto.create.topics.enable=false}
 * is in an effect - the topic {@code nonExistingTopic} does not exist on the broker.
 * See {@code /resources/kafka-broker.properties} and Maven Surefire plugin configuration.
 */
@SpringBootTest
@DirtiesContext
class Sample05Application2Tests {

	@Autowired
	KafkaTemplate<String, String> kafkaTemplate;

	@Test
	void testKafkaTemplateSend() {
		assertThatExceptionOfType(KafkaException.class)
				.isThrownBy(() ->
						this.kafkaTemplate.send("nonExistingTopic", "fake data").get(10, TimeUnit.SECONDS))
				.withRootCauseExactlyInstanceOf(TimeoutException.class)
				.withMessageContaining("Send failed")
				.withStackTraceContaining("Topic nonExistingTopic not present in metadata");
	}

}
