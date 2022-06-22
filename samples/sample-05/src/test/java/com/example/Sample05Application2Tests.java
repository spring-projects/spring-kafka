/*
 * Copyright 2022 the original author or authors.
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

package com.example;

import static org.assertj.core.api.Assertions.assertThatExceptionOfType;

import java.util.concurrent.TimeUnit;

import org.apache.kafka.common.errors.TimeoutException;
import org.junit.jupiter.api.Test;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.KafkaException;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.test.annotation.DirtiesContext;

/**
 * This test is going to fail from IDE since there is no exposed {@code spring.embedded.kafka.brokers} system property.
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
