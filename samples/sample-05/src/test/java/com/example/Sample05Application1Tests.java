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

import static org.awaitility.Awaitility.await;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.system.CapturedOutput;
import org.springframework.boot.test.system.OutputCaptureExtension;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.test.annotation.DirtiesContext;

/**
 * This test is going to fail from IDE since there is no exposed {@code spring.embedded.kafka.brokers} system property.
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
