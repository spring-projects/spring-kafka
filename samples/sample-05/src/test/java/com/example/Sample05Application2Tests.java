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

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.system.CapturedOutput;
import org.springframework.boot.test.system.OutputCaptureExtension;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.test.annotation.DirtiesContext;

/**
 * This test is going to fail from IDE since there is no exposed {@code spring.embedded.kafka.brokers} system property.
 * This test is deliberately failing to demonstrate that global embedded Kafka broker config for
 * {@code auto.create.topics.enable=false} is in an effect.
 * See {@code /resources/kafka-broker.properties} and Maven Surefire plugin configuration.
 */
@SpringBootTest
@DirtiesContext
class Sample05Application2Tests {

	@Autowired
	KafkaTemplate<String, String> kafkaTemplate;

	@Test
	void testKafkaTemplateSend() throws ExecutionException, InterruptedException, TimeoutException {
		SendResult<String, String> sendResult =
				this.kafkaTemplate.send("nonExistingTopic", "fake data").get(10, TimeUnit.SECONDS);

		assertThat(sendResult).isNotNull();
	}

}
