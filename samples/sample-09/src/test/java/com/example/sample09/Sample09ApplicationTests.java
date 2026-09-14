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

import java.time.Duration;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.system.CapturedOutput;
import org.springframework.boot.test.system.OutputCaptureExtension;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.test.annotation.DirtiesContext;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * Verifies that the share consumer listener exercises all three acknowledgment types against an
 * embedded broker, including the redelivery that follows a {@code RELEASE}.
 *
 * @author Omar Morales Ortega
 *
 * @since 4.2
 */
@SpringBootTest
@DirtiesContext
@EmbeddedKafka(topics = "share-demo-topic", partitions = 1,
		brokerProperties = {
				"share.coordinator.state.topic.replication.factor=1",
				"share.coordinator.state.topic.min.isr=1"
		})
@ExtendWith(OutputCaptureExtension.class)
class Sample09ApplicationTests {

	@Test
	void allAcknowledgmentTypesAreExercised(CapturedOutput output) {
		await().atMost(Duration.ofSeconds(60))
				.untilAsserted(() -> assertThat(output.getOut())
						.contains("ACCEPT - order-1 processed successfully")
						.contains("RELEASE - order-2 hit a transient failure")
						.contains("ACCEPT (redelivered) - order-2 hit a transient failure")
						.contains("REJECT - order-3 is malformed"));
	}

}
