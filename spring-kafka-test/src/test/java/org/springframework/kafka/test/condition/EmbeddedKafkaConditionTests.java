/*
 * Copyright 2019-present the original author or authors.
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

package org.springframework.kafka.test.condition;

import java.time.Duration;

import org.junit.jupiter.api.Test;

import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.kafka.test.utils.KafkaTestUtils;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * @author Gary Russell
 * @author Michał Padula
 * @author Artem Bilan
 *
 * @since 2.3
 *
 */
@EmbeddedKafka(bootstrapServersProperty = "my.bss.property", count = 2, controlledShutdown = true, partitions = 3,
		adminTimeout = 67)
public class EmbeddedKafkaConditionTests {

	@Test
	public void test(EmbeddedKafkaBroker broker) {
		assertThat(broker.getBrokersAsString()).isNotNull();
		assertThat(KafkaTestUtils.getPropertyValue(broker, "brokerListProperty")).isEqualTo("my.bss.property");
		assertThat(KafkaTestUtils.getPropertyValue(broker, "adminTimeout")).isEqualTo(Duration.ofSeconds(67));
		assertThat(broker.getPartitionsPerTopic()).isEqualTo(3);
	}

	@Test
	public void testResolver(EmbeddedKafkaBroker broker) {
		assertThat(broker).isNotNull();
	}

}
