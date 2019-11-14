/*
 * Copyright 2018-2019 the original author or authors.
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

package org.springframework.kafka.listener;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;

import java.util.Map;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.condition.EmbeddedKafkaCondition;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.kafka.test.utils.KafkaTestUtils;

/**
 * @author Gary Russell
 * @since 2.2
 *
 */
@EmbeddedKafka(brokerProperties = "auto.create.topics.enable=false")
public class MissingTopicsTests {

	private static EmbeddedKafkaBroker embeddedKafka;

	@BeforeAll
	public static void setup() {
		embeddedKafka = EmbeddedKafkaCondition.getBroker();
	}

	@Test
	public void testMissingTopicCMLC() {
		Map<String, Object> props = KafkaTestUtils.consumerProps("missing1", "true", embeddedKafka);
		DefaultKafkaConsumerFactory<Integer, String> cf = new DefaultKafkaConsumerFactory<>(props);
		ContainerProperties containerProps = new ContainerProperties("notexisting");
		containerProps.setMessageListener((MessageListener<Integer, String>) message -> { });
		containerProps.setMissingTopicsFatal(true);
		ConcurrentMessageListenerContainer<Integer, String> container =
				new ConcurrentMessageListenerContainer<>(cf, containerProps);
		container.setBeanName("testMissing1");

		try {
			container.start();
			fail("Expected exception");
		}
		catch (IllegalStateException e) {
			assertThat(e.getMessage()).contains("missingTopicsFatal");
		}
	}

	@Test
	public void testMissingTopicKMLC() {
		Map<String, Object> props = KafkaTestUtils.consumerProps("missing2", "true", embeddedKafka);
		DefaultKafkaConsumerFactory<Integer, String> cf = new DefaultKafkaConsumerFactory<>(props);
		ContainerProperties containerProps = new ContainerProperties("notexisting");
		containerProps.setMessageListener((MessageListener<Integer, String>) message -> { });
		containerProps.setMissingTopicsFatal(true);
		KafkaMessageListenerContainer<Integer, String> container =
				new KafkaMessageListenerContainer<>(cf, containerProps);
		container.setBeanName("testMissing2");
		try {
			container.start();
			fail("Expected exception");
		}
		catch (IllegalStateException e) {
			assertThat(e.getMessage()).contains("missingTopicsFatal");
		}
		container.getContainerProperties().setMissingTopicsFatal(false);
		container.start();
		container.stop();
	}

}
