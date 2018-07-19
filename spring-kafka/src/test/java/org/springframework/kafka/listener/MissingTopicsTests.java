/*
 * Copyright 2018 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
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

import org.junit.ClassRule;
import org.junit.Test;

import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.rule.EmbeddedKafkaRule;
import org.springframework.kafka.test.utils.KafkaTestUtils;

/**
 * @author Gary Russell
 * @since 2.2
 *
 */
public class MissingTopicsTests {

	@ClassRule
	public static EmbeddedKafkaRule embeddedKafkaRule = new EmbeddedKafkaRule(1, true)
		.brokerProperty("auto.create.topics.enable", false);

	private static EmbeddedKafkaBroker embeddedKafka = embeddedKafkaRule.getEmbeddedKafka();

	@Test
	public void testMissingTopicCMLC() throws Exception {
		Map<String, Object> props = KafkaTestUtils.consumerProps("missing1", "true", embeddedKafka);
		DefaultKafkaConsumerFactory<Integer, String> cf = new DefaultKafkaConsumerFactory<>(props);
		ContainerProperties containerProps = new ContainerProperties("notexisting");
		containerProps.setMessageListener((MessageListener<Integer, String>) message -> { });
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
	public void testMissingTopicKMLC() throws Exception {
		Map<String, Object> props = KafkaTestUtils.consumerProps("missing2", "true", embeddedKafka);
		DefaultKafkaConsumerFactory<Integer, String> cf = new DefaultKafkaConsumerFactory<>(props);
		ContainerProperties containerProps = new ContainerProperties("notexisting");
		containerProps.setMessageListener((MessageListener<Integer, String>) message -> { });
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
