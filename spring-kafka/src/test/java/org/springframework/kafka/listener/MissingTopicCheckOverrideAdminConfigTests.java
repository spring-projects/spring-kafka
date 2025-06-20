/*
 * Copyright 2023-present the original author or authors.
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

import java.util.Map;
import java.util.Properties;

import org.apache.commons.logging.LogFactory;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.junit.jupiter.api.Test;

import org.springframework.beans.DirectFieldAccessor;
import org.springframework.core.log.LogAccessor;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.kafka.test.utils.KafkaTestUtils;

import static org.assertj.core.api.Assertions.assertThatNoException;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;

/**
 * @author Gary Russell
 * @since 3.0
 *
 */
@EmbeddedKafka(topics = "mtccac")
public class MissingTopicCheckOverrideAdminConfigTests {

	@Test
	void configOverride(EmbeddedKafkaBroker broker) {
		Map<String, Object> consumerProps = KafkaTestUtils.consumerProps(broker, "grp", false);
		consumerProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "junkjunk");
		DefaultKafkaConsumerFactory<Integer, String> cf = new DefaultKafkaConsumerFactory<>(consumerProps);
		ContainerProperties props = new ContainerProperties("mtccac");
		props.setMissingTopicsFatal(true);
		props.getKafkaConsumerProperties().setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,
				broker.getBrokersAsString());
		KafkaMessageListenerContainer<Integer, String> container = new KafkaMessageListenerContainer<>(cf, props) {

			@Override
			public void checkTopics() {
				super.checkTopics();
			}

		};
		LogAccessor logger = spy(new LogAccessor(LogFactory.getLog(getClass())));
		new DirectFieldAccessor(container).setPropertyValue("logger", logger);
		assertThatNoException().isThrownBy(() -> container.checkTopics());
		verify(logger, never()).error(any(), anyString());
	}

	@Test
	void configOverrideDefault(EmbeddedKafkaBroker broker) {
		Map<String, Object> consumerProps = KafkaTestUtils.consumerProps(broker, "grp", false);
		consumerProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "junkjunk");
		DefaultKafkaConsumerFactory<Integer, String> cf = new DefaultKafkaConsumerFactory<>(consumerProps);
		ContainerProperties props = new ContainerProperties("mtccac");
		props.setMissingTopicsFatal(true);
		/*
		 *  Ensure this works if there are property defaults.
		 *  We have to iterate over the hash table because the user might have
		 *  used put() instead of setProperty().
		 */
		Properties defaultProperties = new Properties();
		defaultProperties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, broker.getBrokersAsString());
		Properties properties = new Properties(defaultProperties);
		props.setKafkaConsumerProperties(properties);
		KafkaMessageListenerContainer<Integer, String> container = new KafkaMessageListenerContainer<>(cf, props) {

			@Override
			public void checkTopics() {
				super.checkTopics();
			}

		};
		LogAccessor logger = spy(new LogAccessor(LogFactory.getLog(getClass())));
		new DirectFieldAccessor(container).setPropertyValue("logger", logger);
		assertThatNoException().isThrownBy(() -> container.checkTopics());
		verify(logger, never()).error(any(), anyString());
	}

}
