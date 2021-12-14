/*
 * Copyright 2021 the original author or authors.
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

package org.springframework.kafka.support.serializer;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;

import java.util.Map;
import java.util.regex.Pattern;

import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.Test;

import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.kafka.listener.KafkaMessageListenerContainer;
import org.springframework.kafka.listener.MessageListener;
import org.springframework.kafka.test.condition.EmbeddedKafkaCondition;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.kafka.test.utils.KafkaTestUtils;

/**
 * @author Gary Russell
 * @since 2.8.1
 *
 */
@EmbeddedKafka(topics = SerializationIntegrationTests.DBTD_TOPIC)
public class SerializationIntegrationTests {

	public static final String DBTD_TOPIC = "dbtd";

	@Test
	void configurePreLoadedDelegates() {
		Map<String, Object> consumerProps =
				KafkaTestUtils.consumerProps(DBTD_TOPIC, "false", EmbeddedKafkaCondition.getBroker());
		consumerProps.put(DelegatingByTopicDeserializer.VALUE_SERIALIZATION_TOPIC_CONFIG, DBTD_TOPIC + ":"
				+ TestDeserializer.class.getName());
		TestDeserializer testDeser = new TestDeserializer();
		DelegatingByTopicDeserializer delegating = new DelegatingByTopicDeserializer(
				Map.of(Pattern.compile(DBTD_TOPIC), testDeser), new StringDeserializer());
		DefaultKafkaConsumerFactory<String, Object> cFact =
				new DefaultKafkaConsumerFactory<String, Object>(consumerProps,
					new StringDeserializer(), delegating);
		ContainerProperties props = new ContainerProperties(DBTD_TOPIC);
		props.setMessageListener(mock(MessageListener.class));
		KafkaMessageListenerContainer<String, Object> container = new KafkaMessageListenerContainer<>(cFact, props);
		container.start();
		assertThat(KafkaTestUtils.getPropertyValue(container, "listenerConsumer.consumer.valueDeserializer"))
				.isSameAs(delegating);
		Map<?, ?> delegates = KafkaTestUtils.getPropertyValue(delegating, "delegates", Map.class);
		assertThat(delegates).hasSize(1);
		assertThat(delegates.values().iterator().next()).isSameAs(testDeser);
		assertThat(testDeser.configured).isTrue();
		container.stop();
	}

	static class TestDeserializer implements Deserializer<Object> {

		boolean configured;

		@Override
		public void configure(Map<String, ?> configs, boolean isKey) {
			this.configured = true;
		}

		@Override
		public Object deserialize(String topic, byte[] data) {
			return null;
		}

		@Override
		public Object deserialize(String topic, Headers headers, byte[] data) {
			return Deserializer.super.deserialize(topic, headers, data);
		}

	}

}

